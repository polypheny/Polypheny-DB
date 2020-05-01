/*
 * Copyright 2019-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.router;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.core.TableFunctionScan;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.logical.LogicalAggregate;
import org.polypheny.db.rel.logical.LogicalCorrelate;
import org.polypheny.db.rel.logical.LogicalExchange;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalIntersect;
import org.polypheny.db.rel.logical.LogicalJoin;
import org.polypheny.db.rel.logical.LogicalMatch;
import org.polypheny.db.rel.logical.LogicalMinus;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalSort;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalUnion;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.routing.ExecutionTimeMonitor.ExecutionTimeObserver;
import org.polypheny.db.routing.Router;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.background.BackgroundTask.TaskPriority;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;
import org.polypheny.db.util.background.BackgroundTaskManager;

public class IcarusRouter extends AbstractRouter {

    private static final IcarusRoutingTable routingTable = new IcarusRoutingTable();

    private int selectedStoreId = -2; // Is set in analyze
    private String queryClassString;


    private IcarusRouter() {
        // Intentionally left empty
    }


    @Override
    protected void analyze( RelRoot logicalRoot ) {
        if ( !(logicalRoot.rel instanceof LogicalTableModify) ) {
            IcarusShuttle icarusShuttle = new IcarusShuttle();
            logicalRoot.rel.accept( icarusShuttle );
            queryClassString = icarusShuttle.hashBasis.toString();
            if ( routingTable.contains( queryClassString ) ) {
                for ( Map.Entry<Integer, Integer> entry : routingTable.get( queryClassString ).entrySet() ) {
                    if ( entry.getValue() == IcarusRoutingTable.MISSING_VALUE ) {
                        // We have no execution time for this store.
                        selectedStoreId = entry.getKey();
                        break;
                    }
                    if ( entry.getValue() > 0 ) {
                        selectedStoreId = entry.getKey();
                    }
                }
            } else {
                selectedStoreId = -1;
            }
        }
    }


    // Execute the table scan on the store select in the analysis (in Icarus routing all tables are replicated to all stores)
    @Override
    protected CatalogColumnPlacement selectPlacement( RelNode node, List<CatalogColumnPlacement> available ) {
        // Update known stores
        routingTable.updateKnownStores( available );
        // Route
        if ( selectedStoreId == -1 ) {
            routingTable.initializeRow( queryClassString, available );
            selectedStoreId = available.get( 0 ).storeId;
        }
        for ( CatalogColumnPlacement placement : available ) {
            if ( placement.storeId == selectedStoreId ) {
                executionTimeMonitor.subscribe( routingTable, placement.storeId + "-" + queryClassString );
                return placement;
            }
        }
        throw new RuntimeException( "The previously selected store does not contain a placement of this table. Store ID: " + selectedStoreId );
    }


    // Create table on all stores supporting schema changes
    @Override
    public List<Store> createTable( long schemaId, Transaction transaction ) {
        List<Store> result = new LinkedList<>();
        Map<String, Store> availableStores = StoreManager.getInstance().getStores();
        for ( Store store : availableStores.values() ) {
            if ( !store.isSchemaReadOnly() ) {
                result.add( store );
            }
        }
        if ( result.size() == 0 ) {
            throw new RuntimeException( "No suitable store found" );
        }
        return ImmutableList.copyOf( result );
    }


    private static class IcarusRoutingTable implements ExecutionTimeObserver {

        public static final int MISSING_VALUE = -1;
        public static final int NO_PLACEMENT = -2;

        private final Map<String, Map<Integer, Integer>> routingTable = new ConcurrentHashMap<>();  // QueryClassStr -> (Store -> Percentage)
        private final Map<String, Map<Integer, Long>> times = new ConcurrentHashMap<>();  // QueryClassStr -> (Store -> Percentage)

        private final Map<Integer, String> knownStores = new HashMap<>(); // Store ID -> Store Name

        private final Queue<ExecutionTime> processingQueue = new ConcurrentLinkedQueue<>();


        private IcarusRoutingTable() {
            // Information
            InformationManager im = InformationManager.getInstance();
            InformationPage page = new InformationPage( "IcarusRouting", "Icarus Routing" );
            page.fullWidth();
            im.addPage( page );
            InformationGroup routingTableGroup = new InformationGroup( page, "Routing Table" );
            im.addGroup( routingTableGroup );
            InformationTable routingTableElement = new InformationTable(
                    routingTableGroup,
                    Arrays.asList( "Query Class" ) );
            im.registerInformation( routingTableElement );
            page.setRefreshFunction( () -> {
                // Update labels
                if ( routingTable.size() > 0 ) {
                    routingTableElement.updateLabels(
                            "Query Class",
                            ImmutableList.copyOf( knownStores.values() )
                    );
                }
                // Update rows
                routingTableElement.reset();
                routingTable.forEach( ( k, v ) -> routingTableElement.addRow(
                        k,
                        v.values().stream().map( Functions.toStringFunction()::apply ).collect( Collectors.toList() ) ) );
            } );

            // Background Task
            BackgroundTaskManager.INSTANCE.registerTask(
                    this::process,
                    "Process query execution times and update Icarus routing table",
                    TaskPriority.LOW,
                    TaskSchedulingType.EVERY_FIVE_SECONDS
            );
        }


        public boolean contains( String queryClassStr ) {
            return routingTable.containsKey( queryClassStr );
        }


        public Map<Integer, Integer> get( String queryClassStr ) {
            return routingTable.get( queryClassStr );
        }


        private void process() {
            // Add to times map
            for ( ExecutionTime executionTime : processingQueue ) {
                Map<Integer, Long> row = times.get( executionTime.queryClassString );
                if ( row.containsKey( executionTime.storeId ) ) {
                    long time = (row.get( executionTime.storeId ) + executionTime.nanoTime) / 2;
                    row.replace( executionTime.storeId, time );
                } else {
                    row.put( executionTime.storeId, executionTime.nanoTime );
                }
            }

            // Update routing table
            for ( String queryClass : routingTable.keySet() ) {
                Map<Integer, Long> timeRow = times.get( queryClass );
                // find fastest
                int fastestStore = -1;
                long fastestTime = Long.MAX_VALUE;
                for ( Map.Entry<Integer, Long> entry : timeRow.entrySet() ) {
                    if ( entry.getValue() < fastestTime ) {
                        fastestStore = entry.getKey();
                        fastestTime = entry.getValue();
                    }
                }
                Map<Integer, Integer> newRow = new HashMap<>();
                for ( Map.Entry<Integer, Integer> oldEntry : routingTable.get( queryClass ).entrySet() ) {
                    if ( oldEntry.getValue() == NO_PLACEMENT ) {
                        newRow.put( oldEntry.getKey(), NO_PLACEMENT );
                        continue;
                    }
                    if ( timeRow.containsKey( oldEntry.getKey() ) ) {
                        newRow.put( oldEntry.getKey(), 0 );
                    } else {
                        newRow.put( oldEntry.getKey(), MISSING_VALUE );
                    }
                }
                newRow.replace( fastestStore, 1 );
                routingTable.replace( queryClass, newRow );
            }
        }


        // called by execution monitor to inform about execution time
        @Override
        public void executionTime( String reference, long nanoTime ) {
            String storeIdStr = reference.split( "-" )[0]; // Reference starts with "STORE_ID-..."
            int storeId = Integer.parseInt( storeIdStr );
            String queryClassString = reference.substring( storeIdStr.length() + 1 );
            processingQueue.add( new ExecutionTime( queryClassString, storeId, nanoTime ) );
        }


        public void updateKnownStores( List<CatalogColumnPlacement> available ) {
            for ( CatalogColumnPlacement placement : available ) {
                if ( !knownStores.containsKey( placement.storeId ) ) {
                    knownStores.put( placement.storeId, placement.storeUniqueName );
                }
            }
        }


        public void initializeRow( String queryClassString, List<CatalogColumnPlacement> available ) {
            Map<Integer, Integer> row = new HashMap<>();
            // Initialize with NO_PLACEMENT
            for ( int storeId : knownStores.keySet() ) {
                row.put( storeId, NO_PLACEMENT );
            }
            // Set missing values entry
            for ( CatalogColumnPlacement placement : available ) {
                row.replace( placement.storeId, MISSING_VALUE );
            }
            routingTable.put( queryClassString, row );
            times.put( queryClassString, new HashMap<>() );
        }
    }


    @AllArgsConstructor
    private static class ExecutionTime {

        private String queryClassString;
        private int storeId;
        private long nanoTime;
    }


    public static class IcarusRouterFactory extends RouterFactory {

        @Override
        public Router createInstance() {
            return new IcarusRouter();
        }

    }


    // TODO MV: This should be improved to include more information on the used tables and columns
    private static class IcarusShuttle extends RelShuttleImpl {

        private HashSet<String> hashBasis = new HashSet<>();


        @Override
        public RelNode visit( LogicalAggregate aggregate ) {
            hashBasis.add( "LogicalAggregate#" + aggregate.getAggCallList() );
            return visitChild( aggregate, 0, aggregate.getInput() );
        }


        @Override
        public RelNode visit( LogicalMatch match ) {
            hashBasis.add( "LogicalMatch#" + match.getTable().getQualifiedName() );
            return visitChild( match, 0, match.getInput() );
        }


        @Override
        public RelNode visit( TableScan scan ) {
            hashBasis.add( "TableScan#" + scan.getTable().getQualifiedName() );
            return scan;
        }


        @Override
        public RelNode visit( TableFunctionScan scan ) {
            hashBasis.add( "TableFunctionScan#" + scan.getTable().getQualifiedName() ); // TODO: This is most probably not sufficient
            return visitChildren( scan );
        }


        @Override
        public RelNode visit( LogicalValues values ) {
            return values;
        }


        @Override
        public RelNode visit( LogicalFilter filter ) {
            hashBasis.add( "LogicalFilter" );
            return visitChild( filter, 0, filter.getInput() );
        }


        @Override
        public RelNode visit( LogicalProject project ) {
            hashBasis.add( "LogicalProject#" + project.getProjects().size() );
            return visitChild( project, 0, project.getInput() );
        }


        @Override
        public RelNode visit( LogicalJoin join ) {
            hashBasis.add( "LogicalJoin#" + join.getLeft().getTable().getQualifiedName() + "#" + join.getRight().getTable().getQualifiedName() );
            return visitChildren( join );
        }


        @Override
        public RelNode visit( LogicalCorrelate correlate ) {
            hashBasis.add( "LogicalCorrelate" );
            return visitChildren( correlate );
        }


        @Override
        public RelNode visit( LogicalUnion union ) {
            hashBasis.add( "LogicalUnion" );
            return visitChildren( union );
        }


        @Override
        public RelNode visit( LogicalIntersect intersect ) {
            hashBasis.add( "LogicalIntersect" );
            return visitChildren( intersect );
        }


        @Override
        public RelNode visit( LogicalMinus minus ) {
            hashBasis.add( "LogicalMinus" );
            return visitChildren( minus );
        }


        @Override
        public RelNode visit( LogicalSort sort ) {
            hashBasis.add( "LogicalSort" );
            return visitChildren( sort );
        }


        @Override
        public RelNode visit( LogicalExchange exchange ) {
            hashBasis.add( "LogicalExchange#" + exchange.distribution.getType().shortName );
            return visitChildren( exchange );
        }


        @Override
        public RelNode visit( RelNode other ) {
            hashBasis.add( "other#" + other.getClass().getSimpleName() );
            return visitChildren( other );
        }
    }
}
