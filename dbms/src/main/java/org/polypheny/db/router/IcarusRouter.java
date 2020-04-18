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

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.AllArgsConstructor;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
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
                    if ( entry.getValue() == -1 ) {
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
        if ( selectedStoreId == -1 ) {
            List<Integer> stores = new LinkedList<>();
            for ( CatalogColumnPlacement placement : available ) {
                stores.add( placement.storeId );
            }
            routingTable.add( queryClassString, stores );
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

        private final Map<String, Map<Integer, Integer>> routingTable = new ConcurrentHashMap<>();  // QueryClassStr -> (Store -> Percentage)
        private final Map<String, Map<Integer, Long>> times = new ConcurrentHashMap<>();  // QueryClassStr -> (Store -> Percentage)

        private final Queue<ExecutionTime> processingQueue = new ConcurrentLinkedQueue<>();


        private IcarusRoutingTable() {
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


        public void add( String queryClassStr, List<Integer> stores ) {
            Map<Integer, Integer> row = new HashMap<>();
            for ( Integer store : stores ) {
                row.put( store, -1 );
            }
            routingTable.put( queryClassStr, row );
            times.put( queryClassStr, new HashMap<>() );
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
                    if ( timeRow.containsKey( oldEntry.getKey() ) ) {
                        newRow.put( oldEntry.getKey(), 0 );
                    } else {
                        newRow.put( oldEntry.getKey(), -1 );
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
            // TODO
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
            // TODO
            return visitChildren( correlate );
        }


        @Override
        public RelNode visit( LogicalUnion union ) {
            // TODO
            return visitChildren( union );
        }


        @Override
        public RelNode visit( LogicalIntersect intersect ) {
            // TODO
            return visitChildren( intersect );
        }


        @Override
        public RelNode visit( LogicalMinus minus ) {
            // TODO
            return visitChildren( minus );
        }


        @Override
        public RelNode visit( LogicalSort sort ) {
            // TODO
            return visitChildren( sort );
        }


        @Override
        public RelNode visit( LogicalExchange exchange ) {
            // TODO
            return visitChildren( exchange );
        }


        @Override
        public RelNode visit( RelNode other ) {
            hashBasis.add( "other#" + other.getClass().getSimpleName() );
            return visitChildren( other );
        }
    }
}
