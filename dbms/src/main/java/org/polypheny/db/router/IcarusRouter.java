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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.StatUtils;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationHtml;
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

@Slf4j
public class IcarusRouter extends AbstractRouter {

    public static final int WINDOW_SIZE = 25;
    private static final int SHORT_RUNNING_SIMILAR_THRESHOLD = 100;
    private static final int LONG_RUNNING_SIMILAR_THRESHOLD = 0;

    private static final IcarusRoutingTable routingTable = new IcarusRoutingTable();

    private int selectedStoreId = -2; // Is set in analyze
    private String queryClassString;


    private IcarusRouter() {
        // Intentionally left empty
    }


    @Override
    protected void analyze( Transaction transaction, RelRoot logicalRoot ) {
        if ( !(logicalRoot.rel instanceof LogicalTableModify) ) {
            IcarusShuttle icarusShuttle = new IcarusShuttle();
            logicalRoot.rel.accept( icarusShuttle );
            queryClassString = icarusShuttle.hashBasis.toString();
            if ( routingTable.contains( queryClassString ) ) {
                selectedStoreId = routeQuery( routingTable.get( queryClassString ) );

                // In case the query class is known but the table has been dropped and than recreated with the same name,
                // the query class is known but only contains information for the stores with no placement. To handle this
                // special case (selectedStoreId == -2) we have to set it to -1.
                if ( selectedStoreId == -2 ) {
                    selectedStoreId = -1;
                }
                if ( transaction.isAnalyze() ) {
                    InformationGroup group = new InformationGroup( page, "Routing Table Entry" );
                    transaction.getQueryAnalyzer().addGroup( group );
                    InformationTable table = new InformationTable( group, ImmutableList.copyOf( routingTable.knownStores.values() ) );
                    Map<Integer, Integer> entry = routingTable.get( queryClassString );
                    Map<Integer, CircularFifoQueue<Double>> timesEntry = routingTable.times.get( queryClassString );
                    List<String> row1 = new LinkedList<>();
                    List<String> row2 = new LinkedList<>();
                    for ( Entry<Integer, Integer> e : entry.entrySet() ) {
                        if ( e.getValue() == IcarusRoutingTable.MISSING_VALUE ) {
                            row1.add( "MISSING VALUE" );
                            row2.add( "" );
                        } else if ( e.getValue() == IcarusRoutingTable.NO_PLACEMENT ) {
                            row1.add( "NO PLACEMENT" );
                            row2.add( "" );
                        } else {
                            row1.add( e.getValue() + "" );
                            double mean = StatUtils.mean( ArrayUtils.toPrimitive( timesEntry.get( e.getKey() ).toArray( new Double[0] ) ), 0, timesEntry.get( e.getKey() ).size() );
                            row2.add( mean / 1000000.0 + " ms" );
                        }
                    }
                    table.addRow( row1 );
                    table.addRow( row2 );
                    transaction.getQueryAnalyzer().registerInformation( table );
                }
            } else {
                if ( transaction.isAnalyze() ) {
                    InformationGroup group = new InformationGroup( page, "Routing Table Entry" );
                    transaction.getQueryAnalyzer().addGroup( group );
                    InformationHtml html = new InformationHtml( group, "Unknown query class" );
                    transaction.getQueryAnalyzer().registerInformation( html );
                }
                selectedStoreId = -1;
            }
        }
        if ( transaction.isAnalyze() ) {
            InformationGroup group = new InformationGroup( page, "Icarus Routing" );
            transaction.getQueryAnalyzer().addGroup( group );
            InformationHtml informationHtml = new InformationHtml(
                    group,
                    "<p><b>Selected Store ID:</b> " + selectedStoreId + "</p>"
                            + "<p><b>Query Class:</b> " + queryClassString + "</p>" );
            transaction.getQueryAnalyzer().registerInformation( informationHtml );
        }
    }


    private int routeQuery( Map<Integer, Integer> routingTableRow ) {
        // Check if there is a store for which we do not have a execution time
        for ( Entry<Integer, Integer> entry : routingTable.get( queryClassString ).entrySet() ) {
            if ( entry.getValue() == IcarusRoutingTable.MISSING_VALUE ) {
                // We have no execution time for this store.
                return entry.getKey();
            }
        }

        if ( SHORT_RUNNING_SIMILAR_THRESHOLD == 0 ) {
            // There should only be exactly one entry in the routing table > 0
            for ( Entry<Integer, Integer> entry : routingTable.get( queryClassString ).entrySet() ) {
                if ( entry.getValue() == 100 ) {
                    // We have no execution time for this store.
                    return entry.getKey();
                }
            }
        } else {
            int p = 0;
            int random = (int) (Math.random() * 100) + 1;
            for ( Map.Entry<Integer, Integer> entry : routingTableRow.entrySet() ) {
                p += entry.getValue();
                if ( p >= random ) {
                    return entry.getKey();
                }
            }
        }
        throw new RuntimeException( "Something went wrong..." );
    }


    @Override
    protected void wrapUp( Transaction transaction, RelNode routed ) {
        executionTimeMonitor.subscribe( routingTable, selectedStoreId + "-" + queryClassString );
        if ( transaction.isAnalyze() ) {
            InformationGroup executionTimeGroup = new InformationGroup( page, "Execution Time" );
            transaction.getQueryAnalyzer().addGroup( executionTimeGroup );
            executionTimeMonitor.subscribe(
                    ( reference, nanoTime ) -> {
                        InformationHtml html = new InformationHtml( executionTimeGroup, nanoTime / 1000000.0 + " ms" );
                        transaction.getQueryAnalyzer().registerInformation( html );
                    },
                    selectedStoreId + "-" + queryClassString );
        }
    }


    // Execute the table scan on the store select in the analysis (in Icarus routing all tables are replicated to all stores)
    @Override
    protected CatalogColumnPlacement selectPlacement( RelNode node, List<CatalogColumnPlacement> available ) {
        // Update known stores
        updateKnownStores( available );
        // Route
        if ( selectedStoreId == -1 ) {
            routingTable.initializeRow( queryClassString, available );
            selectedStoreId = available.get( 0 ).storeId;
        }
        for ( CatalogColumnPlacement placement : available ) {
            if ( placement.storeId == selectedStoreId ) {
                return placement;
            }
        }
        throw new RuntimeException( "The previously selected store does not contain a placement of this table. Store ID: " + selectedStoreId );
    }


    public void updateKnownStores( List<CatalogColumnPlacement> available ) {
        for ( CatalogColumnPlacement placement : available ) {
            if ( !routingTable.knownStores.containsKey( placement.storeId ) ) {
                routingTable.knownStores.put( placement.storeId, placement.storeUniqueName );
                if ( routingTable.routingTable.get( queryClassString ) != null && !routingTable.routingTable.get( queryClassString ).containsKey( placement.storeId ) ) {
                    routingTable.routingTable.get( queryClassString ).put( placement.storeId, IcarusRoutingTable.MISSING_VALUE );
                }
            }
        }
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


    @Override
    public void dropPlacements( List<CatalogColumnPlacement> placements ) {
        routingTable.dropPlacements( placements );
    }


    private static class IcarusRoutingTable implements ExecutionTimeObserver {

        public static final int MISSING_VALUE = -1;
        public static final int NO_PLACEMENT = -2;

        private final Map<String, Map<Integer, Integer>> routingTable = new ConcurrentHashMap<>();  // QueryClassStr -> (Store -> Percentage)
        private final Map<String, Map<Integer, CircularFifoQueue<Double>>> times = new ConcurrentHashMap<>();  // QueryClassStr -> (Store -> Time)

        private final Map<Integer, String> knownStores = new HashMap<>(); // Store ID -> Store Name

        private final Queue<ExecutionTime> processingQueue = new ConcurrentLinkedQueue<>();
        private final Lock processingQueueLock = new ReentrantLock();


        private IcarusRoutingTable() {
            // Information
            InformationManager im = InformationManager.getInstance();
            InformationPage page = new InformationPage( "Icarus Routing" );
            page.fullWidth();
            im.addPage( page );
            // Routing table
            InformationGroup routingTableGroup = new InformationGroup( page, "Routing Table" );
            im.addGroup( routingTableGroup );
            InformationTable routingTableElement = new InformationTable(
                    routingTableGroup,
                    Arrays.asList( "Query Class" ) );
            im.registerInformation( routingTableElement );
            // Processing queue size
            InformationGroup processingQueueGroup = new InformationGroup( page, "Processing Queue" );
            im.addGroup( processingQueueGroup );
            InformationHtml processingQueueSize = new InformationHtml(
                    processingQueueGroup,
                    "Processing queue size: " + processingQueue.size() );
            im.registerInformation( processingQueueSize );
            // update
            page.setRefreshFunction( () -> {
                // Update labels
                if ( routingTable.size() > 0 ) {
                    LinkedList<String> labels = new LinkedList<>();
                    labels.add( "Query Class" );
                    labels.addAll( knownStores.values() );
                    routingTableElement.updateLabels( labels );
                }
                // Update rows
                routingTableElement.reset();
                routingTable.forEach( ( k, v ) -> {
                    List<String> row = new LinkedList<>();
                    row.add( k );
                    for ( Integer integer : v.values() ) {
                        if ( integer == IcarusRoutingTable.MISSING_VALUE ) {
                            row.add( "Unknown" );
                        } else if ( integer == IcarusRoutingTable.NO_PLACEMENT ) {
                            row.add( "-" );
                        } else {
                            row.add( integer + "" );
                        }
                    }
                    routingTableElement.addRow( row );
                } );
                // Update processing queue size
                processingQueueSize.updateHtml( "Processing queue size: " + processingQueue.size() );
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
            processingQueueLock.lock();
            // Add to times map
            while ( processingQueue.size() > 0 ) {
                ExecutionTime executionTime = processingQueue.poll();
                Map<Integer, CircularFifoQueue<Double>> row = times.get( executionTime.queryClassString );
                if ( row.containsKey( executionTime.storeId ) ) {
                    row.get( executionTime.storeId ).offer( (double) executionTime.nanoTime );
                } else {
                    CircularFifoQueue<Double> queue = new CircularFifoQueue<>( WINDOW_SIZE );
                    queue.offer( (double) executionTime.nanoTime );
                    row.put( executionTime.storeId, queue );
                }
            }

            // Update routing table
            for ( String queryClass : routingTable.keySet() ) {
                Map<Integer, Double> meanTimeRow = new HashMap<>();
                for ( Map.Entry<Integer, CircularFifoQueue<Double>> entry : times.get( queryClass ).entrySet() ) {
                    double mean = StatUtils.mean( ArrayUtils.toPrimitive( entry.getValue().toArray( new Double[0] ) ), 0, entry.getValue().size() );
                    meanTimeRow.put( entry.getKey(), mean );
                }

                Map<Integer, Integer> newRow = new HashMap<>();
                for ( Integer storeId : knownStores.keySet() ) {
                    newRow.put( storeId, IcarusRoutingTable.NO_PLACEMENT );
                }
                Map<Integer, Integer> calculatedRow = generateRow( meanTimeRow );
                for ( Map.Entry<Integer, Integer> oldEntry : routingTable.get( queryClass ).entrySet() ) {
                    if ( oldEntry.getValue() == NO_PLACEMENT ) {
                        newRow.put( oldEntry.getKey(), NO_PLACEMENT );
                    } else if ( calculatedRow.containsKey( oldEntry.getKey() ) ) {
                        newRow.replace( oldEntry.getKey(), calculatedRow.get( oldEntry.getKey() ) );
                    } else {
                        newRow.replace( oldEntry.getKey(), MISSING_VALUE );
                    }
                }
                routingTable.replace( queryClass, newRow );
            }
            processingQueueLock.unlock();
        }


        // called by execution monitor to inform about execution time
        @Override
        public void executionTime( String reference, long nanoTime ) {
            String storeIdStr = reference.split( "-" )[0]; // Reference starts with "STORE_ID-..."
            int storeId = Integer.parseInt( storeIdStr );
            String queryClassString = reference.substring( storeIdStr.length() + 1 );
            processingQueue.add( new ExecutionTime( queryClassString, storeId, nanoTime ) );
        }


        public void dropPlacements( List<CatalogColumnPlacement> placements ) {
            process();// empty processing queue
            processingQueueLock.lock();
            for ( CatalogColumnPlacement placement : placements ) {
                knownStores.remove( placement.storeId );
                for ( Map<Integer, CircularFifoQueue<Double>> entry : times.values() ) {
                    entry.remove( placement.storeId );
                }
                for ( Map<Integer, Integer> entry : routingTable.values() ) {
                    entry.remove( placement.storeId );
                }
            }
            processingQueueLock.unlock();
            process();// update routing table
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


        protected Map<Integer, Integer> generateRow( Map<Integer, Double> map ) {
            Map<Integer, Integer> row;
            // find fastest
            int fastestStore = -1;
            double fastestTime = Double.MAX_VALUE;
            for ( Map.Entry<Integer, Double> entry : map.entrySet() ) {
                if ( fastestTime > entry.getValue() ) {
                    fastestStore = entry.getKey();
                    fastestTime = entry.getValue();
                }
            }
            long shortRunningLongRunningThreshold = SHORT_RUNNING_SIMILAR_THRESHOLD * 1000000; // multiply with 1000000 to get nanoseconds
            if ( fastestTime < shortRunningLongRunningThreshold && SHORT_RUNNING_SIMILAR_THRESHOLD != 0 ) {
                row = calc( map, SHORT_RUNNING_SIMILAR_THRESHOLD, fastestTime, fastestStore );
            } else if ( fastestTime > shortRunningLongRunningThreshold && LONG_RUNNING_SIMILAR_THRESHOLD != 0 ) {
                row = calc( map, LONG_RUNNING_SIMILAR_THRESHOLD, fastestTime, fastestStore );
            } else {
                row = new HashMap<>();
                if ( fastestStore != -1 && fastestTime > 0 ) {
                    row.put( fastestStore, 100 );
                }
                /*else {
                    log.error( "Something went wrong while analyzing data. This should not happen!" );
                }*/
            }
            return row;
        }


        private Map<Integer, Integer> calc( Map<Integer, Double> map, int similarThreshold, double fastestTime, int fastestStore ) {
            HashMap<Integer, Integer> row = new HashMap<>();
            ArrayList<Integer> percents = new ArrayList<>();
            Map<Integer, Double> stores = new TreeMap<>();
            // init row with 0
            for ( Integer storeId : map.keySet() ) {
                row.put( storeId, 0 );
            }
            // calc 100%
            int threshold = (int) (fastestTime + (fastestTime * (similarThreshold / 100.0)));
            int hundredPercent = 0;
            for ( Map.Entry<Integer, Double> entry : map.entrySet() ) {
                if ( threshold >= entry.getValue() ) {
                    hundredPercent += entry.getValue();
                }
            }
            // calc percents
            int onePercent = hundredPercent / 100;
            for ( Map.Entry<Integer, Double> entry : map.entrySet() ) {
                if ( threshold >= entry.getValue() ) {
                    int t = Math.min( entry.getValue().intValue() / onePercent, 100 ); // This is not nice... But if there is only one entry with 100 percent, it some time happens that we get 101
                    percents.add( t );
                    stores.put( entry.getKey(), entry.getValue() );
                }
            }
            // add
            Collections.sort( percents );
            Collections.reverse( percents );
            for ( Map.Entry<Integer, Double> entry : entriesSortedByValues( stores ) ) {
                row.put( entry.getKey(), percents.remove( 0 ) );
            }
            // normalize to 100
            int sum = 0;
            for ( Map.Entry<Integer, Integer> entry : row.entrySet() ) {
                sum += entry.getValue();
            }
            if ( sum == 0 ) {
                log.error( "Routing table row is empty! This should not happen!" );
            } else if ( sum > 100 ) {
                log.error( "Routing table row does sum up to a value greater 100! This should not happen!" );
            } else if ( sum < 100 ) {
                if ( fastestStore == -1 ) {
                    log.error( "Fastest Store is -1! This should not happen!" );
                } else if ( !row.containsKey( fastestStore ) ) {
                    log.error( "Row does not contain the fastest row! This should not happen!" );
                } else {
                    int delta = 100 - sum;
                    row.replace( fastestStore, row.get( fastestStore ) + delta );
                }
            }
            return row;
        }


        //http://stackoverflow.com/a/2864923
        private static <K, V extends Comparable<? super V>> SortedSet<Entry<K, V>> entriesSortedByValues( Map<K, V> map ) {
            SortedSet<Map.Entry<K, V>> sortedEntries = new TreeSet<>( ( e1, e2 ) -> {
                int res = e1.getValue().compareTo( e2.getValue() );
                return res != 0 ? res : 1;
            } );
            sortedEntries.addAll( map.entrySet() );
            return sortedEntries;
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
