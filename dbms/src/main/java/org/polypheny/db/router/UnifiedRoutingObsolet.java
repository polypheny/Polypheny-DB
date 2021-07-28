/*
 * Copyright 2019-2021 The Polypheny Project
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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.math3.stat.StatUtils;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.ConfigBoolean;
import org.polypheny.db.config.ConfigEnum;
import org.polypheny.db.config.ConfigInteger;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationHtml;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.RoutingEvent;
import org.polypheny.db.monitoring.events.metrics.RoutingDataPoint;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.processing.QueryAnalyzeRelShuttle;
import org.polypheny.db.processing.QueryParameterizer;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.router.SimpleRouter.SimpleRouterFactory;
import org.polypheny.db.routing.ExecutionTimeMonitor.ExecutionTimeObserver;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.background.BackgroundTask.TaskPriority;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;
import org.polypheny.db.util.background.BackgroundTaskManager;

@Slf4j
public class UnifiedRoutingObsolet extends AbstractRouter {

    private static final ConfigBoolean TRAINING = new ConfigBoolean(
            "unifiedRouting/training",
            "Whether routing table should be adjusted according to the measured execution times. Setting this to false keeps the routing table in its current state.",
            true );
    private static final ConfigInteger WINDOW_SIZE = new ConfigInteger(
            "unifiedRouting/windowSize",
            "Size of the moving average on the execution times per query class used for calculating the routing table.",
            25 );
    private static final ConfigInteger SHORT_RUNNING_SIMILAR_THRESHOLD = new ConfigInteger(
            "unifiedRouting/shortRunningSimilarThreshold",
            "The amount of time (specified as percentage of the fastest time) an adapter can be slower than the fastest adapter in order to be still considered for executing queries of a certain query class. Setting this to zero results in only considering the fastest adapter.",
            0 );
    private static final ConfigInteger LONG_RUNNING_SIMILAR_THRESHOLD = new ConfigInteger(
            "unifiedRouting/longRunningSimilarThreshold",
            "The amount of time (specified as percentage of the fastest time) an adapter can be slower than the fastest adapter in order to be still considered for executing queries of a certain query class. Setting this to zero results in only considering the fastest adapter.",
            0 );
    private static final ConfigInteger SHORT_RUNNING_LONG_RUNNING_THRESHOLD = new ConfigInteger(
            "unifiedRouting/shortRunningLongRunningThreshold",
            "The minimal execution time (in milliseconds) for a query to be considered as long-running. Queries with lower execution times are considered as short-running.",
            1000 );

    private static final ConfigEnum QUERY_CLASS_PROVIDER = new ConfigEnum(
            "unifiedRouting/queryClassProvider",
            "Which implementation to use for deriving the query class from a query plan.",
            QUERY_CLASS_PROVIDER_METHOD.class,
            QUERY_CLASS_PROVIDER_METHOD.QUERY_PARAMETERIZER );



    private enum QUERY_CLASS_PROVIDER_METHOD {QUERY_NAME_SHUTTLE, QUERY_PARAMETERIZER}


    private static final UnifiedRoutingTable routingTable = new UnifiedRoutingTable();
    private static final SimpleRouter simpleRouter = SimpleRouterFactory.createSimpleRouterInstance();

    // state fields for single routing run
    private Optional<RoutingTableEntry> selectedRoutingEntry = Optional.empty();
    private boolean basePlacementInitialized = false;
    private boolean useSimpleRouter = false;
    private String queryClassString;

    private HashMap<Long, String> usedColumns = new HashMap<>(); // colId -> fullname
    private HashMap<CatalogTable, Set<CatalogColumn>> usedCatalogColumnsPerTable = new HashMap<>(); //table -> used columns

    private UnifiedRoutingObsolet() {
        // Intentionally left empty
        //MonitoringServiceProvider.getInstance()
    }

    private void updateUsedColumns(Statement statement, RelRoot logicalRoot){
        // get used columns:
        val shuttle = new QueryAnalyzeRelShuttle( statement );
        logicalRoot.rel.accept( shuttle );

        // update column values
        this.usedColumns.clear();
        this.usedColumns.putAll( shuttle.getUsedColumns());

        // update column table values
        this.usedCatalogColumnsPerTable.clear();
        for ( val entry: this.usedColumns.keySet()) {
            val column = Catalog.getInstance().getColumn( entry );
            val table = Catalog.getInstance().getTable( column.tableId );
            if (this.usedCatalogColumnsPerTable.containsKey( table )){
                this.usedCatalogColumnsPerTable.get( table ).add( column );
            }
            this.usedCatalogColumnsPerTable.putIfAbsent( table,  Sets.newHashSet( column));
        }


    }

    private void updateQueryClassString(RelRoot logicalRoot){
        if ( QUERY_CLASS_PROVIDER.getEnum() == QUERY_CLASS_PROVIDER_METHOD.QUERY_NAME_SHUTTLE ) {
            QueryNameShuttle queryNameShuttle = new QueryNameShuttle();
            logicalRoot.rel.accept( queryNameShuttle );
            queryClassString = queryNameShuttle.hashBasis.toString();
        } else if ( QUERY_CLASS_PROVIDER.getEnum() == QUERY_CLASS_PROVIDER_METHOD.QUERY_PARAMETERIZER ) {
            QueryParameterizer parameterizer = new QueryParameterizer( 0, new LinkedList<>() );
            RelNode parameterized = logicalRoot.rel.accept( parameterizer );
            queryClassString = parameterized.relCompareString();
        } else {
            throw new RuntimeException( "Unknown value for QUERY_CLASS_PROVIDER config: " + QUERY_CLASS_PROVIDER.getEnum().name() );
        }
    }

    private void checkForSimpleRouter(){
        val partitionedTable = this.usedCatalogColumnsPerTable.keySet().stream().filter( elem -> elem.isPartitioned ).findAny();
        if(partitionedTable.isPresent() || this.usedColumns.isEmpty()){
            this.useSimpleRouter = true;
        }
    }

    private void initializeQueryAnalyzerUi( Statement statement ){
        InformationGroup group = new InformationGroup( page, "Routing Table Entry" );
        statement.getTransaction().getQueryAnalyzer().addGroup( group );


        InformationTable table = new InformationTable( group, Collections.emptyList()  ); // ImmutableList.copyOf( routingTable.knownAdapters.values()
        Map<RoutingTableEntry, Integer> entry = routingTable.get( queryClassString );

        Map<RoutingTableEntry, List<Double>> timesEntry = routingTable.getExecutionTimes( queryClassString );

        List<String> row1 = new LinkedList<>();
        List<String> row2 = new LinkedList<>();
        List<String> row3 = new LinkedList<>();
        for ( Entry<RoutingTableEntry, Integer> e : entry.entrySet() ) {
            val routingEntry = e.getKey().tableToAdapterIdMapping.toString();
            row1.add( routingEntry );
            if ( e.getValue() == UnifiedRoutingTable.MISSING_VALUE ) {
                row2.add( "MISSING VALUE" );
                row3.add( "" );
            } else if ( e.getValue() == UnifiedRoutingTable.NO_PLACEMENT ) {
                row2.add( "NO PLACEMENT" );
                row3.add( "" );
            } else {
                row2.add( e.getValue() + "" );
                double mean = StatUtils.mean(
                        timesEntry.get( e.getKey() ).stream().mapToDouble( d -> d ).toArray(),
                        0,
                        timesEntry.get( e.getKey() ).size() );
                row3.add( mean / 1000000.0 + " ms" );
            }
        }
        table.addRow( row1 );
        table.addRow( row2 );
        table.addRow( row3 );
        statement.getTransaction().getQueryAnalyzer().registerInformation( table );
    }

    private void updateQueryAnalyzerWithUnknownTableEntry( Statement statement ) {
        InformationGroup group = new InformationGroup( page, "Routing Table Entry" );
        statement.getTransaction().getQueryAnalyzer().addGroup( group );
        InformationHtml html = new InformationHtml( group, "Unknown query class" );
        statement.getTransaction().getQueryAnalyzer().registerInformation( html );
    }

    private void updateQueryAnalyzerWithKnownTableEntry( Statement statement, Optional<RoutingTableEntry> selectedRoutingEntry ) {
        if(selectedRoutingEntry.isPresent()){
            InformationGroup group = new InformationGroup( page, "Unified Routing" );
            statement.getTransaction().getQueryAnalyzer().addGroup( group );
            InformationHtml informationHtml = new InformationHtml(
                    group,
                    "<p><b>Selected Table Adapter Mapping:</b> " + this.selectedRoutingEntry.get().tableToAdapterIdMapping + "</p>"
                            + "<p><b>Query Class:</b> " + queryClassString + "</p>" );
            statement.getTransaction().getQueryAnalyzer().registerInformation( informationHtml );
        }
    }


    private void updateCurrentRoutingElementState( Statement statement, RelRoot logicalRoot ){
        // reset base placement bool
        this.basePlacementInitialized = false;
        this.useSimpleRouter = false;
        this.selectedRoutingEntry = Optional.empty();

        // updates query string value
        this.updateQueryClassString(logicalRoot);
        log.info( "UpdateQueryString" );
        log.info( this.queryClassString );


        // shuttle logical root and extract all needed values ino
        // this.usedCatalogColumnsPerTable and this.usedColumns
        this.updateUsedColumns(statement, logicalRoot);
        log.info( "Analyze: Used col:" + this.usedColumns.size() );
        log.info( "Analyze: Used col", this.usedColumns );

        log.info( "Analyze: table-adapter mapping" + this.usedCatalogColumnsPerTable.size());


        this.checkForSimpleRouter();
        log.info( "Analyze: Check for simple routing: " , this.useSimpleRouter);
    }

    protected void analyze( Statement statement, RelRoot logicalRoot ) {
        if ( !(logicalRoot.rel instanceof LogicalTableModify) ) {
            log.info( "Analyze: Reset parameters" );

            // update current state
            this.updateCurrentRoutingElementState(statement, logicalRoot);
            if ( routingTable.contains( queryClassString ) && !routingTable.get( queryClassString ).isEmpty() ) {
                log.info( "Analyze: Elements and table found!" );
                log.info( "Analyze: Reset selected routing Entry" );
                selectedRoutingEntry = Optional.empty();
                log.info( "Analyze: route query by table entries");
                selectedRoutingEntry = Optional.of( routeQuery( routingTable.get( queryClassString ) ) );

                // TODO: still a case for new routing?
                // In case the query class is known but the table has been dropped and than recreated with the same name,
                // the query class is known but only contains information for the adapters with no placement. To handle this
                // special case (selectedAdapterId == -2) we have to set it to -1.
                if ( statement.getTransaction().isAnalyze() ) {
                    log.info( "Analyze: is Analyze, update UI");
                    this.initializeQueryAnalyzerUi(statement);
                }
            } else {
                if ( statement.getTransaction().isAnalyze() ) {

                    this.updateQueryAnalyzerWithUnknownTableEntry(statement);

                }
                log.info( "Analyze: Reset selected routing Entry" );
                selectedRoutingEntry = Optional.empty();
            }
        }
        if ( statement.getTransaction().isAnalyze()) {
            this.updateQueryAnalyzerWithKnownTableEntry(statement, selectedRoutingEntry);
        }
    }


    private void coverBuildDql( RelNode node, RelBuilder builder, Statement statement, RelOptCluster cluster ){
        //return super.buildSelect( node, builder, statement, cluster );
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            // Check if partition used in general to reduce overhead if not for un-partitioned
            if ( node instanceof LogicalFilter && ((LogicalFilter) node).getInput().getTable() != null ) {

                RelOptTableImpl table = (RelOptTableImpl) ((LogicalFilter) node).getInput().getTable();

                if ( table.getTable() instanceof LogicalTable ) {

                    // TODO Routing of partitioned tables is very limited. This should be improved to also apply sophisticated
                    //  routing strategies, especially when we also get rid of the worst-case routing.

                    LogicalTable t = ((LogicalTable) table.getTable());
                    CatalogTable catalogTable;
                    catalogTable = Catalog.getInstance().getTable( t.getTableId() );
                    if ( catalogTable.isPartitioned ) {
                        WhereClauseVisitor whereClauseVisitor = new WhereClauseVisitor( statement, catalogTable.columnIds.indexOf( catalogTable.partitionColumnId ) );
                        node.accept( new RelShuttleImpl() {
                            @Override
                            public RelNode visit( LogicalFilter filter ) {
                                super.visit( filter );
                                filter.accept( whereClauseVisitor );
                                return filter;
                            }
                        } );

                        if ( whereClauseVisitor.valueIdentified ) {
                            List<Object> values = whereClauseVisitor.getValues().stream()
                                    .map( Object::toString )
                                    .collect( Collectors.toList() );
                            int scanId = 0;
                            if ( !values.isEmpty() ) {
                                scanId = ((LogicalFilter) node).getInput().getId();
                                filterMap.put( scanId, whereClauseVisitor.getValues().stream()
                                        .map( Object::toString )
                                        .collect( Collectors.toList() ) );
                            }
                            log.info( "BuildSelect: build DQL 1" );
                            buildDql( node.getInput( i ), Lists.newArrayList(builder), statement, cluster );
                            filterMap.remove( scanId );
                        } else {
                            log.info( "BuildSelect: build DQL 2" );
                            buildDql( node.getInput( i ), Lists.newArrayList(builder), statement, cluster );
                        }
                    } else {
                        log.info( "BuildSelect: build DQL 3" );
                        buildDql( node.getInput( i ), Lists.newArrayList(builder), statement, cluster );
                    }
                }
            } else {
                log.info( "BuildSelect: build DQL 4" );
                buildDql( node.getInput( i ), Lists.newArrayList(builder), statement, cluster );
            }
        }
    }

    private Map<Long, List<CatalogColumnPlacement>> handlePartitioning( RelNode node, CatalogTable catalogTable ){
        // TODO Routing of partitioned tables is very limited. This should be improved to also apply sophisticated
        //  routing strategies, especially when we also get rid of the worst-case routing.
        Map<Long, List<CatalogColumnPlacement>> placements;

        if ( log.isDebugEnabled() ) {
            log.debug( "VALUE from Map: {} id: {}", filterMap.get( node.getId() ), node.getId() );
        }
        List<String> partitionValues = filterMap.get( node.getId() );

        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( catalogTable.partitionType );
        if ( partitionValues != null ) {
            if ( log.isDebugEnabled() ) {
                log.debug( "TableID: {} is partitioned on column: {} - {}",
                        catalogTable.id,
                        catalogTable.partitionColumnId,
                        catalog.getColumn( catalogTable.partitionColumnId ).name );
            }
            if ( partitionValues.size() == 1 ) {
                List<Long> identPartitions = new ArrayList<>();
                for ( String partitionValue : partitionValues ) {
                    log.debug( "Extracted PartitionValue: {}", partitionValue );
                    long identPart = partitionManager.getTargetPartitionId( catalogTable, partitionValue );
                    identPartitions.add( identPart );
                    log.debug( "Identified PartitionId: {} for value: {}", identPart, partitionValue );
                }
                placements = partitionManager.getRelevantPlacements( catalogTable, identPartitions );
            } else {
                placements = partitionManager.getRelevantPlacements( catalogTable, null );
            }
        } else {
            // TODO Change to worst-case
            placements = partitionManager.getRelevantPlacements( catalogTable, null );
            //placements = selectPlacement( node, catalogTable );
        }

        return placements;
    }


    protected RelBuilder buildSelect( RelNode node, RelBuilder builder, Statement statement, RelOptCluster cluster ) {
        log.info( "Build Select:" + this.queryClassString );

        this.coverBuildDql(node, builder, statement, cluster);

        if ( node instanceof LogicalTableScan && node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();

            if ( table.getTable() instanceof LogicalTable ) {
                LogicalTable t = ((LogicalTable) table.getTable());
                CatalogTable catalogTable;
                Map<Long, List<CatalogColumnPlacement>> placements;
                catalogTable = Catalog.getInstance().getTable( t.getTableId() );

                // Check if table is even partitioned
                if ( catalogTable.isPartitioned ) {
                    log.info( "BuildSelect: Partitioned case! " );
                    // TODO: will be done for single table!
                    placements = this.handlePartitioning(node, catalogTable);
                    return builder.push( buildJoinedTableScan( statement, cluster, placements ) );

                } else {
                    log.debug( "{} is NOT partitioned - Routing will be easy", catalogTable.name );
                    if(this.useSimpleRouter){
                        // make it compilable again, class not used!
                        placements = null; //selectPlacement(node, catalogTable);
                        log.info( "BuildSelect: Update builder SR" );
                        log.info( "Placements: " + placements.size() );
                        return builder.push( buildJoinedTableScan( statement, cluster, placements ) );
                    }
                    else if( !this.basePlacementInitialized ){
                        log.info( "BuildSelect: get select placements", this.basePlacementInitialized );

                        val placementsPerTable = selectPlacement( );
                        for(val entry : placementsPerTable.values()){
                            log.info( "BuildSelect: Update builder" );
                            log.info( "Placements: " + entry.size() );
                            builder.push( buildJoinedTableScan( statement, cluster, null ) );
                        }
                        this.basePlacementInitialized = true;
                    }

                    return builder;
                }


            } else {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }
        } else if ( node instanceof LogicalValues ) {
            log.info( "BuildSelect: handle Values" );
            return handleValues( (LogicalValues) node, builder );
        } else {
            log.info( "BuildSelect: handle Generic" );
            return handleGeneric( node, builder );
        }
    }

    private RoutingTableEntry routeQuery( Map<RoutingTableEntry, Integer> routingTableRow ) {
        // Check if there is an routing entry for which we do not have an execution time
        for ( Entry<RoutingTableEntry, Integer> entry : routingTable.get( queryClassString ).entrySet() ) {
            if ( entry.getValue() == UnifiedRoutingTable.MISSING_VALUE ) {
                // We have no execution time for this routing entry.
                return entry.getKey();
            }
        }

        if ( SHORT_RUNNING_SIMILAR_THRESHOLD.getInt() == 0 ) {
            // There should only be exactly one entry in the routing table > 0
            for ( Entry<RoutingTableEntry, Integer> entry : routingTable.get( queryClassString ).entrySet() ) {
                if ( entry.getValue() == 100 ) {
                    // We have no execution time for this routing entry.
                    return entry.getKey();
                }
            }
        } else {
            int p = 0;
            int random = Math.min( (int) (Math.random() * 100) + 1, 100 );
            for ( Map.Entry<RoutingTableEntry, Integer> entry : routingTableRow.entrySet() ) {
                p += Math.max( entry.getValue(), 0 ); // avoid subtracting -2
                if ( p >= random ) {
                    return entry.getKey();
                }
            }
        }
        throw new RuntimeException( "Something went wrong..." );
    }


    protected void wrapUp( Statement statement, RelNode routed ) {
        if(!this.selectedRoutingEntry.isPresent()){
            // throw exception, should never happen
            return;
        }

        // convert routing entry to json for monitoring purpose.
        Gson gson = new Gson();
        String routingEntryJson = gson.toJson( this.selectedRoutingEntry.get() );

        if ( TRAINING.getBoolean() ) {
            executionTimeMonitor.subscribe( routingTable, routingEntryJson);

        }
        if ( statement.getTransaction().isAnalyze() ) {
           this.subscribeExecutionTimeMonitorForQueryAnalyzer(statement, routingEntryJson);
        }
    }

    private void subscribeExecutionTimeMonitorForQueryAnalyzer( Statement statement, String routingEntryJson ){
        InformationGroup executionTimeGroup = new InformationGroup( page, "Execution Time" );
        statement.getTransaction().getQueryAnalyzer().addGroup( executionTimeGroup );
        executionTimeMonitor.subscribe(
                ( reference, nanoTime ) -> {
                    InformationHtml html = new InformationHtml( executionTimeGroup, nanoTime / 1000000.0 + " ms" );
                    statement.getTransaction().getQueryAnalyzer().registerInformation( html );
                }, routingEntryJson );
    }

    private Map<Integer, List<CatalogColumnPlacement>> selectPlacement() {
        if(this.usedCatalogColumnsPerTable.isEmpty()){
            // TODO: could happen, for example select 1
            log.error( "used catalog columns are empty, should never happen!!!???" );
            return Collections.emptyMap();
            //throw new RuntimeException( "No columns found which are used in query!" );
        }

        // adapter selected during analyze
        if(this.selectedRoutingEntry.isPresent() &&
                !this.selectedRoutingEntry.get().tableToAdapterIdMapping.isEmpty()) {
            // get placement by defined adapters
            val placements = this.selectedRoutingEntry.get().getColumnPlacements(this.usedCatalogColumnsPerTable);
            log.info( "Select placements: Already defined routing from analyze: " + placements.size() );
            return placements;
        }

        // no adapter selected so far, update known adapters and use first one
        if(!this.selectedRoutingEntry.isPresent()) {
            log.info( "SelectPlacements: no adapter selected so far, update known adapters and use first one" );
            val adapters = updateAvailableRoutingEntries( );
            if(adapters.isEmpty()){
                // throw exception
            }
            val placements =  adapters.get( 0 ).getColumnPlacements( this.usedCatalogColumnsPerTable );
            this.selectedRoutingEntry = Optional.of( adapters.get( 0 ) );
            log.info( "found placements: " + placements.size() );
            return placements;
        }

        // TODO :throw exception, should never happen
        return Collections.emptyMap();
    }

    @Override
    protected Set<List<CatalogColumnPlacement>> selectPlacement( RelNode node, CatalogTable table, Statement statement ){
        log.info( "Enter base select Placements" );
        if(this.useSimpleRouter){
            log.info( "Use simple router" );
            return simpleRouter.selectPlacement( node, table, statement );
        }

        if( !this.basePlacementInitialized ){
            log.info( "No base placement" );
            this.basePlacementInitialized = true;
            val placements = selectPlacement( );
            log.info( "Initialize base placement:" + placements.size() );
            return placements.values().stream().collect( Collectors.toSet());
        }else{
            log.info( "Base selectPlace: return empty list" );
            return Collections.emptySet();
        }

        //throw new UnsupportedOperationException("Not used in unified routing, should never be called");
    }

    private Map<CatalogTable, Set<Integer>> getFullPlacementsPerTable(Set<CatalogTable> allTables){
        // all full placements (required columns) for tables
        val fullPlacementsPerTable = new LinkedHashMap<CatalogTable, Set<Integer>>(); // catalogTable -> adapterIds
        for ( val table : allTables ){
            //val fullAdapterPlacements = new ArrayList<Integer>();
            val usedCols = this.usedCatalogColumnsPerTable.get( table ).stream().map( col -> col.id ).collect( Collectors.toList());
            val adapterWithFullPlacement = table.placementsByAdapter.entrySet()
                    .stream()
                    .filter( elem -> elem.getValue().containsAll( usedCols ) )
                    .map( adapter -> adapter.getKey() ) // adapterId
                    .collect( Collectors.toSet());

            fullPlacementsPerTable.putIfAbsent( table, adapterWithFullPlacement );
        }

        return fullPlacementsPerTable;
    }

    private Set<List<Integer>> getAvailablePlacements(List<Set<Integer>> placementsPerTable){
        Set<List<Integer>> availablePlacements;
        if(placementsPerTable.size() > 1){
            availablePlacements = Sets.cartesianProduct( placementsPerTable );
        }else{
            availablePlacements = Sets.newHashSet();
            for(val adapter : placementsPerTable.get( 0 ) ){
                availablePlacements.add( Lists.newArrayList(adapter));
            }
        }

        return availablePlacements;
    }

    private List<RoutingTableEntry> mapPlacementsToRoutingTableEntry( Set<List<Integer>> availablePlacements, List<CatalogTable> tables ) {
        val result = new ArrayList<RoutingTableEntry>();
        for (val placement : availablePlacements){
            Set<Integer> adapterIds = new HashSet<>();
            HashMap<Integer, Integer> tableToAdapterIdMapping = new HashMap<>(); // tableId -> adapterId
            for(int i = 0; i < placement.size(); i++){
                val tableId = tables.get( i );
                val adapterId = placement.get( i );
                adapterIds.add( adapterId );
                tableToAdapterIdMapping.put( (int)tableId.id, adapterId );
            }
            val routingEntry = RoutingTableEntry.builder()
                    .id(this.queryClassString)
                    .adapterIds(adapterIds)
                    .tableToAdapterIdMapping(tableToAdapterIdMapping)
                    .build();

            result.add( routingEntry );
        }

        return result;
    }

    private List<RoutingTableEntry> updateAvailableRoutingEntries() {
        log.info( "Update knownadapter" );
        // TODO
        // initialize threshold to define how many different adapters settings should be considered...
        // every adapterPlacement can only be defined once
        List<RoutingTableEntry> adapterPlacements = new ArrayList<>();

        // all used tables in query
        val allTables = this.usedCatalogColumnsPerTable.keySet();

        val fullPlacementsPerTable = this.getFullPlacementsPerTable( allTables );



        // TODO: check performance, otherwise do it in a more clever way.
        // Maybe needed to prefer adapter ids.
        // val mostPlacementsByAdapter = Multisets.copyHighestCountFirst( ImmutableMultiset.copyOf( adapterCounter )).asList();

        // get all availablePlacements
        // Calculate cartesian product of available table adapter mappings
        // TODO: add threshold?
        // will become pretty large with a lot of available mappings
        List<Set<Integer>> placementsPerTable = new ArrayList<>(fullPlacementsPerTable.values()); // list of adapterIds per table
        List<CatalogTable> tables = new ArrayList<>(fullPlacementsPerTable.keySet()); //

        val availablePlacements = this.getAvailablePlacements( placementsPerTable );

        // map adapter ids to RoutingTableEntry values
        val availableRoutingEntries = this.mapPlacementsToRoutingTableEntry(availablePlacements, tables);


        log.info( "found all adapter placements:" +  availableRoutingEntries.size());

        // initialize routing table
        routingTable.initializeRow( this.queryClassString, availableRoutingEntries );

        return availableRoutingEntries;
    }





    /*private RoutingTableEntry getPlacementForAdapter( Integer adapter, ImmutableList<Integer> mostPlacementsByAdapter, HashMap<CatalogTable, List<Integer>> allFullAdapterPlacementsPerTable ) {
        val allTables = this.usedCatalogColumnsPerTable.keySet();
        val result = new LinkedList<Integer>();
        result.add( adapter );

        for ( val table : allTables ){
            // check if placement already satisfied
            if( CollectionUtils.containsAny( allFullAdapterPlacementsPerTable.get( table ), result  )){
                // placement of table already satisfied
                continue;
            }

            // check if there is a available full placement for the table
            if(!allFullAdapterPlacementsPerTable.get( table ).isEmpty()) {
                // there is at least one full placement of the table, get the one with most full placements:
                val allPossibleAdapter = allFullAdapterPlacementsPerTable.get( table );
                val adapterWithMostPlacements = mostPlacementsByAdapter.stream().filter( elem -> allPossibleAdapter.contains( elem )  ).findFirst();
                if(adapterWithMostPlacements.isPresent()){
                    result.add(adapterWithMostPlacements.get());
                }
                else{
                    // throw exception
                }
            }
            // no full placement available.
            else{
                // get placements by already collected adapters
                val usedCols = this.usedCatalogColumnsPerTable.get( table );

                // iterate over column placements
                for ( val col : usedCols ){
                    val allPlacements = Catalog.getInstance().getColumnPlacements( col.id );
                    val satisfiedPlacement = allPlacements.stream().filter( elem -> result.contains( elem.adapterId ) ).findFirst();
                    // already satisfied
                    if(satisfiedPlacement.isPresent()){
                        continue;
                    }

                    // get next adapter, ordered by most placements
                    val mostPlacement = allPlacements.stream().filter( elem -> mostPlacementsByAdapter.contains( elem.adapterId ) ).findFirst();
                    if(mostPlacement.isPresent()){
                        result.add( mostPlacement.get().adapterId );
                    }else{
                        // throw exception
                    }
                }
            }
        }

        return result;
    }*/


    // Create table on all data stores (not on data sources)
    public List<DataStore> createTable( long schemaId, Statement statement ) {
        Map<String, DataStore> availableStores = AdapterManager.getInstance().getStores();
        List<DataStore> result = new LinkedList<>( availableStores.values() );
        if ( result.size() == 0 ) {
            throw new RuntimeException( "No suitable data store found" );
        }
        return ImmutableList.copyOf( result );
    }


    public List<DataStore> addColumn( CatalogTable catalogTable, Statement statement ) {
        List<DataStore> result = new LinkedList<>();
        for ( int storeId : catalogTable.placementsByAdapter.keySet() ) {
            result.add( AdapterManager.getInstance().getStore( storeId ) );
        }
        if ( result.size() == 0 ) {
            throw new RuntimeException( "No suitable data store found" );
        }
        return ImmutableList.copyOf( result );
    }


    public void dropPlacements( List<CatalogColumnPlacement> placements ) {
        routingTable.dropPlacements( placements );
    }


    private static class UnifiedRoutingTable implements ExecutionTimeObserver {

        public static final int MISSING_VALUE = -1;
        public static final int NO_PLACEMENT = -2;

        private final Map<String, Map<RoutingTableEntry, Integer>> routingTable = new ConcurrentHashMap<>(); // QueryClassStr -> (RoutingEntry -> Percentage)
        private final Map<String, Set<Integer>> knownAdapterTableMappings = new HashMap<>(); // Adapter Id -> TableIds
        // private final Map<List<Integer>, String> knownAdapters = new HashMap<>(); // Adapter Id -> Adapter Name

        //private final Map<String, Map<List<Integer>, Integer>> routingTable = new ConcurrentHashMap<>();  // QueryClassStr -> (Adapter -> Percentage)
        //private final Map<List<Integer>, String> knownAdapters = new HashMap<>(); // Adapter Id -> Adapter Name
        //private final Map<String, Map<Set<Integer>, CircularFifoQueue<Double>>> times = new ConcurrentHashMap<>();  // QueryClassStr -> (Adapter -> Time)

        private final Lock processingQueueLock = new ReentrantLock();


        private UnifiedRoutingTable() {
            this.initializeUi();
            this.registerBackgroundTask();
        }


        private void initializeUi() {
            // Information
            InformationManager im = InformationManager.getInstance();
            InformationPage page = new InformationPage( "Unified Routing" );
            page.fullWidth();
            im.addPage( page );
            // Routing table
            InformationGroup routingTableGroup = new InformationGroup( page, "Routing Table" );
            im.addGroup( routingTableGroup );
            InformationTable routingTableElement = new InformationTable(
                    routingTableGroup,
                    Arrays.asList( "Query Class" ) );
            im.registerInformation( routingTableElement );

            // update
            page.setRefreshFunction( () -> {
                // Update labels
                if ( routingTable.size() > 0 ) {
                    LinkedList<String> labels = new LinkedList<>();
                    labels.add( "Query Class" );
                    labels.add( "1" );
                    labels.add( "2" );
                    labels.add( "3" );
                    labels.add( "4" );
                    routingTableElement.updateLabels( labels );
                }
                // Update rows
                routingTableElement.reset();
                routingTable.forEach( ( k, v ) -> {
                    List<String> row = new LinkedList<>();
                    row.add( k );
                    val maxElement = 4;
                    val allEntries = v.entrySet().stream().sorted(Map.Entry.comparingByValue()).limit( maxElement ).collect( Collectors.toList());
                    for ( val routingEntry : allEntries ) {

                        if ( routingEntry.getValue() == UnifiedRoutingTable.MISSING_VALUE ) {
                            row.add( "Missing value" );
                        } else if ( routingEntry.getValue() == UnifiedRoutingTable.NO_PLACEMENT ) {
                            row.add( "-" );
                        } else {
                            row.add( routingEntry.getValue() + "" );
                        }
                    }

                    routingTableElement.addRow( row );
                } );
            } );
        }


        private void registerBackgroundTask(){
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


        public Map<RoutingTableEntry, Integer> get( String queryClassStr ) {
            return routingTable.get( queryClassStr );
        }

        public Map<RoutingTableEntry, List<Double>> getExecutionTimes(String queryClassString){
            val points = MonitoringServiceProvider.getInstance().getRoutingDataPoints( queryClassString );

            val mapped = points.stream()
                    .map( elem -> (RoutingDataPoint)elem )
                    .collect( Collectors.toList());


            if(!mapped.isEmpty()){
                val tableToAdapterIdMappingTimes =   mapped.stream()
                        .collect(
                                Collectors.groupingBy(
                                        elem -> RoutingTableEntry.builder()
                                                .id(elem.getQueryClassString())
                                                .tableToAdapterIdMapping(elem.getTableToAdapterIdMapping())
                                                .adapterIds(elem.getAdapterIds())
                                                .build(),
                                        Collectors.mapping(elem -> elem.getNanoTime(), Collectors.toList())
                                )
                        );

                return tableToAdapterIdMappingTimes;
            }

            return Collections.emptyMap();
        }


        private void process() {
            processingQueueLock.lock();

            // Update routing table
            // get values from monitoring
            for ( String queryClass : routingTable.keySet() ) {
                Map<RoutingTableEntry, Double> meanTimeRow = new HashMap<>();

                for ( Map.Entry<RoutingTableEntry, List<Double>> entry : this.getExecutionTimes( queryClass ).entrySet() ) {
                    double mean = StatUtils.mean(
                            entry.getValue().stream()
                                    .mapToDouble( d -> d )
                                    .toArray(),0,entry.getValue().size() );

                    //val result = routingTable.get( queryClass  ).get( entry.getKey() );
                    //if(result != null){
                    //}
                    meanTimeRow.put( entry.getKey(), mean );

                }
                // nothing to update
                if(meanTimeRow.isEmpty()){
                    processingQueueLock.unlock();
                    return;
                }

                Map<RoutingTableEntry, Integer> newRow = new HashMap<>();

                Map<RoutingTableEntry, Integer> calculatedRow = generateRow( meanTimeRow );
                for ( Map.Entry<RoutingTableEntry, Integer> oldEntry : routingTable.get( queryClass ).entrySet() ) {
                    if ( oldEntry.getValue() == NO_PLACEMENT ) {
                        newRow.put( oldEntry.getKey(), NO_PLACEMENT );
                    } else if ( calculatedRow.containsKey( oldEntry.getKey() ) ) {
                        newRow.put( oldEntry.getKey(), calculatedRow.get( oldEntry.getKey() ) );
                    } else {
                        newRow.put( oldEntry.getKey(), MISSING_VALUE );
                    }
                }
                routingTable.replace( queryClass, newRow );
            }
            processingQueueLock.unlock();
        }

        // called by execution monitor to inform about execution time
        @Override
        public void executionTime( String reference, long nanoTime ) {
            RoutingTableEntry routingEntry = new Gson().fromJson(reference, RoutingTableEntry.class);
            MonitoringServiceProvider.getInstance().monitorEvent( new RoutingEvent(routingEntry.id, routingEntry.adapterIds, routingEntry.tableToAdapterIdMapping, nanoTime) );
        }


        public void dropPlacements( List<CatalogColumnPlacement> placements ) {
            process();// empty processing queue
            processingQueueLock.lock();

            // TODO: make it more performant
            for ( CatalogColumnPlacement placement : placements ) {
                for(val entries : routingTable.values()){
                    for(val routingEntry : entries.keySet()){
                        if (routingEntry.adapterIds.contains( placement.adapterId ) &&
                                routingEntry.tableToAdapterIdMapping.get( placement.tableId ) == placement.adapterId){
                            entries.remove( routingEntry );
                        }
                    }
                }

            }
            processingQueueLock.unlock();
            process();// update routing table
        }


        public void initializeRow( String queryClassString, List<RoutingTableEntry> routingTableEntries ) {
            Map<RoutingTableEntry, Integer> row = new HashMap<>();

            for(val routingEntry : routingTableEntries){
                row.put( routingEntry, MISSING_VALUE );
            }

            routingTable.put( queryClassString, row );
        }


        protected Map<RoutingTableEntry, Integer> generateRow( Map<RoutingTableEntry, Double> map ) {
            Map<RoutingTableEntry, Integer> row;
            // find fastest
            Optional<RoutingTableEntry> fastestEntry = Optional.empty();
            double fastestTime = Double.MAX_VALUE;
            for ( Map.Entry<RoutingTableEntry, Double> entry : map.entrySet() ) {
                if ( fastestTime > entry.getValue() ) {
                    fastestEntry = Optional.of( entry.getKey() );
                    fastestTime = entry.getValue();
                }
            }
            long shortRunningLongRunningThreshold = SHORT_RUNNING_LONG_RUNNING_THRESHOLD.getInt() * 1_000_000L; // multiply with 1000000 to get nanoseconds
            if ( fastestTime < shortRunningLongRunningThreshold && SHORT_RUNNING_SIMILAR_THRESHOLD.getInt() != 0 && fastestEntry.isPresent()) {
                row = calc( map, SHORT_RUNNING_SIMILAR_THRESHOLD.getInt(), fastestTime, fastestEntry.get() );
            } else if ( fastestTime > shortRunningLongRunningThreshold && LONG_RUNNING_SIMILAR_THRESHOLD.getInt() != 0  && fastestEntry.isPresent()) {
                row = calc( map, LONG_RUNNING_SIMILAR_THRESHOLD.getInt(), fastestTime, fastestEntry.get() );
            } else {
                row = new HashMap<>();
                // init row with 0
                for ( val adapterIds : map.keySet() ) {
                    row.put( adapterIds, 0 );
                }
                if (fastestEntry.isPresent() && fastestTime > 0 ) {
                    row.put( fastestEntry.get(), 100 );
                }
            }
            return row;
        }


        private Map<RoutingTableEntry, Integer> calc( Map<RoutingTableEntry, Double> map, int similarThreshold, double fastestTime, RoutingTableEntry fastestStore ) {
            ArrayList<Integer> percents = new ArrayList<>();
            Map<RoutingTableEntry, Integer> result = new HashMap<>();

            // init all routing entries with 0
            for ( val mapEntry : map.keySet() ) {
                result.put(mapEntry, 0);
            }

            // calc 100%
            int threshold = (int) (fastestTime + (fastestTime * (similarThreshold / 100.0)));
            val hundredPercent = map.values().stream().filter( time -> threshold >= time ).reduce( (a,b) -> a + b );
            val hundredPercentValue = hundredPercent.isPresent() ? hundredPercent.get() : 0;
            if(!hundredPercent.isPresent()){
                log.error( "should never happen, no values available." );
                throw new RuntimeException("Could no calculated routing percentage");
            }

            // calc percents
            double onePercent = hundredPercentValue / 100.0;
            for ( Map.Entry<RoutingTableEntry, Double> entry : map.entrySet() ) {
                if ( threshold >= entry.getValue() ) {
                    double d = entry.getValue().intValue() / onePercent;
                    int t = Math.min( (int) d, 100 ); // This is not nice... But if there is only one entry with 100 percent, it some time happens that we get 101
                    percents.add( t );
                    //stores.put( entry.getKey(), entry.getValue() );
                    result.put( entry.getKey(), t );
                }
            }

            val total = percents.stream().reduce( (a, b) -> a + b );
            if(total.isPresent() && total.get() < 100){
                int delta = 100 - total.get();
                result.replace( fastestStore, result.get( fastestStore ) + delta );
            }

            val sortedResult = entriesSortedByValues( result );
            return sortedResult;

            // TODO: is this cleanup needed?
            // add
            /*Collections.sort( percents );
            Collections.reverse( percents );
            val sortedStores = entriesSortedByValues( stores );


            for ( Map.Entry<RoutingTableEntry, Double> entry : sortedStores.entrySet() ) {
                row.put( entry.getKey(), percents.remove( 0 ) );
            }
            // normalize to 100
            int sum = 0;
            for ( Map.Entry<RoutingTableEntry, Integer> entry : row.entrySet() ) {
                sum += entry.getValue();
            }
            if ( sum == 0 ) {
                log.error( "Routing table row is empty! This should not happen!" );
            } else if ( sum > 100 ) {
                log.error( "Routing table row does sum up to a value greater 100! This should not happen! The value is: " + sum + " | Entries: " + row.values().toString() );
            } else if ( sum < 100 ) {
                if ( fastestStore != null &&  fastestStore.adapterIds.isEmpty() ) {
                    log.error( "Fastest Store is -1! This should not happen!" );
                } else if ( !row.containsKey( fastestStore ) ) {
                    log.error( "Row does not contain the fastest row! This should not happen!" );
                } else {
                    int delta = 100 - sum;
                    row.replace( fastestStore, row.get( fastestStore ) + delta );
                }
            }
            return row;*/
        }


        //http://stackoverflow.com/a/2864923
        private static <K, V extends Comparable<? super V>> Map<K, V> entriesSortedByValues( Map<K, V> map ) {
            val sorted = map.entrySet().stream().
                    sorted(Entry.comparingByValue()).collect( Collectors.toList());

            val result = sorted.stream().collect( Collectors.toMap( Entry::getKey, Entry::getValue,
                    (e1, e2) -> e1, TreeMap::new) );

            return result;
        }
    }

    /*public static class UnifiedRouterFactory extends RouterFactory {

        public UnifiedRouterFactory() {
            super();
            final ConfigManager configManager = ConfigManager.getInstance();
            // Only initialize ones
            if ( configManager.getConfig( TRAINING.getKey() ) == null ) {
                final WebUiGroup unifiedGroupGroup = new WebUiGroup( "unifiedGroup", RouterManager.getInstance().routingPage.getId(), 2 );
                unifiedGroupGroup.withTitle( "Unified Routing" );
                configManager.registerWebUiGroup( unifiedGroupGroup );

                configManager.registerConfig( TRAINING );
                TRAINING.withUi( unifiedGroupGroup.getId() );

                configManager.registerConfig( WINDOW_SIZE );
                WINDOW_SIZE.withUi( unifiedGroupGroup.getId() );

                configManager.registerConfig( SHORT_RUNNING_SIMILAR_THRESHOLD );
                SHORT_RUNNING_SIMILAR_THRESHOLD.withUi( unifiedGroupGroup.getId() );

                configManager.registerConfig( LONG_RUNNING_SIMILAR_THRESHOLD );
                LONG_RUNNING_SIMILAR_THRESHOLD.withUi( unifiedGroupGroup.getId() );

                configManager.registerConfig( SHORT_RUNNING_LONG_RUNNING_THRESHOLD );
                SHORT_RUNNING_LONG_RUNNING_THRESHOLD.withUi( unifiedGroupGroup.getId() );

                configManager.registerConfig( QUERY_CLASS_PROVIDER );
                QUERY_CLASS_PROVIDER.withUi( unifiedGroupGroup.getId() );
            }
        }


        @Override
        public Router createInstance() {
            return new UnifiedRouting();
        }

    }*/

}
