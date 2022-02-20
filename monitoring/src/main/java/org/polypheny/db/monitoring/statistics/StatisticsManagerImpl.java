/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.monitoring.statistics;


import com.google.common.collect.Lists;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeSupport;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.TableScan;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.LogicalAggregate;
import org.polypheny.db.algebra.logical.LogicalProject;
import org.polypheny.db.algebra.logical.LogicalSort;
import org.polypheny.db.algebra.logical.LogicalTableScan;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationAction.Action;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.DateTimeStringUtils;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.background.BackgroundTask.TaskPriority;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;
import org.polypheny.db.util.background.BackgroundTaskManager;


/**
 * Stores all available statistics and updates INSERTs dynamically
 * DELETEs and UPDATEs should wait to be reprocessed
 */
@Slf4j
public class StatisticsManagerImpl<T extends Comparable<T>> extends StatisticsManager<T> {

    private static StatisticQueryProcessor statisticQueryInterface;

    private final ExecutorService threadPool = Executors.newSingleThreadExecutor();

    protected final PropertyChangeSupport listeners = new PropertyChangeSupport( this );

    private int buffer = RuntimeConfig.STATISTIC_BUFFER.getInteger();

    @Setter
    @Getter
    private String revalId = null;

    private DashboardInformation dashboardInformation;

    @Getter
    private final Map<Long, StatisticTable<T>> tableStatistic;

    @Getter
    private volatile Map<Long, Map<Long, Map<Long, StatisticColumn<T>>>> statisticSchemaMap;

    private final Queue<Long> tablesToUpdate = new ConcurrentLinkedQueue<>();

    private Transaction transaction;
    private Statement statement;


    public StatisticsManagerImpl( StatisticQueryProcessor statisticQueryProcessor ) {
        this.setQueryInterface( statisticQueryProcessor );
        this.statisticSchemaMap = new ConcurrentHashMap<>();
        this.tableStatistic = new ConcurrentHashMap<>();

        this.listeners.addPropertyChangeListener( this );
    }


    @Override
    public void initializeStatisticSettings() {
        this.dashboardInformation = new DashboardInformation();
        displayInformation();
        registerTaskTracking();
        registerIsFullTracking();

        if ( RuntimeConfig.STATISTICS_ON_STARTUP.getBoolean() ) {
            this.asyncReevaluateAllStatistics();
        }
    }


    @Override
    public void updateColumnName( CatalogColumn catalogColumn, String newName ) {
        if ( statisticSchemaMap.containsKey( catalogColumn.schemaId )
                && statisticSchemaMap.get( catalogColumn.schemaId ).containsKey( catalogColumn.tableId )
                && statisticSchemaMap.get( catalogColumn.schemaId ).get( catalogColumn.tableId ).containsKey( catalogColumn.id ) ) {
            StatisticColumn<T> statisticColumn = statisticSchemaMap.get( catalogColumn.schemaId ).get( catalogColumn.tableId ).get( catalogColumn.id );
            statisticColumn.updateColumnName( newName );
            statisticSchemaMap.get( catalogColumn.schemaId ).get( catalogColumn.tableId ).put( catalogColumn.id, statisticColumn );
        }
    }


    @Override
    public void updateTableName( CatalogTable catalogTable, String newName ) {
        if ( statisticSchemaMap.containsKey( catalogTable.schemaId ) && statisticSchemaMap.get( catalogTable.schemaId ).containsKey( catalogTable.id ) ) {
            Map<Long, StatisticColumn<T>> columnsInformation = statisticSchemaMap.get( catalogTable.schemaId ).get( catalogTable.id );
            for ( Entry<Long, StatisticColumn<T>> columnInfo : columnsInformation.entrySet() ) {
                StatisticColumn<T> statisticColumn = columnInfo.getValue();
                statisticColumn.updateTableName( newName );
                statisticSchemaMap.get( catalogTable.schemaId ).get( catalogTable.id ).put( columnInfo.getKey(), statisticColumn );
            }
        }
        if ( tableStatistic.containsKey( catalogTable.id ) ) {
            StatisticTable<T> tableStatistics = tableStatistic.get( catalogTable.id );
            tableStatistics.updateTableName( newName );
            tableStatistic.put( catalogTable.id, tableStatistics );
        }
    }


    @Override
    public void updateSchemaName( CatalogSchema catalogSchema, String newName ) {
        if ( statisticSchemaMap.containsKey( catalogSchema.id ) ) {
            Map<Long, Map<Long, StatisticColumn<T>>> tableInformation = statisticSchemaMap.get( catalogSchema.id );
            for ( long tableId : tableInformation.keySet() ) {
                Map<Long, StatisticColumn<T>> columnsInformation = statisticSchemaMap.get( catalogSchema.id ).remove( tableId );
                for ( Entry<Long, StatisticColumn<T>> columnInfo : columnsInformation.entrySet() ) {
                    StatisticColumn<T> statisticColumn = columnInfo.getValue();
                    statisticColumn.updateSchemaName( newName );
                    statisticSchemaMap.get( catalogSchema.id ).get( tableId ).put( columnInfo.getKey(), statisticColumn );
                }
            }
        }
    }


    private Transaction getTransaction() {
        Transaction transaction = null;
        try {
            transaction = statisticQueryInterface.getTransactionManager().startTransaction( "pa", "APP", false, "Statistic Manager" );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( e );
        }
        return transaction;
    }


    public void setQueryInterface( StatisticQueryProcessor statisticQueryProcessor ) {
        statisticQueryInterface = statisticQueryProcessor;
    }


    /**
     * Registers if on configChange statistics are tracked and displayed or not.
     */
    private void registerTaskTracking() {
        TrackingListener listener = new TrackingListener();
        RuntimeConfig.PASSIVE_TRACKING.addObserver( listener );
        RuntimeConfig.DYNAMIC_QUERYING.addObserver( listener );
    }


    /**
     * Registers the isFull reevaluation on config change.
     */
    private void registerIsFullTracking() {
        ConfigListener listener = new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                buffer = c.getInt();
                resetAllIsFull();
            }


            @Override
            public void restart( Config c ) {
                buffer = c.getInt();
                resetAllIsFull();
            }
        };
        RuntimeConfig.STATISTIC_BUFFER.addObserver( listener );
    }


    private void resetAllIsFull() {
        this.statisticSchemaMap.values().forEach( s -> s.values().forEach( t -> t.values().forEach( c -> {
            assignUnique( c, this.prepareNode( new QueryResult( c.getSchemaId(), c.getTableId(), c.getColumnId(), c.getType() ), NodeType.UNIQUE_VALUE ) );
        } ) ) );
    }


    /**
     * Reset all statistics and reevaluate them.
     */
    private void reevaluateAllStatistics() {
        if ( statisticQueryInterface == null ) {
            return;
        }
        log.debug( "Resetting StatisticManager." );
        Map<Long, Map<Long, Map<Long, StatisticColumn<T>>>> statisticSchemaMapCopy = new ConcurrentHashMap<>();
        transaction = getTransaction();
        statement = transaction.createStatement();
        statement.getQueryProcessor().lock( statement );

        for ( QueryResult column : statisticQueryInterface.getAllColumns() ) {
            StatisticColumn<T> col = reevaluateColumn( column );
            if ( col != null ) {
                put( statisticSchemaMapCopy, column, col );
            }
        }
        reevaluateRowCount();
        replaceStatistics( statisticSchemaMapCopy );
        log.debug( "Finished resetting StatisticManager." );
        statisticQueryInterface.commitTransaction( transaction, statement );
    }


    /**
     * Update the row count for all tables.
     */
    private void reevaluateRowCount() {
        if ( statisticQueryInterface == null ) {
            return;
        }
        log.debug( "Reevaluate Row Count." );

        statisticQueryInterface.getAllTable().forEach( table -> {
            int rowCount = getNumberColumnCount( this.prepareNode( new QueryResult( table.schemaId, table.id, null, null ), NodeType.ROW_COUNT_TABLE ) );
            updateRowCountPerTable( table.id, rowCount, "SET-ROW-COUNT" );
        } );
    }


    /**
     * Gets a columns of a table and reevaluates them.
     *
     * @param tableId id of table
     */
    @Override
    public void reevaluateTable( long tableId ) {
        transaction = getTransaction();
        statement = transaction.createStatement();
        statement.getQueryProcessor().lock( statement );

        if ( statisticQueryInterface == null ) {
            return;
        }
        if ( Catalog.getInstance().checkIfExistsTable( tableId ) ) {
            deleteTable( Catalog.getInstance().getTable( tableId ).schemaId, tableId );

            List<QueryResult> res = statisticQueryInterface.getAllColumns( tableId );

            for ( QueryResult column : res ) {
                StatisticColumn<T> col = reevaluateColumn( column );
                if ( col != null ) {
                    put( column, col );
                }
            }
        }
        statisticQueryInterface.commitTransaction( transaction, statement );
    }


    private void deleteTable( long schemaId, long tableId ) {
        if ( this.statisticSchemaMap.get( schemaId ) != null ) {
            this.statisticSchemaMap.get( schemaId ).remove( tableId );
        }
    }


    /**
     * Replace the tracked statistics with new statistics.
     */
    private synchronized void replaceStatistics( Map<Long, Map<Long, Map<Long, StatisticColumn<T>>>> map ) {
        this.statisticSchemaMap = new ConcurrentHashMap<>( map );
    }


    /**
     * Method to sort a column into the different kinds of column types and hands it to the specific reevaluation
     */
    private StatisticColumn<T> reevaluateColumn( QueryResult column ) {
        if ( !Catalog.getInstance().checkIfExistsTable( column.getTableId() )
                && !Catalog.getInstance().checkIfExistsColumn( column.getTableId(), column.getColumn() ) ) {
            return null;
        }

        if ( column.getType().getFamily() == PolyTypeFamily.NUMERIC ) {
            return this.reevaluateNumericalColumn( column );
        } else if ( column.getType().getFamily() == PolyTypeFamily.CHARACTER ) {
            return this.reevaluateAlphabeticalColumn( column );
        } else if ( PolyType.DATETIME_TYPES.contains( column.getType() ) ) {
            return this.reevaluateTemporalColumn( column );
        }
        return null;
    }


    /**
     * Reevaluates a numerical column, with the configured statistics.
     */
    private StatisticColumn<T> reevaluateNumericalColumn( QueryResult column ) {
        StatisticQueryResult min = this.prepareNode( column, NodeType.MIN );
        StatisticQueryResult max = this.prepareNode( column, NodeType.MAX );
        Integer count = getNumberColumnCount( this.prepareNode( column, NodeType.ROW_COUNT_COLUMN ) );
        NumericalStatisticColumn<T> statisticColumn = new NumericalStatisticColumn<>( column );
        if ( min != null ) {
            //noinspection unchecked
            statisticColumn.setMin( (T) min.getData()[0] );
        }
        if ( max != null ) {
            //noinspection unchecked
            statisticColumn.setMax( (T) max.getData()[0] );
        }

        StatisticQueryResult unique = this.prepareNode( column, NodeType.UNIQUE_VALUE );
        assignUnique( statisticColumn, unique );

        statisticColumn.setCount( count );

        return statisticColumn;
    }


    /**
     * Reevaluates a temporal column.
     */
    private StatisticColumn<T> reevaluateTemporalColumn( QueryResult column ) {
        StatisticQueryResult min = this.prepareNode( column, NodeType.MIN );
        StatisticQueryResult max = this.prepareNode( column, NodeType.MAX );
        Integer count = getNumberColumnCount( this.prepareNode( column, NodeType.ROW_COUNT_COLUMN ) );

        TemporalStatisticColumn<T> statisticColumn = new TemporalStatisticColumn<>( column );
        if ( min != null ) {
            if ( max.getData()[0] instanceof Integer ) {
                statisticColumn.setMin( (T) new Date( (Integer) min.getData()[0] ) );
            } else if ( max.getData()[0] instanceof Long ) {
                statisticColumn.setMin( (T) new Timestamp( (Long) min.getData()[0] ) );
            } else {
                statisticColumn.setMin( (T) min.getData()[0] );
            }
        }

        if ( max != null ) {
            if ( max.getData()[0] instanceof Integer ) {
                statisticColumn.setMax( (T) new Date( (Integer) max.getData()[0] ) );
            } else if ( max.getData()[0] instanceof Long ) {
                statisticColumn.setMax( (T) new Timestamp( (Long) max.getData()[0] ) );
            } else {
                statisticColumn.setMax( (T) max.getData()[0] );
            }
        }

        StatisticQueryResult unique = this.prepareNode( column, NodeType.UNIQUE_VALUE );
        if ( unique != null ) {
            for ( int idx = 0; idx < unique.getData().length; idx++ ) {
                unique.getData()[idx] = DateTimeStringUtils.longToAdjustedString( (Number) unique.getData()[idx], column.getType() );

            }
        }

        assignUnique( statisticColumn, unique );
        statisticColumn.setCount( count );

        return statisticColumn;
    }


    /**
     * Helper method tho assign unique values or set isFull if too much exist
     *
     * @param column the column in which the values should be inserted
     */
    private void assignUnique( StatisticColumn<T> column, StatisticQueryResult unique ) {
        if ( unique == null || unique.getData() == null ) {
            return;
        }
        if ( unique.getData().length <= this.buffer ) {
            column.setUniqueValues( Lists.newArrayList( (T[]) unique.getData() ) );
        } else {
            column.setFull( true );
        }
    }


    /**
     * Reevaluates an alphabetical column, with the configured statistics
     */
    private StatisticColumn<T> reevaluateAlphabeticalColumn( QueryResult column ) {
        StatisticQueryResult unique = this.prepareNode( column, NodeType.UNIQUE_VALUE );
        Integer count = getNumberColumnCount( this.prepareNode( column, NodeType.ROW_COUNT_COLUMN ) );

        AlphabeticStatisticColumn<T> statisticColumn = new AlphabeticStatisticColumn<>( column );
        assignUnique( statisticColumn, unique );
        statisticColumn.setCount( count );

        return statisticColumn;
    }


    private Integer getNumberColumnCount( StatisticQueryResult countColumn ) {
        if ( countColumn != null && countColumn.getData() != null && countColumn.getData().length != 0 ) {
            Object value = countColumn.getData()[0];
            if ( value instanceof Integer ) {
                return (Integer) value;
            } else if ( value instanceof Long ) {
                return ((Long) value).intValue();
            } else {
                try {
                    return Integer.parseInt( value.toString() );
                } catch ( NumberFormatException e ) {
                    log.error( "Count could not be parsed for column {}.", countColumn.getColumn(), e );
                }
            }
        }
        return 0;
    }


    private void put( QueryResult queryResult, StatisticColumn<T> statisticColumn ) {
        put( this.statisticSchemaMap,
                queryResult.getSchemaId(),
                queryResult.getTableId(),
                queryResult.getColumnId(),
                statisticColumn );
    }


    private void put(
            Map<Long, Map<Long, Map<Long, StatisticColumn<T>>>> statisticSchemaMapCopy,
            QueryResult queryResult,
            StatisticColumn<T> statisticColumn ) {
        put( statisticSchemaMapCopy,
                queryResult.getSchemaId(),
                queryResult.getTableId(),
                queryResult.getColumnId(),
                statisticColumn );
    }


    /**
     * Places a column at the correct position in the schemaMap.
     */
    private void put(
            Map<Long, Map<Long, Map<Long, StatisticColumn<T>>>> map,
            long schemaId,
            long tableId,
            long columnId,
            StatisticColumn<T> statisticColumn ) {
        if ( !map.containsKey( schemaId ) ) {
            map.put( schemaId, new HashMap<>() );
        }
        if ( !map.get( schemaId ).containsKey( tableId ) ) {
            map.get( schemaId ).put( tableId, new HashMap<>() );
        }
        map.get( schemaId ).get( tableId ).put( columnId, statisticColumn );

        if ( !tableStatistic.containsKey( tableId ) ) {
            tableStatistic.put( tableId, new StatisticTable<T>( tableId ) );
        }
    }


    private StatisticQueryResult prepareNode( QueryResult queryResult, NodeType nodeType ) {

        PolyphenyDbCatalogReader reader = statement.getTransaction().getCatalogReader();
        AlgBuilder relBuilder = AlgBuilder.create( statement );
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();
        final AlgOptCluster cluster = AlgOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder );

        if ( Catalog.getInstance().checkIfExistsTable( queryResult.getTableId() ) ) {
            LogicalTableScan tableScan = getLogicalTableScan( queryResult.getSchema(), queryResult.getTable(), reader, cluster );

            StatisticQueryResult statisticQueryColumn;
            switch ( nodeType ) {
                case MIN:
                case MAX:
                    statisticQueryColumn = getAggregateColumn( queryResult, nodeType, tableScan, rexBuilder, cluster, transaction, statement );
                    return statisticQueryColumn;
                case UNIQUE_VALUE:
                    statisticQueryColumn = getUniqueValues( queryResult, tableScan, rexBuilder, transaction, statement );
                    return statisticQueryColumn;
                case ROW_COUNT_COLUMN:
                    statisticQueryColumn = getColumnCount( queryResult, tableScan, rexBuilder, cluster, transaction, statement );
                    return statisticQueryColumn;
                case ROW_COUNT_TABLE:
                    statisticQueryColumn = getTableCount( queryResult, tableScan, cluster, transaction, statement );
                    return statisticQueryColumn;
                default:
                    throw new RuntimeException( "Used nodeType is not defined in statistics." );
            }
        }
        return null;
    }


    /**
     * Gets a tableScan for a given table.
     */
    private LogicalTableScan getLogicalTableScan( String schema, String table, CatalogReader reader, AlgOptCluster cluster ) {
        AlgOptTable relOptTable = reader.getTable( Arrays.asList( schema, table ) );
        return LogicalTableScan.create( cluster, relOptTable );
    }


    /**
     * Queries the database with an aggregate query, to get the min value or max value.
     */
    private StatisticQueryResult getAggregateColumn( QueryResult queryResult, NodeType nodeType, TableScan tableScan, RexBuilder rexBuilder, AlgOptCluster cluster, Transaction transaction, Statement statement ) {
        for ( int i = 0; i < tableScan.getRowType().getFieldNames().size(); i++ ) {
            if ( queryResult.getColumn() != null && tableScan.getRowType().getFieldNames().get( i ).equals( queryResult.getColumn() ) ) {
                LogicalProject logicalProject = LogicalProject.create(
                        tableScan,
                        Collections.singletonList( rexBuilder.makeInputRef( tableScan, i ) ),
                        Collections.singletonList( tableScan.getRowType().getFieldNames().get( i ) ) );

                AggFunction operator = null;
                if ( nodeType == NodeType.MAX ) {
                    operator = OperatorRegistry.getAgg( OperatorName.MAX );
                } else if ( nodeType == NodeType.MIN ) {
                    operator = OperatorRegistry.getAgg( OperatorName.MIN );
                } else {
                    throw new RuntimeException( "Unknown aggregate is used in Statistic Manager." );
                }

                AlgDataType relDataType = logicalProject.getRowType().getFieldList().get( 0 ).getType();
                AlgDataType dataType;
                if ( relDataType.getPolyType() == PolyType.DECIMAL ) {
                    dataType = cluster.getTypeFactory().createTypeWithNullability(
                            cluster.getTypeFactory().createPolyType( relDataType.getPolyType(), relDataType.getPrecision(), relDataType.getScale() ),
                            true );
                } else {
                    dataType = cluster.getTypeFactory().createTypeWithNullability(
                            cluster.getTypeFactory().createPolyType( relDataType.getPolyType() ),
                            true );
                }

                AggregateCall aggregateCall = AggregateCall.create(
                        operator,
                        false,
                        false,
                        Collections.singletonList( 0 ),
                        -1,
                        AlgCollations.EMPTY,
                        dataType,
                        "min-max" );

                AlgNode relNode = LogicalAggregate.create(
                        logicalProject,
                        ImmutableBitSet.of(),
                        Collections.singletonList( ImmutableBitSet.of() ),
                        Collections.singletonList( aggregateCall ) );

                return statisticQueryInterface.selectOneColumnStat( relNode, transaction, statement, queryResult );
            }
        }
        return null;
    }


    private StatisticQueryResult getUniqueValues( QueryResult queryResult, TableScan tableScan, RexBuilder rexBuilder, Transaction transaction, Statement statement ) {

        for ( int i = 0; i < tableScan.getRowType().getFieldNames().size(); i++ ) {
            if ( queryResult.getColumn() != null && tableScan.getRowType().getFieldNames().get( i ).equals( queryResult.getColumn() ) ) {
                LogicalProject logicalProject = LogicalProject.create(
                        tableScan,
                        Collections.singletonList( rexBuilder.makeInputRef( tableScan, i ) ),
                        Collections.singletonList( tableScan.getRowType().getFieldNames().get( i ) ) );

                LogicalAggregate logicalAggregate = LogicalAggregate.create(
                        logicalProject, ImmutableBitSet.of( 0 ),
                        Collections.singletonList( ImmutableBitSet.of( 0 ) ),
                        Collections.emptyList() );

                Pair<BigDecimal, PolyType> valuePair = new Pair<>( new BigDecimal( (int) 6 ), PolyType.DECIMAL );

                AlgNode relNode = LogicalSort.create(
                        logicalAggregate,
                        AlgCollations.of(),
                        null,
                        new RexLiteral( valuePair.left, rexBuilder.makeInputRef( tableScan, i ).getType(), valuePair.right ) );

                return statisticQueryInterface.selectOneColumnStat( relNode, transaction, statement, queryResult );
            }
        }
        return null;
    }


    /**
     * Gets the amount of entries for a column
     */
    private StatisticQueryResult getColumnCount( QueryResult queryResult, TableScan tableScan, RexBuilder rexBuilder, AlgOptCluster cluster, Transaction transaction, Statement statement ) {
        for ( int i = 0; i < tableScan.getRowType().getFieldNames().size(); i++ ) {
            if ( queryResult.getColumn() != null && tableScan.getRowType().getFieldNames().get( i ).equals( queryResult.getColumn() ) ) {
                LogicalProject logicalProject = LogicalProject.create(
                        tableScan,
                        Collections.singletonList( rexBuilder.makeInputRef( tableScan, i ) ),
                        Collections.singletonList( tableScan.getRowType().getFieldNames().get( i ) ) );

                AggregateCall aggregateCall = getRowCountAggregateCall( cluster );

                AlgNode relNode = LogicalAggregate.create(
                        logicalProject,
                        ImmutableBitSet.of(),
                        Collections.singletonList( ImmutableBitSet.of() ),
                        Collections.singletonList( aggregateCall ) );

                return statisticQueryInterface.selectOneColumnStat( relNode, transaction, statement, queryResult );
            }
        }
        return null;
    }


    /**
     * Gets the amount of entries for a table.
     */
    private StatisticQueryResult getTableCount( QueryResult queryResult, TableScan tableScan, AlgOptCluster cluster, Transaction transaction, Statement statement ) {
        AggregateCall aggregateCall = getRowCountAggregateCall( cluster );
        AlgNode relNode = LogicalAggregate.create(
                tableScan,
                ImmutableBitSet.of(),
                Collections.singletonList( ImmutableBitSet.of() ),
                Collections.singletonList( aggregateCall ) );
        return statisticQueryInterface.selectOneColumnStat( relNode, transaction, statement, queryResult );
    }


    @NotNull
    private AggregateCall getRowCountAggregateCall( AlgOptCluster cluster ) {
        return AggregateCall.create(
                OperatorRegistry.getAgg( OperatorName.COUNT ),
                false,
                false,
                Collections.singletonList( 0 ),
                -1,
                AlgCollations.EMPTY,
                cluster.getTypeFactory().createTypeWithNullability(
                        cluster.getTypeFactory().createPolyType( PolyType.BIGINT ),
                        false ),
                "rowCount" );
    }


    /**
     * Configures and registers the statistics InformationPage for the frontend.
     */
    @Override
    public void displayInformation() {
        InformationManager im = InformationManager.getInstance();

        InformationPage page = new InformationPage( "Statistics" );
        im.addPage( page );

        InformationGroup contentGroup = new InformationGroup( page, "Column Information" );
        im.addGroup( contentGroup );

        InformationTable statisticsInformation = new InformationTable( contentGroup, Arrays.asList( "Column Name", "Type" ) );
        im.registerInformation( statisticsInformation );

        InformationGroup alphabeticalGroup = new InformationGroup( page, "Alphabetical Statistics" );
        im.addGroup( alphabeticalGroup );

        InformationGroup numericalGroup = new InformationGroup( page, "Numerical Statistics" );
        im.addGroup( numericalGroup );

        InformationGroup temporalGroup = new InformationGroup( page, "Temporal Statistics" );
        im.addGroup( temporalGroup );

        InformationTable temporalInformation = new InformationTable( temporalGroup, Arrays.asList( "Column Name", "Min", "Max" ) );

        InformationTable numericalInformation = new InformationTable( numericalGroup, Arrays.asList( "Column Name", "Min", "Max" ) );

        InformationTable alphabeticalInformation = new InformationTable( alphabeticalGroup, Arrays.asList( "Column Name", "Unique Values" ) );

        im.registerInformation( temporalInformation );
        im.registerInformation( numericalInformation );
        im.registerInformation( alphabeticalInformation );

        InformationGroup tableSelectGroup = new InformationGroup( page, "Calls per Table" );
        im.addGroup( tableSelectGroup );

        InformationTable tableSelectInformation = new InformationTable( tableSelectGroup, Arrays.asList( "Table Name", "#SELECTS", "#INSERT", "#DELETE", "#UPDATE" ) );
        im.registerInformation( tableSelectInformation );

        InformationGroup tableInformationGroup = new InformationGroup( page, "Table Information" );
        im.addGroup( tableInformationGroup );

        InformationTable tableInformation = new InformationTable( tableInformationGroup, Arrays.asList( "Table Name", "Schema Type", "Row Count" ) );
        im.registerInformation( tableInformation );

        InformationGroup actionGroup = new InformationGroup( page, "Action" );
        im.addGroup( actionGroup );
        Action reevaluateAction = parameters -> {
            reevaluateAllStatistics();
            page.refresh();
            return "Recalculated statistics";
        };
        InformationAction reevaluateAllInfo = new InformationAction( actionGroup, "Recalculate Statistics", reevaluateAction );
        actionGroup.addInformation( reevaluateAllInfo );
        im.registerInformation( reevaluateAllInfo );
        Action reevaluateRowCount = parameters -> {
            reevaluateRowCount();
            page.refresh();
            return "Reevaluate Row Count";
        };
        InformationAction reevaluateRowCountInfo = new InformationAction( actionGroup, "Reevaluate Row Count", reevaluateRowCount );
        actionGroup.addInformation( reevaluateRowCountInfo );
        im.registerInformation( reevaluateRowCountInfo );

        page.setRefreshFunction( () -> {
            numericalInformation.reset();
            alphabeticalInformation.reset();
            temporalInformation.reset();
            tableSelectInformation.reset();
            tableInformation.reset();
            statisticsInformation.reset();
            statisticSchemaMap.values().forEach( schema -> schema.values().forEach( table -> table.forEach( ( k, v ) -> {
                if ( v instanceof NumericalStatisticColumn ) {
                    if ( ((NumericalStatisticColumn<T>) v).getMin() != null && ((NumericalStatisticColumn<T>) v).getMax() != null ) {
                        numericalInformation.addRow(
                                v.getQualifiedColumnName(),
                                ((NumericalStatisticColumn<T>) v).getMin().toString(),
                                ((NumericalStatisticColumn<T>) v).getMax().toString() );
                    } else {
                        numericalInformation.addRow( v.getQualifiedColumnName(), "❌", "❌" );
                    }
                }
                if ( v instanceof TemporalStatisticColumn ) {
                    if ( ((TemporalStatisticColumn<T>) v).getMin() != null && ((TemporalStatisticColumn<T>) v).getMax() != null ) {
                        temporalInformation.addRow(
                                v.getQualifiedColumnName(),
                                ((TemporalStatisticColumn<T>) v).getMin().toString(),
                                ((TemporalStatisticColumn<T>) v).getMax().toString() );
                    } else {
                        temporalInformation.addRow( v.getQualifiedColumnName(), "❌", "❌" );
                    }
                } else {
                    String values = v.getUniqueValues().toString();
                    if ( !v.isFull() ) {
                        alphabeticalInformation.addRow( v.getQualifiedColumnName(), values );
                    } else {
                        alphabeticalInformation.addRow( v.getQualifiedColumnName(), "is Full" );
                    }
                }
                statisticsInformation.addRow( v.getQualifiedColumnName(), v.getType().name() );

            } ) ) );

            tableStatistic.forEach( ( k, v ) -> {
                tableInformation.addRow( v.getTable(), v.getSchemaType(), v.getNumberOfRows() );

                if ( RuntimeConfig.ACTIVE_TRACKING.getBoolean() && v.getTableType() != TableType.MATERIALIZED_VIEW ) {
                    tableSelectInformation.addRow(
                            v.getTable(),
                            v.getCalls().getNumberOfSelects(),
                            v.getCalls().getNumberOfInserts(),
                            v.getCalls().getNumberOfDeletes(),
                            v.getCalls().getNumberOfUpdates() );
                }
            } );
        } );
    }


    /**
     * Reevaluates all statistics that can be updated using nodes.
     */
    @Override
    public void asyncReevaluateAllStatistics() {
        threadPool.execute( this::reevaluateAllStatistics );
    }


    @Override
    public void propertyChange( PropertyChangeEvent evt ) {
        threadPool.execute( this::workQueue );
    }


    /**
     * All tables that need to be updated (for example after a demolition in a table)
     * are reevaluated with this method.
     */
    private void workQueue() {
        while ( !this.tablesToUpdate.isEmpty() ) {
            long tableId = this.tablesToUpdate.poll();
            if ( Catalog.getInstance().checkIfExistsTable( tableId ) ) {
                reevaluateTable( tableId );
            }
            tableStatistic.remove( tableId );
        }
    }


    /**
     * Tables that needs an update with the help of a node are added to {@link StatisticsManagerImpl#tablesToUpdate}.
     *
     * @param tableId of table
     */
    @Override
    public void tablesToUpdate( long tableId ) {
        if ( !tablesToUpdate.contains( tableId ) ) {
            tablesToUpdate.add( tableId );
            listeners.firePropertyChange( "tablesToUpdate", null, tableId );
        }
    }


    /**
     * Updates the StatisticColumn (min, max, uniqueValues and temporalStatistics).
     * After an insert the inserted values are added to the Statistics if necessary.
     * After a truncate all values are deleted.
     * Method is not used if rows are deleted because it wouldn't be accurate anymore.
     *
     * @param tableId of tables
     * @param changedValues of the table
     * @param type of change on the table
     * @param schemaId of the table
     */
    @Override
    public void tablesToUpdate( long tableId, Map<Long, List<Object>> changedValues, String type, long schemaId ) {
        Catalog catalog = Catalog.getInstance();
        if ( catalog.checkIfExistsTable( tableId ) ) {
            switch ( type ) {
                case "INSERT":
                    handleInsert( tableId, changedValues, schemaId, catalog );
                    break;
                case "TRUNCATE":
                    handleTruncate( tableId, schemaId, catalog );
                    break;
                case "DROP_COLUMN":
                    handleDrop( tableId, changedValues, schemaId );
                    break;
            }
        }
    }


    private void handleDrop( long tableId, Map<Long, List<Object>> changedValues, long schemaId ) {
        Map<Long, Map<Long, StatisticColumn<T>>> schema = this.statisticSchemaMap.get( schemaId );
        if ( schema != null ) {
            Map<Long, StatisticColumn<T>> table = this.statisticSchemaMap.get( schemaId ).get( tableId );
            if ( table != null ) {
                table.remove( changedValues.keySet().stream().findFirst().get() );
            }
        }
    }


    private void handleTruncate( long tableId, long schemaId, Catalog catalog ) {
        CatalogTable catalogTable = catalog.getTable( tableId );
        for ( int i = 0; i < catalogTable.columnIds.size(); i++ ) {
            PolyType polyType = catalog.getColumn( catalogTable.columnIds.get( i ) ).type;
            QueryResult queryResult = new QueryResult( schemaId, catalogTable.id, catalogTable.columnIds.get( i ), polyType );
            if ( this.statisticSchemaMap.get( schemaId ).get( tableId ).get( catalogTable.columnIds.get( i ) ) != null ) {
                StatisticColumn<T> statisticColumn = createNewStatisticColumns( polyType, queryResult );
                if ( statisticColumn != null ) {
                    put( queryResult, statisticColumn );
                }
            }
        }
    }


    private StatisticColumn<T> createNewStatisticColumns( PolyType polyType, QueryResult queryResult ) {
        StatisticColumn<T> statisticColumn = null;
        if ( polyType.getFamily() == PolyTypeFamily.NUMERIC ) {
            statisticColumn = new NumericalStatisticColumn<>( queryResult );
        } else if ( polyType.getFamily() == PolyTypeFamily.CHARACTER ) {
            statisticColumn = new AlphabeticStatisticColumn<T>( queryResult );
        } else if ( PolyType.DATETIME_TYPES.contains( polyType ) ) {
            statisticColumn = new TemporalStatisticColumn<T>( queryResult );
        }
        return statisticColumn;
    }


    private void handleInsert( long tableId, Map<Long, List<Object>> changedValues, long schemaId, Catalog catalog ) {
        CatalogTable catalogTable = catalog.getTable( tableId );
        List<Long> columns = catalogTable.columnIds;
        if ( this.statisticSchemaMap.get( schemaId ) != null ) {
            if ( this.statisticSchemaMap.get( schemaId ).get( tableId ) != null ) {
                for ( int i = 0; i < columns.size(); i++ ) {
                    PolyType polyType = catalog.getColumn( columns.get( i ) ).type;
                    QueryResult queryResult = new QueryResult( schemaId, catalogTable.id, columns.get( i ), polyType );
                    if ( this.statisticSchemaMap.get( schemaId ).get( tableId ).get( columns.get( i ) ) != null && changedValues.get( (long) i ) != null ) {
                        handleInsertColumn( tableId, changedValues, schemaId, columns, i, queryResult );
                    } else {
                        addNewColumnStatistics( changedValues, i, polyType, queryResult );
                    }
                }
            } else {
                addInserts( changedValues, catalog, catalogTable, columns );
            }
        } else {
            addInserts( changedValues, catalog, catalogTable, columns );
        }
    }


    /**
     * Creates new StatisticColumns and inserts the values.
     */
    private void addInserts( Map<Long, List<Object>> changedValues, Catalog catalog, CatalogTable catalogTable, List<Long> columns ) {
        for ( int i = 0; i < columns.size(); i++ ) {
            PolyType polyType = catalog.getColumn( columns.get( i ) ).type;
            QueryResult queryResult = new QueryResult( catalogTable.schemaId, catalogTable.id, columns.get( i ), polyType );
            addNewColumnStatistics( changedValues, i, polyType, queryResult );
        }
    }


    private void addNewColumnStatistics( Map<Long, List<Object>> changedValues, long i, PolyType polyType, QueryResult queryResult ) {
        StatisticColumn<T> statisticColumn = createNewStatisticColumns( polyType, queryResult );
        if ( statisticColumn != null ) {
            statisticColumn.insert( (List) changedValues.get( i ) );
            put( queryResult, statisticColumn );
        }
    }


    private void handleInsertColumn( long tableId, Map<Long, List<Object>> changedValues, long schemaId, List<Long> columns, int i, QueryResult queryResult ) {
        StatisticColumn<T> statisticColumn = this.statisticSchemaMap.get( schemaId ).get( tableId ).get( columns.get( i ) );
        statisticColumn.insert( (List) changedValues.get( (long) i ) );
        put( queryResult, statisticColumn );
    }


    /**
     * Removes statistics from a given table.
     */
    @Override
    public void deleteTableToUpdate( long tableId, long schemaId ) {
        if ( statisticSchemaMap.containsKey( schemaId ) && statisticSchemaMap.get( schemaId ).containsKey( tableId ) ) {
            statisticSchemaMap.get( schemaId ).remove( tableId );
        }
        tableStatistic.remove( tableId );
        if ( tablesToUpdate.contains( tableId ) ) {
            this.tablesToUpdate.remove( tableId );
        }
    }


    /**
     * This method updates the rowCount of a given table, depending on the information source.
     * For inserts, the rowCount is increased by the number of inserted rows.
     * For deletes, the rowCount is decreased by the number of deleted rows.
     * Truncate sets the row count to 0 and Set-row-count is used to set the rowCount to a specific amount.
     *
     * @param tableId of the table
     * @param number of changed rows or explicit number for the rowCount
     * @param source of the rowCount information
     */
    @Override
    public void updateRowCountPerTable( long tableId, int number, String source ) {
        StatisticTable<T> statisticTable;
        switch ( source ) {
            case "INSERT":
                if ( tableStatistic.containsKey( tableId ) ) {
                    statisticTable = tableStatistic.get( tableId );
                    int totalRows = statisticTable.getNumberOfRows() + number;

                    statisticTable.setNumberOfRows( totalRows );
                } else {
                    statisticTable = new StatisticTable<T>( tableId );
                    statisticTable.setNumberOfRows( number );
                }
                break;
            case "DELETE":
                if ( tableStatistic.containsKey( tableId ) ) {
                    statisticTable = tableStatistic.get( tableId );
                    int totalRows = statisticTable.getNumberOfRows() - number;
                    if ( totalRows < 0 ) {
                        totalRows = 0;
                    }
                    statisticTable.setNumberOfRows( totalRows );
                } else {
                    statisticTable = new StatisticTable<T>( tableId );
                }
                break;
            case "SET-ROW-COUNT":
            case "TRUNCATE":
                if ( tableStatistic.containsKey( tableId ) ) {
                    statisticTable = tableStatistic.get( tableId );
                } else {
                    statisticTable = new StatisticTable<T>( tableId );
                }
                statisticTable.setNumberOfRows( number );
                break;
            default:
                throw new RuntimeException( "updateRowCountPerTable is not implemented for: " + source );
        }

        tableStatistic.put( tableId, statisticTable );
    }


    /**
     * The index size is used to update the rowCount of a table.
     *
     * @param tableId of the table
     * @param indexSize of the table
     */
    @Override
    public void setIndexSize( long tableId, int indexSize ) {
        if ( tableStatistic.containsKey( tableId ) ) {
            int numberOfRows = tableStatistic.remove( tableId ).getNumberOfRows();
            if ( numberOfRows != indexSize ) {
                // Use indexSize because it should be correct
                StatisticTable<T> statisticTable = tableStatistic.get( tableId );
                statisticTable.setNumberOfRows( indexSize );
                tableStatistic.put( tableId, statisticTable );
            }
        } else {
            StatisticTable<T> statisticTable = new StatisticTable<T>( tableId );
            statisticTable.setNumberOfRows( indexSize );
            tableStatistic.put( tableId, statisticTable );
        }
    }


    /**
     * Updates how many times a DML (SELECT, INSERT, DELETE, UPDATE) was used on a table. It checks if
     * the {@link StatisticsManagerImpl#tableStatistic} already holds information about the tableCalls
     * and if not creates a new TableCall.
     *
     * @param tableId of the table
     * @param kind of DML
     */
    @Override
    public void setTableCalls( long tableId, String kind ) {
        TableCalls calls;
        if ( tableStatistic.containsKey( tableId ) ) {
            if ( tableStatistic.get( tableId ).getCalls() != null ) {
                calls = tableStatistic.get( tableId ).getCalls();
            } else {
                calls = new TableCalls( tableId, 0, 0, 0, 0 );
            }
        } else {
            calls = new TableCalls( tableId, 0, 0, 0, 0 );
        }
        updateCalls( tableId, kind, calls );
    }


    /**
     * Updates the TableCalls.
     */
    private synchronized void updateCalls( long tableId, String kind, TableCalls calls ) {
        StatisticTable<T> statisticTable;
        if ( tableStatistic.containsKey( tableId ) ) {
            statisticTable = tableStatistic.remove( tableId );
        } else {
            statisticTable = new StatisticTable( tableId );
        }

        switch ( kind ) {
            case "SELECT":
                statisticTable.setCalls( new TableCalls(
                        calls.getTableId(),
                        calls.getNumberOfSelects() + 1,
                        calls.getNumberOfInserts(),
                        calls.getNumberOfDeletes(),
                        calls.getNumberOfUpdates() ) );
                tableStatistic.put( tableId, statisticTable );
                break;
            case "INSERT":
                statisticTable.setCalls( new TableCalls(
                        calls.getTableId(),
                        calls.getNumberOfSelects(),
                        calls.getNumberOfInserts() + 1,
                        calls.getNumberOfDeletes(),
                        calls.getNumberOfUpdates() ) );
                tableStatistic.put( tableId, statisticTable );
                break;
            case "DELETE":
                statisticTable.setCalls( new TableCalls(
                        calls.getTableId(),
                        calls.getNumberOfSelects(),
                        calls.getNumberOfInserts(),
                        calls.getNumberOfDeletes() + 1,
                        calls.getNumberOfUpdates() ) );
                tableStatistic.put( tableId, statisticTable );
                break;
            case "UPDATE":
                statisticTable.setCalls( new TableCalls(
                        calls.getTableId(),
                        calls.getNumberOfSelects() + 1,
                        calls.getNumberOfInserts(),
                        calls.getNumberOfDeletes(),
                        calls.getNumberOfUpdates() + 1 ) );
                tableStatistic.put( tableId, statisticTable );
                break;
            default:
                log.error( "Currently, only SELECT, INSERT, DELETE and UPDATE are available in Statistics." );
        }
    }


    /**
     * Updates how many transactions where committed and roll backed.
     *
     * @param committed true if it is committed and false if it was roll backed
     */
    @Override
    public void updateCommitRollback( boolean committed ) {
        if ( committed ) {
            int numberOfCommits = dashboardInformation.getNumberOfCommits();
            dashboardInformation.setNumberOfCommits( numberOfCommits + 1 );
        } else {
            int numberOfRollbacks = dashboardInformation.getNumberOfRollbacks();
            dashboardInformation.setNumberOfRollbacks( numberOfRollbacks + 1 );
        }
    }


    @Override
    public Object getDashboardInformation() {
        dashboardInformation.updatePolyphenyStatistic();
        return dashboardInformation;
    }


    /**
     * Returns all statistic of a given table, used for table information in the UI.
     *
     * @param schemaId of the table
     * @param tableId ot the table
     * @return an Objet with all available table statistics
     */
    @Override
    public Object getTableStatistic( long schemaId, long tableId ) {
        StatisticTable<T> statisticTable = tableStatistic.get( tableId );
        List<NumericalStatisticColumn<T>> numericInfo = new ArrayList<>();
        List<AlphabeticStatisticColumn<T>> alphabeticInfo = new ArrayList<>();
        List<TemporalStatisticColumn<T>> temporalInfo = new ArrayList<>();
        statisticTable.setNumericalColumn( numericInfo );
        statisticTable.setAlphabeticColumn( alphabeticInfo );
        statisticTable.setTemporalColumn( temporalInfo );
        statisticSchemaMap.get( schemaId ).get( tableId ).forEach( ( k, v ) -> {
            if ( v.getType().getFamily() == PolyTypeFamily.NUMERIC ) {
                numericInfo.add( (NumericalStatisticColumn<T>) v );
                statisticTable.setNumericalColumn( numericInfo );
            } else if ( v.getType().getFamily() == PolyTypeFamily.CHARACTER ) {
                alphabeticInfo.add( (AlphabeticStatisticColumn<T>) v );
                statisticTable.setAlphabeticColumn( alphabeticInfo );
            } else if ( PolyType.DATETIME_TYPES.contains( Catalog.getInstance().getColumn( k ).type ) ) {
                temporalInfo.add( (TemporalStatisticColumn<T>) v );
                statisticTable.setTemporalColumn( temporalInfo );
            }
        } );
        return statisticTable;
    }


    /**
     * This method returns the number of rows for a given table, which is used in
     * {@link org.polypheny.db.schema.impl.AbstractTable#getStatistic()} to update the statistics.
     *
     * @param tableId of the table
     * @return the number of rows of a given table
     */
    @Override
    public synchronized Integer rowCountPerTable( long tableId ) {
        if ( tableStatistic.containsKey( tableId ) ) {
            return tableStatistic.get( tableId ).getNumberOfRows();
        } else {
            return null;
        }
    }


    /**
     * This class reevaluates if background tracking should be stopped or restarted depending on the state of
     * the {@link org.polypheny.db.config.ConfigManager}.
     */
    class TrackingListener implements Config.ConfigListener {

        @Override
        public void onConfigChange( Config c ) {
            registerTrackingToggle();
        }


        @Override
        public void restart( Config c ) {
            registerTrackingToggle();
        }


        private void registerTrackingToggle() {
            String id = getRevalId();
            if ( id == null && RuntimeConfig.DYNAMIC_QUERYING.getBoolean() && RuntimeConfig.PASSIVE_TRACKING.getBoolean() ) {
                String revalId = BackgroundTaskManager.INSTANCE.registerTask(
                        StatisticsManagerImpl.this::asyncReevaluateAllStatistics,
                        "Reevaluate StatisticsManager.",
                        TaskPriority.LOW,
                        (TaskSchedulingType) RuntimeConfig.STATISTIC_RATE.getEnum() );
                setRevalId( revalId );
            } else if ( id != null && (!RuntimeConfig.PASSIVE_TRACKING.getBoolean() || !RuntimeConfig.DYNAMIC_QUERYING.getBoolean()) ) {
                BackgroundTaskManager.INSTANCE.removeBackgroundTask( getRevalId() );
                setRevalId( null );
            }
        }

    }


    private enum NodeType {
        ROW_COUNT_TABLE,
        ROW_COUNT_COLUMN,
        MIN,
        MAX,
        UNIQUE_VALUE
    }

}
