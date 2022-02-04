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


import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeSupport;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.LogicalAggregate;
import org.polypheny.db.algebra.logical.LogicalProject;
import org.polypheny.db.algebra.logical.LogicalSort;
import org.polypheny.db.algebra.logical.LogicalTableScan;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;
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

    private static StatisticQueryProcessor sqlQueryInterface;

    private final ExecutorService threadPool = Executors.newSingleThreadExecutor();

    protected final PropertyChangeSupport listeners = new PropertyChangeSupport( this );

    private int buffer = RuntimeConfig.STATISTIC_BUFFER.getInteger();

    @Setter
    @Getter
    private String revalId = null;

    @Getter
    private final DashboardInformation dashboardInformation;

    @Getter
    private final Map<Long, StatisticTable<T>> tableStatistic;

    @Getter
    private volatile Map<Long, Map<Long, Map<Long, StatisticColumn<T>>>> statisticSchemaMap;

    private final Queue<Long> tablesToUpdate = new ConcurrentLinkedQueue<>();


    public StatisticsManagerImpl( StatisticQueryProcessor statisticQueryProcessor ) {
        this.setQueryInterface( statisticQueryProcessor );
        this.statisticSchemaMap = new ConcurrentHashMap<>();
        this.tableStatistic = new ConcurrentHashMap<>();
        displayInformation();
        registerTaskTracking();
        registerIsFullTracking();
        this.dashboardInformation = new DashboardInformation();
        this.listeners.addPropertyChangeListener( this );
    }


    private Transaction getTransaction() {
        Transaction transaction = null;
        try {
            transaction = sqlQueryInterface.getTransactionManager().startTransaction( "pa", "APP", false, "Statistic Manager" );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( e );
        }
        return transaction;
    }


    public void setQueryInterface( StatisticQueryProcessor statisticQueryProcessor ) {
        sqlQueryInterface = statisticQueryProcessor;
        if ( RuntimeConfig.STATISTICS_ON_STARTUP.getBoolean() ) {
            this.asyncReevaluateAllStatistics();
        }
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
            assignUnique( c, this.getUniqueValues( new QueryColumn( c.getSchemaId(), c.getTableId(), c.getColumnId(), c.getType() ) ) );
        } ) ) );
    }


    /**
     * Reset all statistics and reevaluate them.
     */
    private void reevaluateAllStatistics() {
        if ( sqlQueryInterface == null ) {
            return;
        }
        log.debug( "Resetting StatisticManager." );
        Map<Long, Map<Long, Map<Long, StatisticColumn<T>>>> statisticSchemaMapCopy = new ConcurrentHashMap<>();

        for ( QueryColumn column : sqlQueryInterface.getAllColumns() ) {
            StatisticColumn<T> col = reevaluateColumn( column );
            if ( col != null ) {
                put( statisticSchemaMapCopy, column, col );
            }
        }
        reevaluateRowCount();
        replaceStatistics( statisticSchemaMapCopy );
        log.debug( "Finished resetting StatisticManager." );
    }


    /**
     * Update the row count for all tables.
     */
    private void reevaluateRowCount() {
        if ( sqlQueryInterface == null ) {
            return;
        }
        log.debug( "Reevaluate Row Count." );

        sqlQueryInterface.getAllTable().forEach( table -> {
            Integer rowCount = getTableCount( table.getSchemaName(), table.name );
            updateRowCountPerTable( table.id, rowCount, "SET-ROW-COUNT" );
        } );
    }


    /**
     * Gets a columns of a table and reevaluates them.
     *
     * @param tableId id of table
     */
    @Override
    public void reevaluateTable( Long tableId ) {
        if ( sqlQueryInterface == null ) {
            return;
        }
        if ( Catalog.getInstance().checkIfExistsTable( tableId ) ) {
            deleteTable( Catalog.getInstance().getTable( tableId ).schemaId, tableId );

            List<QueryColumn> res = sqlQueryInterface.getAllColumns( tableId );

            for ( QueryColumn column : res ) {
                StatisticColumn<T> col = reevaluateColumn( column );
                if ( col != null ) {
                    put( column, col );
                }
            }
        }
    }


    private void deleteTable( Long schemaId, Long tableId ) {
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
    private StatisticColumn<T> reevaluateColumn( QueryColumn column ) {

        if ( !Catalog.getInstance().checkIfExistsColumn( column.getTableId(), column.getColumn() ) ) {
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
    private StatisticColumn<T> reevaluateNumericalColumn( QueryColumn column ) {
        StatisticQueryColumn min = this.getAggregateColumn( column, "MIN" );
        StatisticQueryColumn max = this.getAggregateColumn( column, "MAX" );
        Integer count = this.getColumnCount( column );
        NumericalStatisticColumn<T> statisticColumn = new NumericalStatisticColumn<>( column );
        if ( min != null ) {
            //noinspection unchecked
            statisticColumn.setMin( (T) min.getData()[0] );
        }
        if ( max != null ) {
            //noinspection unchecked
            statisticColumn.setMax( (T) max.getData()[0] );
        }

        StatisticQueryColumn unique = this.getUniqueValues( column );
        assignUnique( statisticColumn, unique );

        statisticColumn.setCount( count );

        return statisticColumn;
    }


    /**
     * Reevaluates a temporal column.
     */
    private StatisticColumn<T> reevaluateTemporalColumn( QueryColumn column ) {
        StatisticQueryColumn min = this.getAggregateColumn( column, "MIN" );
        StatisticQueryColumn max = this.getAggregateColumn( column, "MAX" );
        Integer count = this.getColumnCount( column );

        TemporalStatisticColumn<T> statisticColumn = new TemporalStatisticColumn<>( column );
        if ( min != null ) {
            if ( NumberUtils.isParsable( min.getData()[0] ) ) {
                //noinspection unchecked
                statisticColumn.setMin( (T) DateTimeStringUtils.longToAdjustedString( Long.parseLong( min.getData()[0] ), column.getType() ) );
            } else {
                //noinspection unchecked
                statisticColumn.setMin( (T) min.getData()[0] );
            }
        }

        if ( max != null ) {
            if ( NumberUtils.isParsable( max.getData()[0] ) ) {
                //noinspection unchecked
                statisticColumn.setMax( (T) DateTimeStringUtils.longToAdjustedString( Long.parseLong( max.getData()[0] ), column.getType() ) );
            } else {
                //noinspection unchecked
                statisticColumn.setMax( (T) max.getData()[0] );
            }
        }

        StatisticQueryColumn unique = this.getUniqueValues( column );
        if ( unique != null ) {
            for ( int idx = 0; idx < unique.getData().length; idx++ ) {
                if ( unique.getData()[idx] != null ) {
                    unique.getData()[idx] = DateTimeStringUtils.longToAdjustedString( Long.parseLong( unique.getData()[idx] ), column.getType() );
                }
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
    private void assignUnique( StatisticColumn<T> column, StatisticQueryColumn unique ) {
        if ( unique == null || unique.getData() == null ) {
            return;
        }
        if ( unique.getData().length <= this.buffer ) {
            column.setUniqueValues( Arrays.asList( (T[]) unique.getData() ) );
        } else {
            column.setFull( true );
        }
    }


    /**
     * Reevaluates an alphabetical column, with the configured statistics
     */
    private StatisticColumn<T> reevaluateAlphabeticalColumn( QueryColumn column ) {
        StatisticQueryColumn unique = this.getUniqueValues( column );
        Integer count = this.getColumnCount( column );

        AlphabeticStatisticColumn<T> statisticColumn = new AlphabeticStatisticColumn<>( column );
        assignUnique( statisticColumn, unique );
        statisticColumn.setCount( count );

        return statisticColumn;
    }


    private void put( QueryColumn queryColumn, StatisticColumn<T> statisticColumn ) {
        put( this.statisticSchemaMap,
                queryColumn.getSchemaId(),
                queryColumn.getTableId(),
                queryColumn.getColumnId(),
                statisticColumn );
    }


    private void put(
            Map<Long, Map<Long, Map<Long, StatisticColumn<T>>>> statisticSchemaMapCopy,
            QueryColumn queryColumn,
            StatisticColumn<T> statisticColumn ) {
        put( statisticSchemaMapCopy,
                queryColumn.getSchemaId(),
                queryColumn.getTableId(),
                queryColumn.getColumnId(),
                statisticColumn );
    }


    /**
     * Places a column at the correct position in the schemaMap.
     */
    private void put(
            Map<Long, Map<Long, Map<Long, StatisticColumn<T>>>> map,
            Long schemaId,
            Long tableId,
            Long columnId,
            StatisticColumn<T> statisticColumn ) {
        if ( !map.containsKey( schemaId ) ) {
            map.put( schemaId, new HashMap<>() );
        }
        if ( !map.get( schemaId ).containsKey( tableId ) ) {
            map.get( schemaId ).put( tableId, new HashMap<>() );
        }
        map.get( schemaId ).get( tableId ).put( columnId, statisticColumn );

        if ( !tableStatistic.containsKey( tableId ) ) {
            tableStatistic.put( tableId, new StatisticTable( tableId ) );
        }
    }


    /**
     * Queries the database with an aggregate query, to get the min value or max value.
     */
    private StatisticQueryColumn getAggregateColumn( QueryColumn queryColumn, String aggregate ) {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        PolyphenyDbCatalogReader reader = statement.getTransaction().getCatalogReader();
        AlgBuilder relBuilder = AlgBuilder.create( statement );
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();
        final AlgOptCluster cluster = AlgOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder );

        LogicalTableScan tableScan = getLogicalTableScan( queryColumn.getSchema(), queryColumn.getTable(), reader, cluster );

        for ( int i = 0; i < tableScan.getRowType().getFieldNames().size(); i++ ) {
            if ( tableScan.getRowType().getFieldNames().get( i ).equals( queryColumn.getColumn() ) ) {
                LogicalProject logicalProject = LogicalProject.create(
                        tableScan,
                        Collections.singletonList( rexBuilder.makeInputRef( tableScan, i ) ),
                        Collections.singletonList( tableScan.getRowType().getFieldNames().get( i ) ) );

                AggFunction operator = null;
                if ( aggregate.equals( "MAX" ) ) {
                    operator = OperatorRegistry.getAgg( OperatorName.MAX );
                } else if ( aggregate.equals( "MIN" ) ) {
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

                return sqlQueryInterface.selectOneColumnStat( relNode, transaction, statement, queryColumn );

            }
        }
        return null;
    }


    private StatisticQueryColumn getUniqueValues( QueryColumn queryColumn ) {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        PolyphenyDbCatalogReader reader = statement.getTransaction().getCatalogReader();
        AlgBuilder relBuilder = AlgBuilder.create( statement );
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();
        final AlgOptCluster cluster = AlgOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder );

        LogicalTableScan tableScan = getLogicalTableScan( queryColumn.getSchema(), queryColumn.getTable(), reader, cluster );

        AlgNode relNode;
        for ( int i = 0; i < tableScan.getRowType().getFieldNames().size(); i++ ) {
            if ( tableScan.getRowType().getFieldNames().get( i ).equals( queryColumn.getColumn() ) ) {
                LogicalProject logicalProject = LogicalProject.create(
                        tableScan,
                        Collections.singletonList( rexBuilder.makeInputRef( tableScan, i ) ),
                        Collections.singletonList( tableScan.getRowType().getFieldNames().get( i ) ) );

                LogicalAggregate logicalAggregate = LogicalAggregate.create(
                        logicalProject, ImmutableBitSet.of( 0 ),
                        Collections.singletonList( ImmutableBitSet.of( 0 ) ),
                        Collections.emptyList() );

                Pair<BigDecimal, PolyType> valuePair = new Pair<>( new BigDecimal( (int) 6 ), PolyType.DECIMAL );

                relNode = LogicalSort.create(
                        logicalAggregate,
                        AlgCollations.of(),
                        null,
                        new RexLiteral( valuePair.left, rexBuilder.makeInputRef( tableScan, i ).getType(), valuePair.right ) );

                return sqlQueryInterface.selectOneColumnStat( relNode, transaction, statement, queryColumn );
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
     * Gets the amount of entries for a column
     */
    private Integer getColumnCount( QueryColumn queryColumn ) {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        PolyphenyDbCatalogReader reader = statement.getTransaction().getCatalogReader();
        AlgBuilder relBuilder = AlgBuilder.create( statement );
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();
        final AlgOptCluster cluster = AlgOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder );

        LogicalTableScan tableScan = getLogicalTableScan( queryColumn.getSchema(), queryColumn.getTable(), reader, cluster );

        for ( int i = 0; i < tableScan.getRowType().getFieldNames().size(); i++ ) {
            if ( tableScan.getRowType().getFieldNames().get( i ).equals( queryColumn.getColumn() ) ) {
                LogicalProject logicalProject = LogicalProject.create(
                        tableScan,
                        Collections.singletonList( rexBuilder.makeInputRef( tableScan, i ) ),
                        Collections.singletonList( tableScan.getRowType().getFieldNames().get( i ) ) );

                AggregateCall aggregateCall = AggregateCall.create(
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

                AlgNode relNode = LogicalAggregate.create(
                        logicalProject,
                        ImmutableBitSet.of(),
                        Collections.singletonList( ImmutableBitSet.of() ),
                        Collections.singletonList( aggregateCall ) );

                StatisticQueryColumn res = sqlQueryInterface.selectOneColumnStat( relNode, transaction, statement, queryColumn );

                if ( res != null && res.getData() != null && res.getData().length != 0 ) {
                    try {
                        return Integer.parseInt( res.getData()[0] );
                    } catch ( NumberFormatException e ) {
                        log.error( "Count could not be parsed for column {}.", queryColumn.getColumn(), e );
                    }
                }
            }
        }
        return 0;
    }


    /**
     * Gets the amount of entries for a table.
     */
    private Integer getTableCount( String schemaName, String tableName ) {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        PolyphenyDbCatalogReader reader = statement.getTransaction().getCatalogReader();
        AlgBuilder relBuilder = AlgBuilder.create( statement );
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();
        final AlgOptCluster cluster = AlgOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder );

        LogicalTableScan tableScan = getLogicalTableScan( schemaName, tableName, reader, cluster );

        AggregateCall aggregateCall = AggregateCall.create(
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

        AlgNode relNode = LogicalAggregate.create(
                tableScan,
                ImmutableBitSet.of(),
                Collections.singletonList( ImmutableBitSet.of() ),
                Collections.singletonList( aggregateCall ) );

        return Integer.valueOf( sqlQueryInterface.selectTableStat( relNode, transaction, statement ) );
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
            Long tableId = this.tablesToUpdate.poll();
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
    public void tablesToUpdate( Long tableId ) {
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
    public void tablesToUpdate( Long tableId, Map<Long, List<Object>> changedValues, String type, Long schemaId ) {
        if ( type.equals( "INSERT" ) ) {
            Catalog catalog = Catalog.getInstance();
            CatalogTable catalogTable = catalog.getTable( tableId );
            List<Long> columns = catalogTable.columnIds;
            if ( this.statisticSchemaMap.get( schemaId ) != null ) {
                if ( this.statisticSchemaMap.get( schemaId ).get( tableId ) != null ) {
                    for ( int i = 0; i < columns.size(); i++ ) {
                        PolyType polyType = catalog.getColumn( columns.get( i ) ).type;
                        QueryColumn queryColumn = new QueryColumn( schemaId, catalogTable.id, columns.get( i ), polyType );
                        if ( this.statisticSchemaMap.get( schemaId ).get( tableId ).get( columns.get( i ) ) != null ) {
                            StatisticColumn<T> statisticColumn = this.statisticSchemaMap.get( schemaId ).get( tableId ).get( columns.get( i ) );

                            if ( polyType.getFamily() == PolyTypeFamily.NUMERIC ) {
                                ((NumericalStatisticColumn) statisticColumn).insert( changedValues.get( (long) i ) );
                                put( queryColumn, statisticColumn );
                            } else if ( polyType.getFamily() == PolyTypeFamily.CHARACTER ) {
                                ((AlphabeticStatisticColumn) statisticColumn).insert( changedValues.get( (long) i ) );
                                put( queryColumn, statisticColumn );
                            } else if ( PolyType.DATETIME_TYPES.contains( polyType ) ) {
                                ((TemporalStatisticColumn) statisticColumn).insert( changedValues.get( (long) i ) );
                                put( queryColumn, statisticColumn );
                            }
                        }
                    }
                } else {
                    addDataStatistics( changedValues, catalog, catalogTable, columns );
                }
            } else {
                addDataStatistics( changedValues, catalog, catalogTable, columns );
            }
        } else if ( type.equals( "TRUNCATE" ) ) {
            Catalog catalog = Catalog.getInstance();
            CatalogTable catalogTable = catalog.getTable( tableId );
            for ( int i = 0; i < catalogTable.columnIds.size(); i++ ) {
                PolyType polyType = catalog.getColumn( catalogTable.columnIds.get( i ) ).type;
                QueryColumn queryColumn = new QueryColumn( schemaId, catalogTable.id, catalogTable.columnIds.get( i ), polyType );
                if ( this.statisticSchemaMap.get( schemaId ).get( tableId ).get( catalogTable.columnIds.get( i ) ) != null ) {
                    if ( polyType.getFamily() == PolyTypeFamily.NUMERIC ) {
                        NumericalStatisticColumn numericalStatisticColumn = new NumericalStatisticColumn<>( queryColumn );
                        put( queryColumn, numericalStatisticColumn );
                    } else if ( polyType.getFamily() == PolyTypeFamily.CHARACTER ) {
                        AlphabeticStatisticColumn alphabeticStatisticColumn = new AlphabeticStatisticColumn<T>( queryColumn );
                        put( queryColumn, alphabeticStatisticColumn );
                    } else if ( PolyType.DATETIME_TYPES.contains( polyType ) ) {
                        TemporalStatisticColumn temporalStatisticColumn = new TemporalStatisticColumn<T>( queryColumn );
                        put( queryColumn, temporalStatisticColumn );
                    }
                }
            }
        }
    }


    /**
     * Creates new StatisticColumns and inserts the values.
     */
    private void addDataStatistics( Map<Long, List<Object>> changedValues, Catalog catalog, CatalogTable catalogTable, List<Long> columns ) {
        for ( int i = 0; i < columns.size(); i++ ) {
            PolyType polyType = catalog.getColumn( columns.get( i ) ).type;
            QueryColumn queryColumn = new QueryColumn( catalogTable.schemaId, catalogTable.id, columns.get( i ), polyType );

            if ( polyType.getFamily() == PolyTypeFamily.NUMERIC ) {
                NumericalStatisticColumn numericalStatisticColumn = new NumericalStatisticColumn<>( queryColumn );
                numericalStatisticColumn.insert( changedValues.get( (long) i ) );
                put( queryColumn, numericalStatisticColumn );
            } else if ( polyType.getFamily() == PolyTypeFamily.CHARACTER ) {
                AlphabeticStatisticColumn alphabeticStatisticColumn = new AlphabeticStatisticColumn<T>( queryColumn );
                alphabeticStatisticColumn.insert( changedValues.get( (long) i ) );
                put( queryColumn, alphabeticStatisticColumn );
            } else if ( PolyType.DATETIME_TYPES.contains( polyType ) ) {
                TemporalStatisticColumn temporalStatisticColumn = new TemporalStatisticColumn<T>( queryColumn );
                temporalStatisticColumn.insert( changedValues.get( (long) i ) );
                put( queryColumn, temporalStatisticColumn );
            }
        }
    }


    /**
     * Removes statistics from a given table.
     */
    @Override
    public void deleteTableToUpdate( Long tableId, Long schemaId ) {
        statisticSchemaMap.get( schemaId ).remove( tableId );
        tableStatistic.remove( tableId );
        this.tablesToUpdate.remove( tableId );
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
    public void updateRowCountPerTable( Long tableId, Integer number, String source ) {
        StatisticTable statisticTable;
        switch ( source ) {
            case "INSERT":
                if ( tableStatistic.containsKey( tableId ) ) {
                    statisticTable = tableStatistic.get( tableId );
                    int totalRows = statisticTable.getNumberOfRows() + number;

                    statisticTable.setNumberOfRows( totalRows );
                } else {
                    statisticTable = new StatisticTable( tableId );
                    statisticTable.setNumberOfRows( number );
                }
                break;
            case "DELETE":
                if ( tableStatistic.containsKey( tableId ) ) {
                    statisticTable = tableStatistic.get( tableId );
                    int totalRows = statisticTable.getNumberOfRows() - number;

                    statisticTable.setNumberOfRows( totalRows );
                } else {
                    statisticTable = new StatisticTable( tableId );
                }
                break;
            case "SET-ROW-COUNT":
            case "TRUNCATE":
                if ( tableStatistic.containsKey( tableId ) ) {
                    statisticTable = tableStatistic.get( tableId );
                } else {
                    statisticTable = new StatisticTable( tableId );
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
    public void setIndexSize( Long tableId, int indexSize ) {
        if ( tableStatistic.containsKey( tableId ) ) {
            int numberOfRows = tableStatistic.remove( tableId ).getNumberOfRows();
            if ( numberOfRows != indexSize ) {
                // Use indexSize because it should be correct
                StatisticTable statisticTable = tableStatistic.get( tableId );
                statisticTable.setNumberOfRows( indexSize );
                tableStatistic.put( tableId, statisticTable );
            }
        } else {
            StatisticTable statisticTable = new StatisticTable( tableId );
            statisticTable.setNumberOfRows( indexSize );
            tableStatistic.put( tableId, statisticTable );
        }
    }


    /**
     * Updates how many times a DML (SELECT, INSERT, DELETE, UPDATE) was used on a table
     * checks if the {@link StatisticsManagerImpl#tableStatistic} already holds information about the tableCalls and if not creates a new TableCall.
     *
     * @param tableId of the table
     * @param kind of DML
     */
    @Override
    public void setTableCalls( Long tableId, String kind ) {
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
    private synchronized void updateCalls( Long tableId, String kind, TableCalls calls ) {
        StatisticTable statisticTable;
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
    public Object getTableStatistic( Long schemaId, Long tableId ) {
        StatisticTable<T> statisticTable = tableStatistic.get( tableId );
        List<NumericalStatisticColumn<T>> numericInfo = new ArrayList<>();
        List<AlphabeticStatisticColumn<T>> alphabeticInfo = new ArrayList<>();
        List<TemporalStatisticColumn<T>> temporalInfo = new ArrayList<>();
        statisticSchemaMap.get( schemaId ).get( tableId ).forEach( ( k, v ) -> {
            if ( v.getType().getFamily() == PolyTypeFamily.NUMERIC ) {
                numericInfo.add( (NumericalStatisticColumn<T>) v );
                statisticTable.setNumericalColumn( numericInfo );
            } else if ( v.getType().getFamily() == PolyTypeFamily.CHARACTER ) {
                alphabeticInfo.add( (AlphabeticStatisticColumn<T>) v );
                statisticTable.setAlphabeticColumn( alphabeticInfo );
            } else if ( PolyType.DATETIME_TYPES.contains( v.getType().getFamily() ) ) {
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
    public synchronized Integer rowCountPerTable( Long tableId ) {
        if ( tableId != null && tableStatistic.containsKey( tableId ) ) {
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

}
