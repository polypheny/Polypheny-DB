/*
 * Copyright 2019-2023 The Polypheny Project
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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.relational.LogicalAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalSort;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.snapshot.Snapshot;
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
import org.polypheny.db.monitoring.events.MonitoringType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.schema.impl.AbstractEntity;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyBigDecimal;
import org.polypheny.db.type.entity.PolyInteger;
import org.polypheny.db.type.entity.PolyValue;
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
public class StatisticsManagerImpl extends StatisticsManager {

    private static StatisticQueryProcessor statisticQueryInterface;

    private final ExecutorService threadPool = Executors.newSingleThreadExecutor();

    protected final PropertyChangeSupport listeners = new PropertyChangeSupport( this );

    private int buffer = RuntimeConfig.STATISTIC_BUFFER.getInteger();

    @Setter
    @Getter
    private String revalId = null;

    private DashboardInformation dashboardInformation;

    @Getter
    private final Map<Long, StatisticTable> tableStatistic;

    @Getter
    private volatile Map<Long, StatisticColumn> statisticFields;

    private final Queue<Long> tablesToUpdate = new ConcurrentLinkedQueue<>();

    private Transaction transaction;
    private Statement statement;


    public StatisticsManagerImpl( StatisticQueryProcessor statisticQueryProcessor ) {
        this.setQueryInterface( statisticQueryProcessor );
        this.statisticFields = new ConcurrentHashMap<>();
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
            // this.asyncReevaluateAllStatistics();
        }
    }


    private Transaction getTransaction() {
        Transaction transaction;
        transaction = statisticQueryInterface.getTransactionManager().startTransaction( Catalog.getInstance().getSnapshot().getUser( Catalog.defaultUserId ), Catalog.getInstance().getSnapshot().getNamespace( 0 ), false, "Statistic Manager" );
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
        this.statisticFields.values().forEach( c -> {
            assignUnique( c, this.prepareNode( QueryResult.fromCatalogColumn( Catalog.getInstance().getSnapshot().rel().getColumn( c.columnId ).orElseThrow() ), NodeType.UNIQUE_VALUE ) );
        } );
    }


    /**
     * Reset all statistics and reevaluate them.
     */
    private void reevaluateAllStatistics() {
        if ( statisticQueryInterface == null ) {
            return;
        }
        log.debug( "Resetting StatisticManager." );
        Map<Long, StatisticColumn> statisticCopy = new ConcurrentHashMap<>();
        transaction = getTransaction();
        statement = transaction.createStatement();
        statement.getQueryProcessor().lock( statement );
        try {
            for ( QueryResult column : statisticQueryInterface.getAllColumns() ) {
                StatisticColumn col = reevaluateColumn( column );
                if ( col != null ) {
                    put( statisticCopy, column, col );
                }
            }
            reevaluateRowCount();
            replaceStatistics( statisticCopy );
            log.debug( "Finished resetting StatisticManager." );
            statisticQueryInterface.commitTransaction( transaction, statement );
        } catch ( Exception e ) {
            try {
                statement.getQueryProcessor().unlock( statement );
                statement.getTransaction().rollback();
            } catch ( TransactionException ex ) {
                throw new RuntimeException( ex );
            }
        }
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
            PolyInteger rowCount = getNumberColumnCount( this.prepareNode( new QueryResult( Catalog.getInstance().getSnapshot().getLogicalEntity( table.id ).orElseThrow(), null ), NodeType.ROW_COUNT_TABLE ) );
            updateRowCountPerTable( table.id, rowCount.value, MonitoringType.SET_ROW_COUNT );
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
        // LogicalEntity entity = Catalog.getInstance().getSnapshot().getLogicalEntity( tableId ).map( e -> e.unwrap( LogicalTable.class ) ).orElseThrow();
        statisticQueryInterface.commitTransaction( transaction, statement );
    }


    private void deleteTable( LogicalTable table ) {
        for ( long columnId : table.getColumnIds() ) {
            this.statisticFields.remove( columnId );
        }
    }


    /**
     * Replace the tracked statistics with new statistics.
     */
    private synchronized void replaceStatistics( Map<Long, StatisticColumn> statistics ) {
        this.statisticFields = new ConcurrentHashMap<>( statistics );
    }


    /**
     * Method to sort a column into the different kinds of column types and hands it to the specific reevaluation
     */
    private StatisticColumn reevaluateColumn( QueryResult column ) {

        if ( column.getColumn().type.getFamily() == PolyTypeFamily.ARRAY ) {
            log.warn( "array not yet supported" );
            return null;
        }

        if ( column.getColumn().type.getFamily() == PolyTypeFamily.NUMERIC ) {
            return this.reevaluateNumericalColumn( column );
        } else if ( column.getColumn().type.getFamily() == PolyTypeFamily.CHARACTER ) {
            return this.reevaluateAlphabeticalColumn( column );
        } else if ( PolyType.DATETIME_TYPES.contains( column.getColumn().type ) ) {
            return this.reevaluateTemporalColumn( column );
        }
        return null;
    }


    /**
     * Reevaluates a numerical column, with the configured statistics.
     */
    private StatisticColumn reevaluateNumericalColumn( QueryResult column ) {
        StatisticQueryResult min = this.prepareNode( column, NodeType.MIN );
        StatisticQueryResult max = this.prepareNode( column, NodeType.MAX );
        PolyInteger count = getNumberColumnCount( this.prepareNode( column, NodeType.ROW_COUNT_COLUMN ) );
        NumericalStatisticColumn statisticColumn = new NumericalStatisticColumn( column );
        if ( min != null ) {
            statisticColumn.setMin( min.getData()[0].asNumber() );
        }
        if ( max != null ) {
            statisticColumn.setMax( max.getData()[0].asNumber() );
        }

        StatisticQueryResult unique = this.prepareNode( column, NodeType.UNIQUE_VALUE );
        assignUnique( statisticColumn, unique );

        statisticColumn.setCount( count );

        return statisticColumn;
    }


    /**
     * Reevaluates a temporal column.
     */
    private StatisticColumn reevaluateTemporalColumn( QueryResult column ) {
        StatisticQueryResult min = this.prepareNode( column, NodeType.MIN );
        StatisticQueryResult max = this.prepareNode( column, NodeType.MAX );
        PolyInteger count = getNumberColumnCount( this.prepareNode( column, NodeType.ROW_COUNT_COLUMN ) );

        TemporalStatisticColumn statisticColumn = new TemporalStatisticColumn( column );
        if ( min != null && max.getData()[0] != null ) {
            statisticColumn.setMin( min.getData()[0].asTemporal() );
        }

        if ( max != null && max.getData()[0] != null ) {
            statisticColumn.setMin( max.getData()[0].asTemporal() );
        }

        StatisticQueryResult unique = this.prepareNode( column, NodeType.UNIQUE_VALUE );
        if ( unique != null ) {
            for ( int idx = 0; idx < unique.getData().length; idx++ ) {
                unique.getData()[idx] = unique.getData()[idx].asTemporal();

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
    private <T> void assignUnique( StatisticColumn column, StatisticQueryResult unique ) {
        if ( unique == null || unique.getData() == null ) {
            return;
        }
        if ( unique.getData().length <= this.buffer ) {
            column.setUniqueValues( Arrays.asList( unique.getData() ) );
        } else {
            column.setFull( true );
        }
    }


    /**
     * Reevaluates an alphabetical column, with the configured statistics
     */
    private AlphabeticStatisticColumn reevaluateAlphabeticalColumn( QueryResult column ) {
        StatisticQueryResult unique = this.prepareNode( column, NodeType.UNIQUE_VALUE );
        PolyInteger count = getNumberColumnCount( this.prepareNode( column, NodeType.ROW_COUNT_COLUMN ) );

        AlphabeticStatisticColumn statisticColumn = new AlphabeticStatisticColumn( column );
        assignUnique( statisticColumn, unique );
        statisticColumn.setCount( count );

        return statisticColumn;
    }


    private PolyInteger getNumberColumnCount( StatisticQueryResult countColumn ) {
        if ( countColumn != null && countColumn.getData() != null && countColumn.getData().length != 0 ) {
            PolyValue value = countColumn.getData()[0];

            if ( value.isNumber() ) {
                return PolyInteger.of( value.asNumber().intValue() );
            }
        }
        return PolyInteger.of( 0 );
    }


    private void put( QueryResult queryResult, StatisticColumn statisticColumn ) {
        put(
                this.statisticFields,
                queryResult,
                statisticColumn );
    }


    private void put(
            Map<Long, StatisticColumn> statisticsCopy,
            QueryResult queryResult,
            StatisticColumn statisticColumn ) {
        put(
                statisticsCopy,
                queryResult.getColumn().namespaceId,
                queryResult.getColumn().tableId,
                queryResult.getColumn().id,
                statisticColumn );
    }


    /**
     * Places a column at the correct position in the schemaMap.
     */
    private void put(
            Map<Long, StatisticColumn> map,
            long schemaId,
            long tableId,
            long columnId,
            StatisticColumn statisticColumn ) {

        map.put( columnId, statisticColumn );

        if ( !tableStatistic.containsKey( tableId ) ) {
            tableStatistic.put( tableId, new StatisticTable( tableId ) );
        }
    }


    private StatisticQueryResult prepareNode( QueryResult queryResult, NodeType nodeType ) {
        StatisticQueryResult statisticQueryColumn = null;
        if ( Catalog.getInstance().getSnapshot().getLogicalEntity( queryResult.getEntity().id ).isPresent() ) {
            AlgNode queryNode = getQueryNode( queryResult, nodeType );
            statisticQueryColumn = statisticQueryInterface.selectOneColumnStat( queryNode, transaction, statement, queryResult );
        }
        return statisticQueryColumn;
    }


    @Nullable
    private AlgNode getQueryNode( QueryResult queryResult, NodeType nodeType ) {
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        AlgBuilder relBuilder = AlgBuilder.create( statement );
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();
        final AlgOptCluster cluster = AlgOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder, null, statement.getDataContext().getSnapshot() );

        AlgNode queryNode;
        LogicalRelScan tableScan = getLogicalScan( queryResult.getEntity().id, snapshot, cluster );
        switch ( nodeType ) {
            case MIN:
            case MAX:
                queryNode = getAggregateColumn( queryResult, nodeType, tableScan, rexBuilder, cluster );
                break;
            case UNIQUE_VALUE:
                queryNode = getUniqueValues( queryResult, tableScan, rexBuilder );
                break;
            case ROW_COUNT_COLUMN:
                queryNode = getColumnCount( queryResult, tableScan, rexBuilder, cluster );
                break;
            case ROW_COUNT_TABLE:
                queryNode = getTableCount( tableScan, cluster );
                break;
            default:
                throw new RuntimeException( "Used nodeType is not defined in statistics." );
        }
        return queryNode;
    }


    /**
     * Gets a tableScan for a given table.
     */
    private LogicalRelScan getLogicalScan( long tableId, Snapshot snapshot, AlgOptCluster cluster ) {
        return LogicalRelScan.create( cluster, snapshot.getLogicalEntity( tableId ).orElseThrow() );
    }


    /**
     * Queries the database with an aggregate query, to get the min value or max value.
     */
    private AlgNode getAggregateColumn( QueryResult queryResult, NodeType nodeType, RelScan<?> tableScan, RexBuilder rexBuilder, AlgOptCluster cluster ) {
        for ( int i = 0; i < tableScan.getRowType().getFieldNames().size(); i++ ) {
            if ( tableScan.getRowType().getFieldNames().get( i ).equals( queryResult.getColumn().name ) ) {
                LogicalProject logicalProject = LogicalProject.create(
                        tableScan,
                        Collections.singletonList( rexBuilder.makeInputRef( tableScan, i ) ),
                        Collections.singletonList( tableScan.getRowType().getFieldNames().get( i ) ) );

                AggFunction operator;
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

                return LogicalAggregate.create(
                        logicalProject,
                        ImmutableBitSet.of(),
                        Collections.singletonList( ImmutableBitSet.of() ),
                        Collections.singletonList( aggregateCall ) );
            }
        }
        return null;
    }


    private AlgNode getUniqueValues( QueryResult queryResult, RelScan<?> tableScan, RexBuilder rexBuilder ) {
        for ( int i = 0; i < tableScan.getRowType().getFieldNames().size(); i++ ) {
            if ( tableScan.getRowType().getFieldNames().get( i ).equals( queryResult.getColumn().name ) ) {
                LogicalProject logicalProject = LogicalProject.create(
                        tableScan,
                        Collections.singletonList( rexBuilder.makeInputRef( tableScan, i ) ),
                        Collections.singletonList( tableScan.getRowType().getFieldNames().get( i ) ) );

                LogicalAggregate logicalAggregate = LogicalAggregate.create(
                        logicalProject, ImmutableBitSet.of( 0 ),
                        Collections.singletonList( ImmutableBitSet.of( 0 ) ),
                        Collections.emptyList() );

                Pair<BigDecimal, PolyType> valuePair = new Pair<>( new BigDecimal( (int) 6 ), PolyType.DECIMAL );

                return LogicalSort.create(
                        logicalAggregate,
                        AlgCollations.of(),
                        null,
                        new RexLiteral( PolyBigDecimal.of( valuePair.left ), rexBuilder.makeInputRef( tableScan, i ).getType(), valuePair.right ) );
            }
        }
        return null;
    }


    /**
     * Gets the amount of entries for a column
     */
    private AlgNode getColumnCount( QueryResult queryResult, RelScan<?> tableScan, RexBuilder rexBuilder, AlgOptCluster cluster ) {
        for ( int i = 0; i < tableScan.getRowType().getFieldNames().size(); i++ ) {
            if ( queryResult.getColumn() != null && tableScan.getRowType().getFieldNames().get( i ).equals( queryResult.getColumn().name ) ) {
                LogicalProject logicalProject = LogicalProject.create(
                        tableScan,
                        Collections.singletonList( rexBuilder.makeInputRef( tableScan, i ) ),
                        Collections.singletonList( tableScan.getRowType().getFieldNames().get( i ) ) );

                AggregateCall aggregateCall = getRowCountAggregateCall( cluster );

                return LogicalAggregate.create(
                        logicalProject,
                        ImmutableBitSet.of(),
                        Collections.singletonList( ImmutableBitSet.of() ),
                        Collections.singletonList( aggregateCall ) );
            }
        }
        return null;
    }


    /**
     * Gets the amount of entries for a table.
     */
    private AlgNode getTableCount( RelScan<?> tableScan, AlgOptCluster cluster ) {
        AggregateCall aggregateCall = getRowCountAggregateCall( cluster );
        return LogicalAggregate.create(
                tableScan,
                ImmutableBitSet.of(),
                Collections.singletonList( ImmutableBitSet.of() ),
                Collections.singletonList( aggregateCall ) );
    }


    @Nonnull
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
            statisticFields.forEach( ( k, v ) -> {
                if ( v instanceof NumericalStatisticColumn ) {
                    if ( ((NumericalStatisticColumn) v).getMin() != null && ((NumericalStatisticColumn) v).getMax() != null ) {
                        numericalInformation.addRow(
                                v.columnId,
                                ((NumericalStatisticColumn) v).getMin().toString(),
                                ((NumericalStatisticColumn) v).getMax().toString() );
                    } else {
                        numericalInformation.addRow( v.columnId, "❌", "❌" );
                    }
                }
                if ( v instanceof TemporalStatisticColumn ) {
                    if ( ((TemporalStatisticColumn) v).getMin() != null && ((TemporalStatisticColumn) v).getMax() != null ) {
                        temporalInformation.addRow(
                                v.columnId,
                                ((TemporalStatisticColumn) v).getMin().toString(),
                                ((TemporalStatisticColumn) v).getMax().toString() );
                    } else {
                        temporalInformation.addRow( v.columnId, "❌", "❌" );
                    }
                } else {
                    String values = v.getUniqueValues().toString();
                    if ( !v.isFull() ) {
                        alphabeticalInformation.addRow( v.columnId, values );
                    } else {
                        alphabeticalInformation.addRow( v.columnId, "is Full" );
                    }
                }
                statisticsInformation.addRow( v.columnId, v.type.getName() );

            } );

            tableStatistic.forEach( ( k, v ) -> {
                tableInformation.addRow( v.getTable(), v.getNamespaceType(), v.getNumberOfRows() );

                if ( RuntimeConfig.ACTIVE_TRACKING.getBoolean() && v.getEntityType() != EntityType.MATERIALIZED_VIEW ) {
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
            if ( Catalog.getInstance().getSnapshot().getLogicalEntity( tableId ).isPresent() ) {
                reevaluateTable( tableId );
                return;
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
    public void tablesToUpdate( long tableId, Map<Long, List<?>> changedValues, MonitoringType type, long schemaId ) {
        if ( Catalog.snapshot().getLogicalEntity( tableId ).isEmpty() ) {
            return;
        }

        switch ( type ) {
            case INSERT:
                handleInsert( tableId, changedValues, Catalog.snapshot() );
                break;
            case TRUNCATE:
                handleTruncate( tableId, Catalog.snapshot() );
                break;
            case DROP_COLUMN:
                handleDrop( changedValues );
                break;
        }

    }


    private void handleDrop( Map<Long, List<?>> changedValues ) {
        changedValues.keySet().stream().findFirst().ifPresent( id -> statisticFields.remove( id ) );
    }


    private void handleTruncate( long tableId, Snapshot snapshot ) {
        LogicalTable catalogTable = snapshot.getLogicalEntity( tableId ).map( e -> e.unwrap( LogicalTable.class ) ).orElseThrow();
        for ( LogicalColumn column : catalogTable.getColumns() ) {
            PolyType polyType = column.type;
            QueryResult queryResult = new QueryResult( catalogTable, column );
            if ( statisticFields.get( column.id ) == null ) {
                continue;
            }
            StatisticColumn statisticColumn = createStatisticColumn( polyType, queryResult );
            if ( statisticColumn != null ) {
                put( queryResult, statisticColumn );
            }
        }
    }


    private StatisticColumn createStatisticColumn( PolyType polyType, QueryResult queryResult ) {
        StatisticColumn statisticColumn = null;
        if ( polyType.getFamily() == PolyTypeFamily.ARRAY ) {
            log.warn( "statistic are not yet supported" );
            return null;
        }

        if ( polyType.getFamily() == PolyTypeFamily.NUMERIC ) {
            statisticColumn = new NumericalStatisticColumn( queryResult );
        } else if ( polyType.getFamily() == PolyTypeFamily.CHARACTER ) {
            statisticColumn = new AlphabeticStatisticColumn( queryResult );
        } else if ( PolyType.DATETIME_TYPES.contains( polyType ) ) {
            statisticColumn = new TemporalStatisticColumn( queryResult );
        }
        return statisticColumn;
    }


    private void handleInsert( long tableId, Map<Long, List<?>> changedValues, Snapshot snapshot ) {
        LogicalTable catalogTable = snapshot.getLogicalEntity( tableId ).map( e -> e.unwrap( LogicalTable.class ) ).orElseThrow();
        List<LogicalColumn> columns = catalogTable.getColumns();
        if ( changedValues.size() != columns.size() ) {
            log.warn( "non-matching statistics length" );
            return;
        }

        for ( LogicalColumn column : catalogTable.getColumns() ) {
            if ( column.collectionsType != null ) {
                log.warn( "collections statistics are no yet supported" );
                return;
            }

            PolyType polyType = column.type;

            QueryResult queryResult = new QueryResult( catalogTable, column );
            if ( this.statisticFields.containsKey( column.id ) && changedValues.get( (long) column.position ) != null ) {
                handleInsertColumn( changedValues.get( (long) column.position - 1 ), column, queryResult );
            } else {
                addNewColumnStatistics( changedValues, column.position - 1, polyType, queryResult );
            }
        }

    }


    /**
     * Creates new StatisticColumns and inserts the values.
     */
    private void addInserts( Map<Long, List<?>> changedValues, LogicalTable catalogTable, List<LogicalColumn> columns ) {
        for ( LogicalColumn column : columns ) {
            QueryResult queryResult = new QueryResult( catalogTable, column );
            addNewColumnStatistics( changedValues, column.position - 1, column.type, queryResult );
        }
    }


    private void addNewColumnStatistics( Map<Long, List<?>> changedValues, long i, PolyType polyType, QueryResult queryResult ) {
        StatisticColumn statisticColumn = createStatisticColumn( polyType, queryResult );
        if ( statisticColumn != null ) {
            statisticColumn.insert( (List) changedValues.get( i ) );
            put( queryResult, statisticColumn );
        }
    }


    private void handleInsertColumn( List<?> changedValues, LogicalColumn column, QueryResult queryResult ) {
        StatisticColumn statisticColumn = this.statisticFields.get( column.id );
        statisticColumn.insert( (List) changedValues );
        put( queryResult, statisticColumn );
    }


    /**
     * Removes statistics from a given table.
     */
    @Override
    public void deleteTableToUpdate( long tableId ) {
        for ( LogicalColumn column : Catalog.snapshot().rel().getColumns( tableId ) ) {
            statisticFields.get( column.id );
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
     * @param type of the rowCount information
     */
    @Override
    public void updateRowCountPerTable( long tableId, int number, MonitoringType type ) {
        StatisticTable statisticTable;
        switch ( type ) {
            case INSERT:
                if ( tableStatistic.containsKey( tableId ) ) {
                    statisticTable = tableStatistic.get( tableId );
                    int totalRows = statisticTable.getNumberOfRows() + number;

                    statisticTable.setNumberOfRows( totalRows );
                } else {
                    statisticTable = new StatisticTable( tableId );
                    statisticTable.setNumberOfRows( number );
                }
                break;
            case DELETE:
                if ( tableStatistic.containsKey( tableId ) ) {
                    statisticTable = tableStatistic.get( tableId );
                    int totalRows = statisticTable.getNumberOfRows() - number;

                    statisticTable.setNumberOfRows( totalRows );
                } else {
                    statisticTable = new StatisticTable( tableId );
                }
                break;
            case SET_ROW_COUNT:
            case TRUNCATE:
                if ( tableStatistic.containsKey( tableId ) ) {
                    statisticTable = tableStatistic.get( tableId );
                } else {
                    statisticTable = new StatisticTable( tableId );
                }
                statisticTable.setNumberOfRows( number );
                break;
            default:
                throw new RuntimeException( "updateRowCountPerTable is not implemented for: " + type );
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
     * Updates how many times a DML (SELECT, INSERT, DELETE, UPDATE) was used on a table. It checks if
     * the {@link StatisticsManagerImpl#tableStatistic} already holds information about the tableCalls
     * and if not creates a new TableCall.
     *
     * @param tableId of the table
     * @param type of DML
     */
    @Override
    public void setTableCalls( long tableId, MonitoringType type ) {
        TableCalls calls;
        if ( tableStatistic.containsKey( tableId ) ) {
            calls = tableStatistic.get( tableId ).getCalls();
            if ( calls == null ) {
                calls = new TableCalls( tableId, 0, 0, 0, 0 );
            }
        } else {
            calls = new TableCalls( tableId, 0, 0, 0, 0 );
        }
        updateCalls( tableId, type, calls );
    }


    /**
     * Updates the TableCalls.
     */
    private synchronized void updateCalls( long tableId, MonitoringType kind, TableCalls calls ) {
        StatisticTable statisticTable;
        if ( tableStatistic.containsKey( tableId ) ) {
            statisticTable = tableStatistic.remove( tableId );
        } else {
            statisticTable = new StatisticTable( tableId );
        }

        switch ( kind ) {
            case SELECT:
                statisticTable.setCalls( new TableCalls(
                        calls.getTableId(),
                        calls.getNumberOfSelects() + 1,
                        calls.getNumberOfInserts(),
                        calls.getNumberOfDeletes(),
                        calls.getNumberOfUpdates() ) );
                tableStatistic.put( tableId, statisticTable );
                break;
            case INSERT:
                statisticTable.setCalls( new TableCalls(
                        calls.getTableId(),
                        calls.getNumberOfSelects(),
                        calls.getNumberOfInserts() + 1,
                        calls.getNumberOfDeletes(),
                        calls.getNumberOfUpdates() ) );
                tableStatistic.put( tableId, statisticTable );
                break;
            case DELETE:
                statisticTable.setCalls( new TableCalls(
                        calls.getTableId(),
                        calls.getNumberOfSelects(),
                        calls.getNumberOfInserts(),
                        calls.getNumberOfDeletes() + 1,
                        calls.getNumberOfUpdates() ) );
                tableStatistic.put( tableId, statisticTable );
                break;
            case UPDATE:
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
        StatisticTable statisticTable = tableStatistic.get( tableId );
        List<NumericalStatisticColumn> numericInfo = new ArrayList<>();
        List<AlphabeticStatisticColumn> alphabeticInfo = new ArrayList<>();
        List<TemporalStatisticColumn> temporalInfo = new ArrayList<>();
        statisticTable.setNumericalColumn( numericInfo );
        statisticTable.setAlphabeticColumn( alphabeticInfo );
        statisticTable.setTemporalColumn( temporalInfo );
        statisticFields.forEach( ( k, v ) -> {
            if ( v.type.getFamily() == PolyTypeFamily.NUMERIC ) {
                numericInfo.add( (NumericalStatisticColumn) v );
                statisticTable.setNumericalColumn( numericInfo );
            } else if ( v.type.getFamily() == PolyTypeFamily.CHARACTER ) {
                alphabeticInfo.add( (AlphabeticStatisticColumn) v );
                statisticTable.setAlphabeticColumn( alphabeticInfo );
            } else if ( PolyType.DATETIME_TYPES.contains( Catalog.getInstance().getSnapshot().rel().getColumn( k ).orElseThrow().type ) ) {
                temporalInfo.add( (TemporalStatisticColumn) v );
                statisticTable.setTemporalColumn( temporalInfo );
            }
        } );
        return statisticTable;
    }


    /**
     * This method returns the number of rows for a given table, which is used in
     * {@link AbstractEntity#getStatistic()} to update the statistics.
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


    @Override
    public Map<String, StatisticColumn> getQualifiedStatisticMap() {
        Map<String, StatisticColumn> map = new HashMap<>();

        for ( StatisticColumn val : statisticFields.values() ) {
            map.put( String.valueOf( val.columnId ), val );
        }

        return map;
    }

}
