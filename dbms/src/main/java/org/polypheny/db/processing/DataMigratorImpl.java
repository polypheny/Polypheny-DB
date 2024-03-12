/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.processing;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgStructuredTypeFlattener;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.ColumnDistribution;
import org.polypheny.db.routing.RoutingContext;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.Pair;


@Slf4j
public class DataMigratorImpl implements DataMigrator {

    @Override
    public void copyGraphData( AllocationGraph to, LogicalGraph from, Transaction transaction ) {
        Statement statement = transaction.createStatement();

        AlgBuilder builder = AlgBuilder.create( statement );

        LogicalLpgScan scan = (LogicalLpgScan) builder.lpgScan( from ).build();

        AlgNode routed = RoutingManager.getInstance().getFallbackRouter().handleGraphScan( scan, statement, null, List.of( to.id ) );

        AlgRoot algRoot = AlgRoot.of( routed, Kind.SELECT );

        AlgStructuredTypeFlattener typeFlattener = new AlgStructuredTypeFlattener(
                AlgBuilder.create( statement, algRoot.alg.getCluster() ),
                algRoot.alg.getCluster().getRexBuilder(),
                algRoot.alg.getCluster(),
                true );
        algRoot = algRoot.withAlg( typeFlattener.rewrite( algRoot.alg ) );

        PolyImplementation result = statement.getQueryProcessor().prepareQuery(
                algRoot,
                algRoot.alg.getCluster().getTypeFactory().builder().build(),
                true,
                false,
                false );

        final Enumerable<PolyValue[]> enumerable = result.enumerable( statement.getDataContext() );

        Iterator<PolyValue[]> sourceIterator = enumerable.iterator();

        if ( sourceIterator.hasNext() ) {
            PolyGraph graph = sourceIterator.next()[0].asGraph();

            if ( graph.getEdges().isEmpty() && graph.getNodes().isEmpty() ) {
                // nothing to copy
                return;
            }

            // we have a new statement
            statement = transaction.createStatement();
            builder = AlgBuilder.create( statement );

            LogicalLpgValues values = getLogicalLpgValues( builder, graph );

            LogicalLpgModify modify = new LogicalLpgModify( builder.getCluster(), builder.getCluster().traitSetOf( ModelTrait.GRAPH ), to, values, Modify.Operation.INSERT, null, null );

            AlgNode routedModify = RoutingManager.getInstance().getDmlRouter().routeGraphDml( modify, statement, to, List.of( to.placementId ) );

            result = statement.getQueryProcessor().prepareQuery(
                    AlgRoot.of( routedModify, Kind.SELECT ),
                    routedModify.getCluster().getTypeFactory().builder().build(),
                    true,
                    false,
                    false );

            final Enumerable<PolyValue[]> modifyEnumerable = result.enumerable( statement.getDataContext() );

            Iterator<PolyValue[]> modifyIterator = modifyEnumerable.iterator();
            if ( modifyIterator.hasNext() ) {
                modifyIterator.next();
            }
        }
    }


    @Override
    public void copyDocData( AllocationCollection to, LogicalCollection from, Transaction transaction ) {
        Statement statement = transaction.createStatement();

        AlgBuilder builder = AlgBuilder.create( statement );

        LogicalDocumentScan scan = (LogicalDocumentScan) builder.documentScan( from ).build();

        AlgNode routed = RoutingManager.getInstance().getFallbackRouter().handleDocScan( scan, statement, List.of( to.id ) );

        AlgRoot algRoot = AlgRoot.of( routed, Kind.SELECT );

        AlgStructuredTypeFlattener typeFlattener = new AlgStructuredTypeFlattener(
                AlgBuilder.create( statement, algRoot.alg.getCluster() ),
                algRoot.alg.getCluster().getRexBuilder(),
                algRoot.alg.getCluster(),
                true );
        algRoot = algRoot.withAlg( typeFlattener.rewrite( algRoot.alg ) );

        PolyImplementation implementation = statement.getQueryProcessor().prepareQuery(
                algRoot,
                algRoot.alg.getCluster().getTypeFactory().builder().build(),
                true,
                false,
                false );

        int batchSize = RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.getInteger();

        final ResultIterator sourceIterator = implementation.execute( statement, batchSize );

        do {
            // we have a new statement
            statement = transaction.createStatement();
            builder = AlgBuilder.create( statement );

            LogicalDocumentValues values = getLogicalDocValues( builder, sourceIterator.getNextBatch() );

            if ( values.dynamicDocuments.isEmpty() && values.documents.isEmpty() ) {
                // no values retrieved
                return;
            }

            LogicalDocumentModify modify = new LogicalDocumentModify( builder.getCluster().traitSetOf( ModelTrait.DOCUMENT ), to, values, Modify.Operation.INSERT, null, null, null );

            // AlgNode routedModify = RoutingManager.getInstance().getDmlRouter().routeDocumentDml( modify, statement, to, List.of( to.placementId ) );

            PolyImplementation modifyImpl = statement.getQueryProcessor().prepareQuery(
                    AlgRoot.of( modify, Kind.SELECT ),
                    modify.getCluster().getTypeFactory().builder().build(),
                    true,
                    false,
                    false );

            final ResultIterator modifyIterator = modifyImpl.execute( statement, batchSize );

            while ( modifyIterator.hasMoreRows() ) {
                modifyIterator.getNextBatch();
            }
        } while ( implementation.hasMoreRows() );
    }


    private LogicalDocumentValues getLogicalDocValues( AlgBuilder builder, List<List<PolyValue>> tuples ) {
        List<PolyDocument> documents = tuples.stream().map( l -> l.get( 0 ) ).map( PolyValue::asDocument ).toList();

        return new LogicalDocumentValues( builder.getCluster(), builder.getCluster().traitSet(), documents );
    }


    @NotNull
    private static LogicalLpgValues getLogicalLpgValues( AlgBuilder builder, PolyGraph graph ) {
        List<AlgDataTypeField> fields = new ArrayList<>();
        int index = 0;
        if ( !graph.getNodes().isEmpty() ) {
            fields.add( new AlgDataTypeFieldImpl( 1L, "n", index, builder.getTypeFactory().createPolyType( PolyType.NODE ) ) );
            index++;
        }
        if ( !graph.getEdges().isEmpty() ) {
            fields.add( new AlgDataTypeFieldImpl( 1L, "e", index, builder.getTypeFactory().createPolyType( PolyType.EDGE ) ) );
        }

        return new LogicalLpgValues( builder.getCluster(), builder.getCluster().traitSetOf( ModelTrait.GRAPH ), graph.getNodes().values(), graph.getEdges().values(), ImmutableList.of(), new AlgRecordType( fields ) );
    }


    @Override
    public void copyData(
            Transaction transaction,
            LogicalAdapter targetStore,
            LogicalTable source,
            List<LogicalColumn> columns,
            AllocationEntity target ) {
        Snapshot snapshot = Catalog.snapshot();

        Statement sourceStatement = transaction.createStatement();
        Statement targetStatement = transaction.createStatement();

        List<LogicalColumn> copyColumns = new ArrayList<>( columns );
        // we add the primary key columns to the list of columns to copy, if they are not already in the list
        snapshot.rel().getPrimaryKey( source.primaryKey ).orElseThrow().fieldIds.forEach( pkColumnId -> {
            if ( columns.stream().noneMatch( c -> c.id == pkColumnId ) ) {
                copyColumns.add( snapshot.rel().getColumn( pkColumnId ).orElseThrow() );
            }
        } );

        AlgRoot sourceAlg = getSourceIterator(
                sourceStatement,
                new ColumnDistribution( source.id, copyColumns.stream().map( c -> c.id ).toList(), List.of( target.partitionId ), List.of( target.partitionId ), List.of( target.id ), snapshot ) );
        AlgRoot targetAlg;
        AllocationTable allocation = target.unwrap( AllocationTable.class ).orElseThrow();
        Catalog.getInstance().updateSnapshot();
        if ( allocation.getColumns().size() == columns.size() ) {
            // There have been no placements for this table on this storeId before. Build insert statement
            targetAlg = buildInsertStatement( targetStatement, allocation.getColumns(), allocation );
        } else {
            // Build update statement
            targetAlg = buildUpdateStatement( targetStatement, columns.stream().map( c -> Catalog.snapshot().alloc().getColumn( target.placementId, c.id ).orElseThrow() ).toList(), allocation );
        }

        // Execute Query
        executeQuery( allocation.getColumns(), sourceAlg, sourceStatement, targetStatement, targetAlg, false, false );
    }


    @Override
    public void copyData(
            Transaction transaction,
            LogicalAdapter targetStore,
            LogicalTable source,
            List<LogicalColumn> columns,
            AllocationPlacement target ) {
        Snapshot snapshot = Catalog.snapshot();
        for ( AllocationEntity entity : snapshot.alloc().getAllocsOfPlacement( target.id ) ) {
            copyData( transaction, targetStore, source, columns, entity );
        }
    }


    @Override
    public void executeQuery( List<AllocationColumn> selectedColumns, AlgRoot sourceAlg, Statement sourceStatement, Statement targetStatement, AlgRoot targetAlg, boolean isMaterializedView, boolean doesSubstituteOrderBy ) {
        try {
            PolyImplementation implementation;
            if ( isMaterializedView ) {
                implementation = sourceStatement.getQueryProcessor().prepareQuery(
                        sourceAlg,
                        sourceAlg.alg.getCluster().getTypeFactory().builder().build(),
                        false,
                        false,
                        doesSubstituteOrderBy );
            } else {
                implementation = sourceStatement.getQueryProcessor().prepareQuery(
                        sourceAlg,
                        sourceAlg.alg.getCluster().getTypeFactory().builder().build(),
                        true,
                        false,
                        false );
            }

            Map<Long, Integer> resultColMapping = new HashMap<>();
            for ( AllocationColumn column : selectedColumns ) {
                int i = 0;
                for ( AlgDataTypeField metaData : implementation.getTupleType().getFields() ) {
                    if ( metaData.getName().equalsIgnoreCase( column.getLogicalColumnName() ) ) {
                        resultColMapping.put( column.getColumnId(), i );
                    }
                    i++;
                }
            }
            if ( isMaterializedView ) {
                for ( AllocationColumn column : selectedColumns ) {
                    if ( !resultColMapping.containsKey( column.getColumnId() ) ) {
                        int i = resultColMapping.values().stream().mapToInt( v -> v ).max().orElseThrow( NoSuchElementException::new );
                        resultColMapping.put( column.getColumnId(), i + 1 );
                    }
                }
            }

            int batchSize = RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.getInteger();
            int i = 0;

            do {
                ResultIterator iter = implementation.execute( sourceStatement, batchSize );
                List<List<PolyValue>> rows = iter.getNextBatch();
                iter.close();
                if ( rows.isEmpty() ) {
                    continue;
                }
                Map<Long, List<PolyValue>> values = new HashMap<>();

                for ( List<PolyValue> list : rows ) {
                    for ( Map.Entry<Long, Integer> entry : resultColMapping.entrySet() ) {
                        if ( !values.containsKey( entry.getKey() ) ) {
                            values.put( entry.getKey(), new LinkedList<>() );
                        }
                        if ( isMaterializedView ) {
                            if ( entry.getValue() > list.size() - 1 ) {
                                values.get( entry.getKey() ).add( PolyInteger.of( i ) );
                                i++;
                            } else {
                                values.get( entry.getKey() ).add( list.get( entry.getValue() ) );
                            }
                        } else {
                            values.get( entry.getKey() ).add( list.get( entry.getValue() ) );
                        }
                    }
                }
                List<AlgDataTypeField> fields;
                if ( isMaterializedView ) {
                    fields = targetAlg.alg.getEntity().getTupleType().getFields();
                } else {
                    fields = sourceAlg.validatedRowType.getFields();
                }

                for ( Map.Entry<Long, List<PolyValue>> v : values.entrySet() ) {
                    targetStatement.getDataContext().addParameterValues(
                            v.getKey(),
                            fields.get( resultColMapping.get( v.getKey() ) ).getType(),
                            v.getValue() );
                }

                Iterator<?> iterator = targetStatement.getQueryProcessor()
                        .prepareQuery( targetAlg, sourceAlg.validatedRowType, true, false, false )
                        .enumerable( targetStatement.getDataContext() )
                        .iterator();

                //noinspection WhileLoopReplaceableByForEach
                while ( iterator.hasNext() ) {
                    iterator.next();
                }
                targetStatement.getDataContext().resetParameterValues();
            } while ( implementation.hasMoreRows() );
        } catch ( Throwable t ) {
            throw new GenericRuntimeException( t );
        }
    }


    @Override
    public AlgRoot buildDeleteStatement( Statement statement, List<AllocationColumn> to, AllocationEntity allocation ) {
        AlgCluster cluster = AlgCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ), null, statement.getTransaction().getSnapshot() );
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

        List<String> columnNames = new LinkedList<>();
        List<RexNode> values = new ArrayList<>();
        for ( AllocationColumn ccp : to ) {
            LogicalColumn logicalColumn = Catalog.getInstance().getSnapshot().rel().getColumn( ccp.columnId ).orElseThrow();
            columnNames.add( ccp.getLogicalColumnName() );
            values.add( new RexDynamicParam( logicalColumn.getAlgDataType( typeFactory ), (int) logicalColumn.id ) );
        }
        AlgBuilder builder = AlgBuilder.create( statement, cluster );

        if ( to.isEmpty() ) {
            builder.relScan( allocation );
        } else {
            builder.push( LogicalRelValues.createOneRow( cluster ) );
            builder.project( values, columnNames );
        }


        AlgNode node = LogicalRelModify.create( allocation, builder.build(), Modify.Operation.DELETE, null, null, false );

        return AlgRoot.of( node, Kind.DELETE );
    }


    @Override
    public AlgRoot buildInsertStatement( Statement statement, List<AllocationColumn> to, AllocationEntity allocation ) {

        AlgCluster cluster = AlgCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ), null, statement.getDataContext().getSnapshot() );
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

        // while adapters should be able to handle unsorted columnIds for prepared indexes,
        // this often leads to errors, and can be prevented by sorting
        List<AllocationColumn> placements = to.stream().sorted( Comparator.comparingLong( p -> p.columnId ) ).toList();

        List<String> columnNames = new LinkedList<>();
        List<RexNode> values = new LinkedList<>();
        for ( AllocationColumn ccp : placements ) {
            LogicalColumn logicalColumn = Catalog.getInstance().getSnapshot().rel().getColumn( ccp.columnId ).orElseThrow();
            columnNames.add( ccp.getLogicalColumnName() );
            values.add( new RexDynamicParam( logicalColumn.getAlgDataType( typeFactory ), (int) logicalColumn.id ) );
        }
        AlgBuilder builder = AlgBuilder.create( statement, cluster );
        builder.push( LogicalRelValues.createOneRow( cluster ) );
        builder.project( values, columnNames );

        AlgNode node = LogicalRelModify.create( allocation, builder.build(), Operation.INSERT, null, null, false );

        return AlgRoot.of( node, Kind.INSERT );
    }


    private AlgRoot buildUpdateStatement( Statement statement, List<AllocationColumn> to, AllocationEntity allocation ) {
        AlgCluster cluster = AlgCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ), null, statement.getDataContext().getSnapshot() );

        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

        AlgBuilder builder = AlgBuilder.create( statement, cluster );
        builder.relScan( allocation );

        // build condition
        RexNode condition = null;
        LogicalRelSnapshot snapshot = Catalog.getInstance().getSnapshot().rel();
        LogicalTable catalogTable = snapshot.getTable( to.get( 0 ).logicalTableId ).orElseThrow();
        LogicalPrimaryKey primaryKey = snapshot.getPrimaryKey( catalogTable.primaryKey ).orElseThrow();
        for ( long cid : primaryKey.fieldIds ) {
            AllocationColumn ccp = Catalog.getInstance().getSnapshot().alloc().getColumn( to.get( 0 ).placementId, cid ).orElseThrow();
            LogicalColumn logicalColumn = snapshot.getColumn( cid ).orElseThrow();
            RexNode c = builder.equals(
                    builder.field( ccp.getLogicalColumnName() ),
                    new RexDynamicParam( logicalColumn.getAlgDataType( typeFactory ), (int) logicalColumn.id )
            );
            if ( condition == null ) {
                condition = c;
            } else {
                condition = builder.and( condition, c );
            }
        }
        builder = builder.filter( condition );

        List<String> columnNames = new ArrayList<>();
        List<RexNode> values = new ArrayList<>();
        for ( AllocationColumn ccp : to ) {
            LogicalColumn logicalColumn = snapshot.getColumn( ccp.columnId ).orElseThrow();
            columnNames.add( ccp.getLogicalColumnName() );
            values.add( new RexDynamicParam( logicalColumn.getAlgDataType( typeFactory ), (int) logicalColumn.id ) );
        }

        builder.projectPlus( values );

        AlgNode node = builder.push(
                LogicalRelModify.create( allocation, builder.build(), Modify.Operation.UPDATE, columnNames, values, false )
        ).build();
        AlgRoot algRoot = AlgRoot.of( node, Kind.UPDATE );
        AlgStructuredTypeFlattener typeFlattener = new AlgStructuredTypeFlattener(
                AlgBuilder.create( statement, algRoot.alg.getCluster() ),
                algRoot.alg.getCluster().getRexBuilder(),
                algRoot.alg.getCluster(),
                true );
        return algRoot.withAlg( typeFlattener.rewrite( algRoot.alg ) );
    }


    @Override
    public AlgRoot getSourceIterator( Statement statement, ColumnDistribution placementDistribution ) {

        // Build Query
        AlgCluster cluster = AlgCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ), null, statement.getDataContext().getSnapshot() );

        AlgNode node = RoutingManager.getInstance().getFallbackRouter().buildJoinedScan( placementDistribution, new RoutingContext( cluster, statement, null ) );
        return AlgRoot.of( node, Kind.SELECT );
    }


    /**
     * Currently used to transfer data if unpartitioned is about to be partitioned.
     *
     * @param transaction Transactional scope
     * @param store Target Store where data should be migrated to
     * @param sourceTables Source Table from where data is queried
     */
    @Override
    public void copyAllocationData( Transaction transaction, LogicalAdapter store, List<AllocationTable> sourceTables, PartitionProperty targetProperty, List<AllocationTable> targetTables, LogicalTable table ) {
        if ( targetTables.stream().anyMatch( t -> t.logicalId != sourceTables.get( 0 ).logicalId ) ) {
            throw new GenericRuntimeException( "Unsupported migration scenario. Table ID mismatch" );
        }
        Snapshot snapshot = Catalog.getInstance().getSnapshot();

        // Add partition columns to select column list
        long partitionColumnId = targetProperty.partitionColumnId;
        LogicalColumn partitionColumn = partitionColumnId == -1 ? null : snapshot.rel().getColumn( partitionColumnId ).orElseThrow();

        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( targetProperty.partitionType );

        Source source = getSource( transaction, sourceTables, table, partitionColumn, targetTables );

        Map<Long, Statement> targetStatements = new HashMap<>();
        //Creates queue of target Statements depending
        targetTables.forEach( t -> targetStatements.put( t.partitionId, transaction.createStatement() ) );
        //Map PartitionId to TargetStatementQueue
        Map<Long, AlgRoot> targetAlgs = new HashMap<>();

        List<AllocationColumn> columns = snapshot.alloc().getColumns( targetTables.get( 0 ).placementId );
        for ( AllocationTable targetTable : targetTables ) {
            if ( targetTable.getColumns().size() == columns.size() ) {
                // There have been no placements for this table on this storeId before. Build insert statement
                targetAlgs.put( targetTable.partitionId, buildInsertStatement( targetStatements.get( targetTable.partitionId ), columns, targetTable ) );
            } else {
                // Build update statement
                targetAlgs.put( targetTable.partitionId, buildUpdateStatement( targetStatements.get( targetTable.partitionId ), columns, targetTable ) );
            }
        }

        // Execute Query
        try {
            PolyImplementation result = source.sourceStatement.getQueryProcessor().prepareQuery( source.sourceAlg, source.sourceAlg.alg.getCluster().getTypeFactory().builder().build(), true, false, false );

            int columIndex = 0;
            if ( partitionColumn != null && partitionColumn.tableId == table.id ) {
                columIndex = source.sourceAlg.alg.getTupleType().getField( partitionColumn.name, true, false ).getIndex();
            }

            //int partitionColumnIndex = -1;
            String parsedValue;

            int batchSize = RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.getInteger();

            do {
                ResultIterator iter = result.execute( source.sourceStatement, batchSize );
                List<List<PolyValue>> rows = iter.getNextBatch();
                iter.close();
                if ( rows.isEmpty() ) {
                    continue;
                }

                Map<Long, Map<Long, Pair<AlgDataType, List<PolyValue>>>> partitionValues = new HashMap<>();

                for ( List<PolyValue> row : rows ) {

                    if ( row.get( columIndex ) != null ) {
                        parsedValue = row.get( columIndex ).toString();
                    } else {
                        parsedValue = PartitionManager.NULL_STRING;
                    }

                    long currentPartitionId = partitionManager.getTargetPartitionId( table, targetProperty, parsedValue );

                    int i = 0;
                    for ( AllocationColumn column : columns.stream().sorted( Comparator.comparingInt( c -> c.position ) ).toList() ) {
                        if ( !partitionValues.containsKey( currentPartitionId ) ) {
                            partitionValues.put( currentPartitionId, new HashMap<>() );
                        }
                        if ( !partitionValues.get( currentPartitionId ).containsKey( column.columnId ) ) {
                            partitionValues.get( currentPartitionId ).put( column.columnId, Pair.of( column.getAlgDataType(), new ArrayList<>() ) );
                        }
                        partitionValues.get( currentPartitionId ).get( column.columnId ).right.add( row.get( i++ ) );
                    }
                }

                // Iterate over partitionValues in that way we don't even execute a statement which has no rows
                for ( Entry<Long, Map<Long, Pair<AlgDataType, List<PolyValue>>>> dataOnPartition : partitionValues.entrySet() ) {
                    long partitionId = dataOnPartition.getKey();
                    Map<Long, Pair<AlgDataType, List<PolyValue>>> values = dataOnPartition.getValue();
                    Statement currentTargetStatement = targetStatements.get( partitionId );

                    for ( Entry<Long, Pair<AlgDataType, List<PolyValue>>> columnDataOnPartition : values.entrySet() ) {
                        // Check partitionValue
                        currentTargetStatement.getDataContext().addParameterValues( columnDataOnPartition.getKey(), columnDataOnPartition.getValue().left, columnDataOnPartition.getValue().right );
                    }

                    Iterator<?> iterator = currentTargetStatement.getQueryProcessor()
                            .prepareQuery( targetAlgs.get( partitionId ), source.sourceAlg.validatedRowType, true, false, false )
                            .enumerable( currentTargetStatement.getDataContext() )
                            .iterator();
                    //noinspection WhileLoopReplaceableByForEach
                    while ( iterator.hasNext() ) {
                        iterator.next();
                    }
                    currentTargetStatement.getDataContext().resetParameterValues();
                }
            } while ( result.hasMoreRows() );
        } catch ( Throwable t ) {
            throw new GenericRuntimeException( t );
        }
    }


    @NotNull
    private Source getSource( Transaction transaction, List<AllocationTable> sourceTables, LogicalTable table, @Nullable LogicalColumn partitionColumn, List<AllocationTable> targetTables ) {
        List<LogicalColumn> selectColumns = Catalog.snapshot().alloc().getColumns( targetTables.get( 0 ).placementId ).stream().map( a -> Catalog.snapshot().rel().getColumn( a.columnId ).orElseThrow() ).collect( Collectors.toCollection( ArrayList::new ) );

        if ( partitionColumn != null && !selectColumns.contains( partitionColumn ) ) {
            selectColumns.add( partitionColumn );
        }

        Statement sourceStatement = transaction.createStatement();

        List<Long> sourcePartitions = List.copyOf( sourceTables.stream().map( t -> t.partitionId ).collect( Collectors.toSet() ) );
        List<Long> targetPartitions = List.copyOf( targetTables.stream().map( t -> t.partitionId ).collect( Collectors.toSet() ) );
        List<Long> excludedAllocations = List.copyOf( targetTables.stream().map( t -> t.id ).collect( Collectors.toSet() ) );
        List<Long> columnIds = selectColumns.stream().map( c -> c.id ).toList();
        AlgRoot sourceAlg = getSourceIterator( sourceStatement, new ColumnDistribution( table.id, columnIds, targetPartitions, sourcePartitions, excludedAllocations, Catalog.snapshot() ) );

        return new Source( sourceStatement, sourceAlg );

    }


    private record Source(Statement sourceStatement, AlgRoot sourceAlg) {

    }


}
