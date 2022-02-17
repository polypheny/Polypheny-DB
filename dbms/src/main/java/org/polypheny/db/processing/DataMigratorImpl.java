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

package org.polypheny.db.processing;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.PolyResult;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgStructuredTypeFlattener;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.TableModify.Operation;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.LimitIterator;


@Slf4j
public class DataMigratorImpl implements DataMigrator {


    @Override
    public void copyData( Transaction transaction, CatalogAdapter store, List<CatalogColumn> columns, List<Long> partitionIds ) {
        CatalogTable table = Catalog.getInstance().getTable( columns.get( 0 ).tableId );
        CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( table.primaryKey );

        // Check Lists
        List<CatalogColumnPlacement> targetColumnPlacements = new LinkedList<>();
        for ( CatalogColumn catalogColumn : columns ) {
            targetColumnPlacements.add( Catalog.getInstance().getColumnPlacement( store.id, catalogColumn.id ) );
        }

        List<CatalogColumn> selectColumnList = new LinkedList<>( columns );

        // Add primary keys to select column list
        for ( long cid : primaryKey.columnIds ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( cid );
            if ( !selectColumnList.contains( catalogColumn ) ) {
                selectColumnList.add( catalogColumn );
            }
        }

        // We need a columnPlacement for every partition
        Map<Long, List<CatalogColumnPlacement>> placementDistribution = new HashMap<>();
        if ( table.isPartitioned ) {
            PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
            PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( table.partitionProperty.partitionType );
            placementDistribution = partitionManager.getRelevantPlacements( table, partitionIds, Collections.singletonList( store.id ) );
        } else {
            placementDistribution.put(
                    table.partitionProperty.partitionIds.get( 0 ),
                    selectSourcePlacements( table, selectColumnList, targetColumnPlacements.get( 0 ).adapterId ) );
        }

        for ( long partitionId : partitionIds ) {
            Statement sourceStatement = transaction.createStatement();
            Statement targetStatement = transaction.createStatement();

            AlgRoot sourceAlg = getSourceIterator( sourceStatement, placementDistribution );
            AlgRoot targetAlg;
            if ( Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( store.id, table.id ).size() == columns.size() ) {
                // There have been no placements for this table on this store before. Build insert statement
                targetAlg = buildInsertStatement( targetStatement, targetColumnPlacements, partitionId );
            } else {
                // Build update statement
                targetAlg = buildUpdateStatement( targetStatement, targetColumnPlacements, partitionId );
            }

            // Execute Query
            executeQuery( selectColumnList, sourceAlg, sourceStatement, targetStatement, targetAlg, false, false );
        }
    }


    @Override
    public void executeQuery( List<CatalogColumn> selectColumnList, AlgRoot sourceAlg, Statement sourceStatement, Statement targetStatement, AlgRoot targetAlg, boolean isMaterializedView, boolean doesSubstituteOrderBy ) {
        try {
            PolyResult result;
            if ( isMaterializedView ) {
                result = sourceStatement.getQueryProcessor().prepareQuery(
                        sourceAlg,
                        sourceAlg.alg.getCluster().getTypeFactory().builder().build(),
                        false,
                        false,
                        doesSubstituteOrderBy );
            } else {
                result = sourceStatement.getQueryProcessor().prepareQuery(
                        sourceAlg,
                        sourceAlg.alg.getCluster().getTypeFactory().builder().build(),
                        true,
                        false,
                        false );
            }
            final Enumerable<Object> enumerable = result.enumerable( sourceStatement.getDataContext() );
            //noinspection unchecked
            Iterator<Object> sourceIterator = enumerable.iterator();

            Map<Long, Integer> resultColMapping = new HashMap<>();
            for ( CatalogColumn catalogColumn : selectColumnList ) {
                int i = 0;
                for ( AlgDataTypeField metaData : result.getRowType().getFieldList() ) {
                    if ( metaData.getName().equalsIgnoreCase( catalogColumn.name ) ) {
                        resultColMapping.put( catalogColumn.id, i );
                    }
                    i++;
                }
            }
            if ( isMaterializedView ) {
                for ( CatalogColumn catalogColumn : selectColumnList ) {
                    if ( !resultColMapping.containsKey( catalogColumn.id ) ) {
                        int i = resultColMapping.values().stream().mapToInt( v -> v ).max().orElseThrow( NoSuchElementException::new );
                        resultColMapping.put( catalogColumn.id, i + 1 );
                    }
                }
            }

            int batchSize = RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.getInteger();
            int i = 0;
            while ( sourceIterator.hasNext() ) {
                List<List<Object>> rows = MetaImpl.collect( result.getCursorFactory(), LimitIterator.of( sourceIterator, batchSize ), new ArrayList<>() );
                Map<Long, List<Object>> values = new HashMap<>();

                for ( List<Object> list : rows ) {
                    for ( Map.Entry<Long, Integer> entry : resultColMapping.entrySet() ) {
                        if ( !values.containsKey( entry.getKey() ) ) {
                            values.put( entry.getKey(), new LinkedList<>() );
                        }
                        if ( isMaterializedView ) {
                            if ( entry.getValue() > list.size() - 1 ) {
                                values.get( entry.getKey() ).add( i );
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
                    fields = targetAlg.alg.getTable().getRowType().getFieldList();
                } else {
                    fields = sourceAlg.validatedRowType.getFieldList();
                }
                int pos = 0;
                for ( Map.Entry<Long, List<Object>> v : values.entrySet() ) {
                    targetStatement.getDataContext().addParameterValues(
                            v.getKey(),
                            fields.get( resultColMapping.get( v.getKey() ) ).getType(),
                            v.getValue() );
                    pos++;
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
            }
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
    }


    @Override
    public AlgRoot buildDeleteStatement( Statement statement, List<CatalogColumnPlacement> to, long partitionId ) {
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        to.get( 0 ).adapterUniqueName,
                        to.get( 0 ).getLogicalSchemaName(),
                        to.get( 0 ).physicalSchemaName ),
                to.get( 0 ).getLogicalTableName() + "_" + partitionId );
        AlgOptTable physical = statement.getTransaction().getCatalogReader().getTableForMember( qualifiedTableName );
        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

        AlgOptCluster cluster = AlgOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

        List<String> columnNames = new LinkedList<>();
        List<RexNode> values = new LinkedList<>();
        for ( CatalogColumnPlacement ccp : to ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( ccp.columnId );
            columnNames.add( ccp.getLogicalColumnName() );
            values.add( new RexDynamicParam( catalogColumn.getAlgDataType( typeFactory ), (int) catalogColumn.id ) );
        }
        AlgBuilder builder = AlgBuilder.create( statement, cluster );
        builder.push( LogicalValues.createOneRow( cluster ) );
        builder.project( values, columnNames );

        AlgNode node = modifiableTable.toModificationAlg(
                cluster,
                physical,
                statement.getTransaction().getCatalogReader(),
                builder.build(),
                Operation.DELETE,
                null,
                null,
                true
        );

        return AlgRoot.of( node, Kind.DELETE );
    }


    @Override
    public AlgRoot buildInsertStatement( Statement statement, List<CatalogColumnPlacement> to, long partitionId ) {
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        to.get( 0 ).adapterUniqueName,
                        to.get( 0 ).getLogicalSchemaName(),
                        to.get( 0 ).physicalSchemaName ),
                to.get( 0 ).getLogicalTableName() + "_" + partitionId );
        AlgOptTable physical = statement.getTransaction().getCatalogReader().getTableForMember( qualifiedTableName );
        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

        AlgOptCluster cluster = AlgOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

        List<String> columnNames = new LinkedList<>();
        List<RexNode> values = new LinkedList<>();
        for ( CatalogColumnPlacement ccp : to ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( ccp.columnId );
            columnNames.add( ccp.getLogicalColumnName() );
            values.add( new RexDynamicParam( catalogColumn.getAlgDataType( typeFactory ), (int) catalogColumn.id ) );
        }
        AlgBuilder builder = AlgBuilder.create( statement, cluster );
        builder.push( LogicalValues.createOneRow( cluster ) );
        builder.project( values, columnNames );

        AlgNode node = modifiableTable.toModificationAlg(
                cluster,
                physical,
                statement.getTransaction().getCatalogReader(),
                builder.build(),
                Operation.INSERT,
                null,
                null,
                true
        );
        return AlgRoot.of( node, Kind.INSERT );
    }


    private AlgRoot buildUpdateStatement( Statement statement, List<CatalogColumnPlacement> to, long partitionId ) {
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        to.get( 0 ).adapterUniqueName,
                        to.get( 0 ).getLogicalSchemaName(),
                        to.get( 0 ).physicalSchemaName ),
                to.get( 0 ).getLogicalTableName() + "_" + partitionId );
        AlgOptTable physical = statement.getTransaction().getCatalogReader().getTableForMember( qualifiedTableName );
        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

        AlgOptCluster cluster = AlgOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

        AlgBuilder builder = AlgBuilder.create( statement, cluster );
        builder.scan( qualifiedTableName );

        // build condition
        RexNode condition = null;
        CatalogTable catalogTable = Catalog.getInstance().getTable( to.get( 0 ).tableId );
        CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey );
        for ( long cid : primaryKey.columnIds ) {
            CatalogColumnPlacement ccp = Catalog.getInstance().getColumnPlacement( to.get( 0 ).adapterId, cid );
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( cid );
            RexNode c = builder.equals(
                    builder.field( ccp.getLogicalColumnName() ),
                    new RexDynamicParam( catalogColumn.getAlgDataType( typeFactory ), (int) catalogColumn.id )
            );
            if ( condition == null ) {
                condition = c;
            } else {
                condition = builder.and( condition, c );
            }
        }
        builder = builder.filter( condition );

        List<String> columnNames = new LinkedList<>();
        List<RexNode> values = new LinkedList<>();
        for ( CatalogColumnPlacement ccp : to ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( ccp.columnId );
            columnNames.add( ccp.getLogicalColumnName() );
            values.add( new RexDynamicParam( catalogColumn.getAlgDataType( typeFactory ), (int) catalogColumn.id ) );
        }

        builder.projectPlus( values );

        AlgNode node = modifiableTable.toModificationAlg(
                cluster,
                physical,
                statement.getTransaction().getCatalogReader(),
                builder.build(),
                Operation.UPDATE,
                columnNames,
                values,
                false
        );
        AlgRoot algRoot = AlgRoot.of( node, Kind.UPDATE );
        AlgStructuredTypeFlattener typeFlattener = new AlgStructuredTypeFlattener(
                AlgBuilder.create( statement, algRoot.alg.getCluster() ),
                algRoot.alg.getCluster().getRexBuilder(),
                algRoot.alg::getCluster,
                true );
        return algRoot.withAlg( typeFlattener.rewrite( algRoot.alg ) );
    }


    @Override
    public AlgRoot getSourceIterator( Statement statement, Map<Long, List<CatalogColumnPlacement>> placementDistribution ) {

        // Build Query
        AlgOptCluster cluster = AlgOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );

        AlgNode node = RoutingManager.getInstance().getFallbackRouter().buildJoinedTableScan( statement, cluster, placementDistribution );
        return AlgRoot.of( node, Kind.SELECT );
    }


    private List<CatalogColumnPlacement> selectSourcePlacements( CatalogTable table, List<CatalogColumn> columns, int excludingAdapterId ) {
        // Find the adapter with the most column placements
        int adapterIdWithMostPlacements = -1;
        int numOfPlacements = 0;
        for ( Entry<Integer, ImmutableList<Long>> entry : table.placementsByAdapter.entrySet() ) {
            if ( entry.getKey() != excludingAdapterId && entry.getValue().size() > numOfPlacements ) {
                adapterIdWithMostPlacements = entry.getKey();
                numOfPlacements = entry.getValue().size();
            }
        }

        List<Long> columnIds = new LinkedList<>();
        for ( CatalogColumn catalogColumn : columns ) {
            columnIds.add( catalogColumn.id );
        }

        // Take the adapter with most placements as base and add missing column placements
        List<CatalogColumnPlacement> placementList = new LinkedList<>();
        for ( long cid : table.columnIds ) {
            if ( columnIds.contains( cid ) ) {
                if ( table.placementsByAdapter.get( adapterIdWithMostPlacements ).contains( cid ) ) {
                    placementList.add( Catalog.getInstance().getColumnPlacement( adapterIdWithMostPlacements, cid ) );
                } else {
                    for ( CatalogColumnPlacement placement : Catalog.getInstance().getColumnPlacement( cid ) ) {
                        if ( placement.adapterId != excludingAdapterId ) {
                            placementList.add( placement );
                            break;
                        }
                    }
                }
            }
        }

        return placementList;
    }


    /**
     * Currently used to to transfer data if partitioned table is about to be merged.
     * For Table Partitioning use {@link #copyPartitionData(Transaction, CatalogAdapter, CatalogTable, CatalogTable, List, List, List)}  } instead
     *
     * @param transaction Transactional scope
     * @param store Target Store where data should be migrated to
     * @param sourceTable Source Table from where data is queried
     * @param targetTable Source Table from where data is queried
     * @param columns Necessary columns on target
     * @param placementDistribution Pre-computed mapping of partitions and the necessary column placements
     * @param targetPartitionIds Target Partitions where data should be inserted
     */
    @Override
    public void copySelectiveData( Transaction transaction, CatalogAdapter store, CatalogTable sourceTable, CatalogTable targetTable, List<CatalogColumn> columns, Map<Long, List<CatalogColumnPlacement>> placementDistribution, List<Long> targetPartitionIds ) {
        CatalogPrimaryKey sourcePrimaryKey = Catalog.getInstance().getPrimaryKey( sourceTable.primaryKey );

        // Check Lists
        List<CatalogColumnPlacement> targetColumnPlacements = new LinkedList<>();
        for ( CatalogColumn catalogColumn : columns ) {
            targetColumnPlacements.add( Catalog.getInstance().getColumnPlacement( store.id, catalogColumn.id ) );
        }

        List<CatalogColumn> selectColumnList = new LinkedList<>( columns );

        // Add primary keys to select column list
        for ( long cid : sourcePrimaryKey.columnIds ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( cid );
            if ( !selectColumnList.contains( catalogColumn ) ) {
                selectColumnList.add( catalogColumn );
            }
        }

        Statement sourceStatement = transaction.createStatement();
        Statement targetStatement = transaction.createStatement();

        AlgRoot sourceAlg = getSourceIterator( sourceStatement, placementDistribution );
        AlgRoot targetAlg;
        if ( Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( store.id, targetTable.id ).size() == columns.size() ) {
            // There have been no placements for this table on this store before. Build insert statement
            targetAlg = buildInsertStatement( targetStatement, targetColumnPlacements, targetPartitionIds.get( 0 ) );
        } else {
            // Build update statement
            targetAlg = buildUpdateStatement( targetStatement, targetColumnPlacements, targetPartitionIds.get( 0 ) );
        }

        // Execute Query
        try {
            PolyResult result = sourceStatement.getQueryProcessor().prepareQuery( sourceAlg, sourceAlg.alg.getCluster().getTypeFactory().builder().build(), true, false, false );
            final Enumerable<Object> enumerable = result.enumerable( sourceStatement.getDataContext() );
            //noinspection unchecked
            Iterator<Object> sourceIterator = enumerable.iterator();

            Map<Long, Integer> resultColMapping = new HashMap<>();
            for ( CatalogColumn catalogColumn : selectColumnList ) {
                int i = 0;
                for ( AlgDataTypeField metaData : result.getRowType().getFieldList() ) {
                    if ( metaData.getName().equalsIgnoreCase( catalogColumn.name ) ) {
                        resultColMapping.put( catalogColumn.id, i );
                    }
                    i++;
                }
            }

            int batchSize = RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.getInteger();
            while ( sourceIterator.hasNext() ) {
                List<List<Object>> rows = MetaImpl.collect(
                        result.getCursorFactory(),
                        LimitIterator.of( sourceIterator, batchSize ),
                        new ArrayList<>() );
                Map<Long, List<Object>> values = new HashMap<>();
                for ( List<Object> list : rows ) {
                    for ( Map.Entry<Long, Integer> entry : resultColMapping.entrySet() ) {
                        if ( !values.containsKey( entry.getKey() ) ) {
                            values.put( entry.getKey(), new LinkedList<>() );
                        }
                        values.get( entry.getKey() ).add( list.get( entry.getValue() ) );
                    }
                }
                for ( Map.Entry<Long, List<Object>> v : values.entrySet() ) {
                    targetStatement.getDataContext().addParameterValues( v.getKey(), null, v.getValue() );
                }
                Iterator<?> iterator = targetStatement.getQueryProcessor()
                        .prepareQuery( targetAlg, sourceAlg.validatedRowType, true, false, true )
                        .enumerable( targetStatement.getDataContext() )
                        .iterator();

                //noinspection WhileLoopReplaceableByForEach
                while ( iterator.hasNext() ) {
                    iterator.next();
                }
                targetStatement.getDataContext().resetParameterValues();
            }
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
    }


    /**
     * Currently used to transfer data if unpartitioned is about to be partitioned.
     * For Table Merge use {@link #copySelectiveData(Transaction, CatalogAdapter, CatalogTable, CatalogTable, List, Map, List)}   } instead
     *
     * @param transaction Transactional scope
     * @param store Target Store where data should be migrated to
     * @param sourceTable Source Table from where data is queried
     * @param targetTable Target Table where data is to be inserted
     * @param columns Necessary columns on target
     * @param sourcePartitionIds Source Partitions which need to be considered for querying
     * @param targetPartitionIds Target Partitions where data should be inserted
     */
    @Override
    public void copyPartitionData( Transaction transaction, CatalogAdapter store, CatalogTable sourceTable, CatalogTable targetTable, List<CatalogColumn> columns, List<Long> sourcePartitionIds, List<Long> targetPartitionIds ) {
        if ( sourceTable.id != targetTable.id ) {
            throw new RuntimeException( "Unsupported migration scenario. Table ID mismatch" );
        }

        CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( sourceTable.primaryKey );

        // Check Lists
        List<CatalogColumnPlacement> targetColumnPlacements = new LinkedList<>();
        for ( CatalogColumn catalogColumn : columns ) {
            targetColumnPlacements.add( Catalog.getInstance().getColumnPlacement( store.id, catalogColumn.id ) );
        }

        List<CatalogColumn> selectColumnList = new LinkedList<>( columns );

        // Add primary keys to select column list
        for ( long cid : primaryKey.columnIds ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( cid );
            if ( !selectColumnList.contains( catalogColumn ) ) {
                selectColumnList.add( catalogColumn );
            }
        }

        // Add partition columns to select column list
        long partitionColumnId = targetTable.partitionProperty.partitionColumnId;
        CatalogColumn partitionColumn = Catalog.getInstance().getColumn( partitionColumnId );
        if ( !selectColumnList.contains( partitionColumn ) ) {
            selectColumnList.add( partitionColumn );
        }

        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( targetTable.partitionProperty.partitionType );

        //We need a columnPlacement for every partition
        Map<Long, List<CatalogColumnPlacement>> placementDistribution = new HashMap<>();

        placementDistribution.put( sourceTable.partitionProperty.partitionIds.get( 0 ), selectSourcePlacements( sourceTable, selectColumnList, -1 ) );

        Statement sourceStatement = transaction.createStatement();

        //Map PartitionId to TargetStatementQueue
        Map<Long, Statement> targetStatements = new HashMap<>();

        //Creates queue of target Statements depending
        targetPartitionIds.forEach( id -> targetStatements.put( id, transaction.createStatement() ) );

        Map<Long, AlgRoot> targetAlgs = new HashMap<>();

        AlgRoot sourceAlg = getSourceIterator( sourceStatement, placementDistribution );
        if ( Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( store.id, sourceTable.id ).size() == columns.size() ) {
            // There have been no placements for this table on this store before. Build insert statement
            targetPartitionIds.forEach( id -> targetAlgs.put( id, buildInsertStatement( targetStatements.get( id ), targetColumnPlacements, id ) ) );
        } else {
            // Build update statement
            targetPartitionIds.forEach( id -> targetAlgs.put( id, buildUpdateStatement( targetStatements.get( id ), targetColumnPlacements, id ) ) );
        }

        // Execute Query
        try {
            PolyResult result = sourceStatement.getQueryProcessor().prepareQuery( sourceAlg, sourceAlg.alg.getCluster().getTypeFactory().builder().build(), true, false, false );
            final Enumerable<?> enumerable = result.enumerable( sourceStatement.getDataContext() );
            //noinspection unchecked
            Iterator<Object> sourceIterator = (Iterator<Object>) enumerable.iterator();

            Map<Long, Integer> resultColMapping = new HashMap<>();
            for ( CatalogColumn catalogColumn : selectColumnList ) {
                int i = 0;
                for ( AlgDataTypeField metaData : result.getRowType().getFieldList() ) {
                    if ( metaData.getName().equalsIgnoreCase( catalogColumn.name ) ) {
                        resultColMapping.put( catalogColumn.id, i );
                    }
                    i++;
                }
            }

            int partitionColumnIndex = -1;
            String parsedValue = null;
            String nullifiedPartitionValue = partitionManager.getUnifiedNullValue();
            if ( targetTable.isPartitioned ) {
                if ( resultColMapping.containsKey( targetTable.partitionProperty.partitionColumnId ) ) {
                    partitionColumnIndex = resultColMapping.get( targetTable.partitionProperty.partitionColumnId );
                } else {
                    parsedValue = nullifiedPartitionValue;
                }
            }

            int batchSize = RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.getInteger();
            while ( sourceIterator.hasNext() ) {
                List<List<Object>> rows = MetaImpl.collect( result.getCursorFactory(), LimitIterator.of( sourceIterator, batchSize ), new ArrayList<>() );

                Map<Long, Map<Long, List<Object>>> partitionValues = new HashMap<>();

                for ( List<Object> row : rows ) {
                    long currentPartitionId = -1;
                    if ( partitionColumnIndex >= 0 ) {
                        parsedValue = nullifiedPartitionValue;
                        if ( row.get( partitionColumnIndex ) != null ) {
                            parsedValue = row.get( partitionColumnIndex ).toString();
                        }
                    }

                    currentPartitionId = partitionManager.getTargetPartitionId( targetTable, parsedValue );

                    for ( Map.Entry<Long, Integer> entry : resultColMapping.entrySet() ) {
                        if ( entry.getKey() == partitionColumn.id && !columns.contains( partitionColumn ) ) {
                            continue;
                        }
                        if ( !partitionValues.containsKey( currentPartitionId ) ) {
                            partitionValues.put( currentPartitionId, new HashMap<>() );
                        }
                        if ( !partitionValues.get( currentPartitionId ).containsKey( entry.getKey() ) ) {
                            partitionValues.get( currentPartitionId ).put( entry.getKey(), new LinkedList<>() );
                        }
                        partitionValues.get( currentPartitionId ).get( entry.getKey() ).add( row.get( entry.getValue() ) );
                    }
                }

                // Iterate over partitionValues in that way we don't even execute a statement which has no rows
                for ( Map.Entry<Long, Map<Long, List<Object>>> dataOnPartition : partitionValues.entrySet() ) {
                    long partitionId = dataOnPartition.getKey();
                    Map<Long, List<Object>> values = dataOnPartition.getValue();
                    Statement currentTargetStatement = targetStatements.get( partitionId );

                    for ( Map.Entry<Long, List<Object>> columnDataOnPartition : values.entrySet() ) {
                        // Check partitionValue
                        currentTargetStatement.getDataContext().addParameterValues( columnDataOnPartition.getKey(), null, columnDataOnPartition.getValue() );
                    }

                    Iterator iterator = currentTargetStatement.getQueryProcessor()
                            .prepareQuery( targetAlgs.get( partitionId ), sourceAlg.validatedRowType, true, false, false )
                            .enumerable( currentTargetStatement.getDataContext() )
                            .iterator();
                    //noinspection WhileLoopReplaceableByForEach
                    while ( iterator.hasNext() ) {
                        iterator.next();
                    }
                    currentTargetStatement.getDataContext().resetParameterValues();
                }
            }
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
    }

}
