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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.ViewExpanders;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql2rel.RelStructuredTypeFlattener;
import org.polypheny.db.tools.RelBuilder;
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
            placementDistribution = partitionManager.getRelevantPlacements( table, partitionIds, new ArrayList<>( Arrays.asList( store.id ) ) );
        } else {
            placementDistribution.put( table.partitionProperty.partitionIds.get( 0 ), selectSourcePlacements( table, selectColumnList, targetColumnPlacements.get( 0 ).adapterId ) );
        }

        for ( long partitionId : partitionIds ) {
            Statement sourceStatement = transaction.createStatement();
            Statement targetStatement = transaction.createStatement();

            RelRoot sourceRel = getSourceIterator( sourceStatement, placementDistribution );
            RelRoot targetRel;
            if ( Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( store.id, table.id ).size() == columns.size() ) {
                // There have been no placements for this table on this store before. Build insert statement
                targetRel = buildInsertStatement( targetStatement, targetColumnPlacements, partitionId );
            } else {
                // Build update statement
                targetRel = buildUpdateStatement( targetStatement, targetColumnPlacements, partitionId );
            }

            // Execute Query
            executeQuery( selectColumnList, sourceRel, sourceStatement, targetStatement, targetRel, false );
        }
    }


    @Override
    public void executeQuery( List<CatalogColumn> selectColumnList, RelRoot sourceRel, Statement sourceStatement, Statement targetStatement, RelRoot targetRel, boolean isMaterializedView ) {
        try {
            PolyphenyDbSignature signature;
            if ( isMaterializedView ) {
                signature = sourceStatement.getQueryProcessor().prepareQuery( sourceRel, sourceRel.validatedRowType, false, false );
            } else {
                signature = sourceStatement.getQueryProcessor().prepareQuery( sourceRel, sourceRel.rel.getCluster().getTypeFactory().builder().build(), true );
            }
            final Enumerable enumerable = signature.enumerable( sourceStatement.getDataContext() );
            //noinspection unchecked
            Iterator<Object> sourceIterator = enumerable.iterator();

            Map<Long, Integer> resultColMapping = new HashMap<>();
            for ( CatalogColumn catalogColumn : selectColumnList ) {
                int i = 0;
                for ( ColumnMetaData metaData : signature.columns ) {
                    if ( metaData.columnName.equalsIgnoreCase( catalogColumn.name ) ) {
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
                List<List<Object>> rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( sourceIterator, batchSize ), new ArrayList<>() );
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
                List<RelDataTypeField> fields;
                if ( isMaterializedView ) {
                    fields = targetRel.rel.getTable().getRowType().getFieldList();
                } else {
                    fields = sourceRel.validatedRowType.getFieldList();
                }
                int pos = 0;
                for ( Map.Entry<Long, List<Object>> v : values.entrySet() ) {
                    targetStatement.getDataContext().addParameterValues( v.getKey(), fields.get( resultColMapping.get( v.getKey() ) ).getType(), v.getValue() );
                    pos++;
                }

                Iterator iterator = targetStatement.getQueryProcessor()
                        .prepareQuery( targetRel, sourceRel.validatedRowType, true )
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
    public RelRoot buildDeleteStatement( Statement statement, List<CatalogColumnPlacement> to, long partitionId ) {
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        to.get( 0 ).adapterUniqueName,
                        to.get( 0 ).getLogicalSchemaName(),
                        to.get( 0 ).physicalSchemaName ),
                to.get( 0 ).getLogicalTableName() + "_" + partitionId );
        RelOptTable physical = statement.getTransaction().getCatalogReader().getTableForMember( qualifiedTableName );
        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

        RelOptCluster cluster = RelOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );
        RelDataTypeFactory typeFactory = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT );

        List<String> columnNames = new LinkedList<>();
        List<RexNode> values = new LinkedList<>();
        for ( CatalogColumnPlacement ccp : to ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( ccp.columnId );
            columnNames.add( ccp.getLogicalColumnName() );
            values.add( new RexDynamicParam( catalogColumn.getRelDataType( typeFactory ), (int) catalogColumn.id ) );
        }
        RelBuilder builder = RelBuilder.create( statement, cluster );
        builder.push( LogicalValues.createOneRow( cluster ) );
        builder.project( values, columnNames );

        RelNode node = modifiableTable.toModificationRel(
                cluster,
                physical,
                statement.getTransaction().getCatalogReader(),
                builder.build(),
                Operation.DELETE,
                null,
                null,
                true
        );

        return RelRoot.of( node, SqlKind.DELETE );
    }


    @Override
    public RelRoot buildInsertStatement( Statement statement, List<CatalogColumnPlacement> to, long partitionId ) {
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        to.get( 0 ).adapterUniqueName,
                        to.get( 0 ).getLogicalSchemaName(),
                        to.get( 0 ).physicalSchemaName ),
                to.get( 0 ).getLogicalTableName() + "_" + partitionId );
        RelOptTable physical = statement.getTransaction().getCatalogReader().getTableForMember( qualifiedTableName );
        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

        RelOptCluster cluster = RelOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );
        RelDataTypeFactory typeFactory = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT );

        List<String> columnNames = new LinkedList<>();
        List<RexNode> values = new LinkedList<>();
        for ( CatalogColumnPlacement ccp : to ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( ccp.columnId );
            columnNames.add( ccp.getLogicalColumnName() );
            values.add( new RexDynamicParam( catalogColumn.getRelDataType( typeFactory ), (int) catalogColumn.id ) );
        }
        RelBuilder builder = RelBuilder.create( statement, cluster );
        builder.push( LogicalValues.createOneRow( cluster ) );
        builder.project( values, columnNames );

        RelNode node = modifiableTable.toModificationRel(
                cluster,
                physical,
                statement.getTransaction().getCatalogReader(),
                builder.build(),
                Operation.INSERT,
                null,
                null,
                true
        );
        return RelRoot.of( node, SqlKind.INSERT );
    }


    private RelRoot buildUpdateStatement( Statement statement, List<CatalogColumnPlacement> to, long partitionId ) {
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        to.get( 0 ).adapterUniqueName,
                        to.get( 0 ).getLogicalSchemaName(),
                        to.get( 0 ).physicalSchemaName ),
                to.get( 0 ).getLogicalTableName() + "_" + partitionId );
        RelOptTable physical = statement.getTransaction().getCatalogReader().getTableForMember( qualifiedTableName );
        ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );

        RelOptCluster cluster = RelOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );
        RelDataTypeFactory typeFactory = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT );

        RelBuilder builder = RelBuilder.create( statement, cluster );
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
                    new RexDynamicParam( catalogColumn.getRelDataType( typeFactory ), (int) catalogColumn.id )
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
            values.add( new RexDynamicParam( catalogColumn.getRelDataType( typeFactory ), (int) catalogColumn.id ) );
        }

        builder.projectPlus( values );

        RelNode node = modifiableTable.toModificationRel(
                cluster,
                physical,
                statement.getTransaction().getCatalogReader(),
                builder.build(),
                Operation.UPDATE,
                columnNames,
                values,
                false
        );
        RelRoot relRoot = RelRoot.of( node, SqlKind.UPDATE );
        RelStructuredTypeFlattener typeFlattener = new RelStructuredTypeFlattener(
                RelBuilder.create( statement, relRoot.rel.getCluster() ),
                relRoot.rel.getCluster().getRexBuilder(),
                ViewExpanders.toRelContext( statement.getQueryProcessor(), relRoot.rel.getCluster() ),
                true );
        return relRoot.withRel( typeFlattener.rewrite( relRoot.rel ) );
    }


    @Override
    public RelRoot getSourceIterator( Statement statement, Map<Long, List<CatalogColumnPlacement>> placementDistribution ) {

        // Build Query
        RelOptCluster cluster = RelOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );

        RelNode node = statement.getRouter().buildJoinedTableScan( statement, cluster, placementDistribution );
        return RelRoot.of( node, SqlKind.SELECT );
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
     * @param placementDistribution Pre computed mapping of partitions and the necessary column placements
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

        RelRoot sourceRel = getSourceIterator( sourceStatement, placementDistribution );
        RelRoot targetRel;
        if ( Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( store.id, targetTable.id ).size() == columns.size() ) {
            // There have been no placements for this table on this store before. Build insert statement
            targetRel = buildInsertStatement( targetStatement, targetColumnPlacements, targetPartitionIds.get( 0 ) );
        } else {
            // Build update statement
            targetRel = buildUpdateStatement( targetStatement, targetColumnPlacements, targetPartitionIds.get( 0 ) );
        }

        // Execute Query
        try {
            PolyphenyDbSignature signature = sourceStatement.getQueryProcessor().prepareQuery( sourceRel, sourceRel.rel.getCluster().getTypeFactory().builder().build(), true );
            final Enumerable enumerable = signature.enumerable( sourceStatement.getDataContext() );
            //noinspection unchecked
            Iterator<Object> sourceIterator = enumerable.iterator();

            Map<Long, Integer> resultColMapping = new HashMap<>();
            for ( CatalogColumn catalogColumn : selectColumnList ) {
                int i = 0;
                for ( ColumnMetaData metaData : signature.columns ) {
                    if ( metaData.columnName.equalsIgnoreCase( catalogColumn.name ) ) {
                        resultColMapping.put( catalogColumn.id, i );
                    }
                    i++;
                }
            }

            int batchSize = RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.getInteger();
            while ( sourceIterator.hasNext() ) {
                List<List<Object>> rows = MetaImpl.collect(
                        signature.cursorFactory,
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
                Iterator iterator = targetStatement.getQueryProcessor()
                        .prepareQuery( targetRel, sourceRel.validatedRowType, true )
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
     * Currently used to to transfer data if unpartitioned is about to be partitioned.
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

        Map<Long, RelRoot> targetRels = new HashMap<>();

        RelRoot sourceRel = getSourceIterator( sourceStatement, placementDistribution );
        if ( Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( store.id, sourceTable.id ).size() == columns.size() ) {
            // There have been no placements for this table on this store before. Build insert statement
            targetPartitionIds.forEach( id -> targetRels.put( id, buildInsertStatement( targetStatements.get( id ), targetColumnPlacements, id ) ) );
        } else {
            // Build update statement
            targetPartitionIds.forEach( id -> targetRels.put( id, buildUpdateStatement( targetStatements.get( id ), targetColumnPlacements, id ) ) );
        }

        // Execute Query
        try {
            PolyphenyDbSignature signature = sourceStatement.getQueryProcessor().prepareQuery( sourceRel, sourceRel.rel.getCluster().getTypeFactory().builder().build(), true );
            final Enumerable enumerable = signature.enumerable( sourceStatement.getDataContext() );
            //noinspection unchecked
            Iterator<Object> sourceIterator = enumerable.iterator();

            Map<Long, Integer> resultColMapping = new HashMap<>();
            for ( CatalogColumn catalogColumn : selectColumnList ) {
                int i = 0;
                for ( ColumnMetaData metaData : signature.columns ) {
                    if ( metaData.columnName.equalsIgnoreCase( catalogColumn.name ) ) {
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
                List<List<Object>> rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( sourceIterator, batchSize ), new ArrayList<>() );

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
                            .prepareQuery( targetRels.get( partitionId ), sourceRel.validatedRowType, true )
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
