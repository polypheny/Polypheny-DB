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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.ViewExpanders;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rel.type.RelDataTypeFactory;
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

        Statement sourceStatement = transaction.createStatement();
        Statement targetStatement = transaction.createStatement();

        // Check Lists
        List<CatalogColumnPlacement> columnPlacements = new LinkedList<>();
        for ( CatalogColumn catalogColumn : columns ) {
            columnPlacements.add( Catalog.getInstance().getColumnPlacement( store.id, catalogColumn.id) );
        }

        List<CatalogColumn> selectColumnList = new LinkedList<>( columns );
        CatalogTable table = Catalog.getInstance().getTable( columnPlacements.get( 0 ).tableId );
        CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( table.primaryKey );

        // Add primary keys to select column list
        for ( long cid : primaryKey.columnIds ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( cid );
            if ( !selectColumnList.contains( catalogColumn ) ) {
                selectColumnList.add( catalogColumn );
            }
        }

        for ( long partitionId : partitionIds ) {

            RelRoot sourceRel = getSourceIterator( sourceStatement, selectSourcePlacements( table, selectColumnList, columnPlacements.get( 0 ).adapterId ),partitionId );
            RelRoot targetRel;
            if ( Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( store.id, table.id ).size() == columns.size() ) {
                // There have been no placements for this table on this store before. Build insert statement
                targetRel = buildInsertStatement( targetStatement, columnPlacements, partitionId);
            } else {
                // Build update statement
                targetRel = buildUpdateStatement( targetStatement, columnPlacements, partitionId );
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
                    List<List<Object>> rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( sourceIterator, batchSize ), new ArrayList<>() );
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
    }


    private RelRoot buildInsertStatement( Statement statement, List<CatalogColumnPlacement> to, long partitionId ) {
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        to.get( 0 ).adapterUniqueName,
                        to.get( 0 ).getLogicalSchemaName(),
                        to.get( 0 ).physicalSchemaName,
                        partitionId),
                to.get( 0 ).getLogicalTableName() );
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
                        to.get( 0 ).physicalSchemaName,
                        partitionId),
                to.get( 0 ).getLogicalTableName() );
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


    private RelRoot getSourceIterator( Statement statement, List<CatalogColumnPlacement> placements, long partitionId ) {
        // Get map of placements by adapter
        Map<String, List<CatalogColumnPlacement>> placementsByAdapter = new HashMap<>();
        long tableId = -1;
        for ( CatalogColumnPlacement p : placements ) {
            placementsByAdapter.putIfAbsent( p.getAdapterUniqueName(), new LinkedList<>() );
            placementsByAdapter.get( p.getAdapterUniqueName() ).add( p );

            if ( tableId == -1){
                tableId = p.tableId;
            }
        }

        // Build Query
        RelOptCluster cluster = RelOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );

        Map<Long,List<CatalogColumnPlacement>> distributionPlacements = new HashMap<>();
        distributionPlacements.put( partitionId,placements );

        RelNode node = statement.getRouter().buildJoinedTableScan( statement, cluster, distributionPlacements );
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

}
