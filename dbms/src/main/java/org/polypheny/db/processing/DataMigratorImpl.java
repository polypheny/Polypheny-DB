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
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogStore;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
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
    public void copyData( Transaction transaction, CatalogStore store, List<CatalogColumn> columns ) {

        Statement sourceStatement = transaction.createStatement();
        Statement targetStatement = transaction.createStatement();

        // Check Lists
        List<CatalogColumnPlacement> columnPlacements = new LinkedList<>();
        try {
            for ( CatalogColumn catalogColumn : columns ) {
                columnPlacements.add( Catalog.getInstance().getColumnPlacement( store.id, catalogColumn.id ) );
            }
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        }

        CatalogTable table;
        CatalogPrimaryKey primaryKey;
        List<CatalogColumn> selectColumnList = new LinkedList<>( columns );
        try {
            table = Catalog.getInstance().getTable( columnPlacements.get( 0 ).tableId );
            primaryKey = Catalog.getInstance().getPrimaryKey( table.primaryKey );

            // Add primary keys to select column list
            for ( long cid : primaryKey.columnIds ) {
                CatalogColumn catalogColumn = Catalog.getInstance().getColumn( cid );
                if ( !selectColumnList.contains( catalogColumn ) ) {
                    selectColumnList.add( catalogColumn );
                }
            }
        } catch ( UnknownTableException | GenericCatalogException | UnknownKeyException e ) {
            throw new RuntimeException( e );
        }

        RelRoot sourceRel = getSourceIterator( sourceStatement, selectSourcePlacements( table, selectColumnList, columnPlacements.get( 0 ).storeId ) );
        RelRoot targetRel;
        if ( Catalog.getInstance().getColumnPlacementsOnStore( store.id, table.id ).size() == columns.size() ) {
            // There have been no placements for this table on this store before. Build insert statement
            targetRel = buildInsertStatement( targetStatement, columnPlacements );
        } else {
            // Build update statement
            targetRel = buildUpdateStatement( targetStatement, columnPlacements );
        }

        // Execute Query
        try {
            PolyphenyDbSignature signature = sourceStatement.getQueryProcessor().prepareQuery( sourceRel, sourceRel.rel.getCluster().getTypeFactory().builder().build(), null, true );
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

            while ( sourceIterator.hasNext() ) {
                List<List<Object>> rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( sourceIterator, 100 ), new ArrayList<>() );
                for ( List<Object> list : rows ) {
                    Map<String, Object> values = new HashMap<>();
                    for ( Map.Entry<Long, Integer> entry : resultColMapping.entrySet() ) {
                        values.put( "?" + entry.getKey(), list.get( entry.getValue() ) );
                    }
                    Iterator iterator = targetStatement.getQueryProcessor()
                            .prepareQuery( targetRel, sourceRel.validatedRowType, values, true )
                            .enumerable( targetStatement.getDataContext() )
                            .iterator();
                    //noinspection WhileLoopReplaceableByForEach
                    while ( iterator.hasNext() ) {
                        iterator.next();
                    }
                }
            }
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
    }


    private RelRoot buildInsertStatement( Statement statement, List<CatalogColumnPlacement> to ) {
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildStoreSchemaName(
                        to.get( 0 ).storeUniqueName,
                        to.get( 0 ).getLogicalSchemaName(),
                        to.get( 0 ).physicalSchemaName ),
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


    private RelRoot buildUpdateStatement( Statement statement, List<CatalogColumnPlacement> to ) {
        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildStoreSchemaName(
                        to.get( 0 ).storeUniqueName,
                        to.get( 0 ).getLogicalSchemaName(),
                        to.get( 0 ).physicalSchemaName ),
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
        CatalogTable catalogTable;
        CatalogPrimaryKey primaryKey;
        try {
            catalogTable = Catalog.getInstance().getTable( to.get( 0 ).tableId );
            primaryKey = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey );
        } catch ( GenericCatalogException | UnknownKeyException | UnknownTableException e ) {
            throw new RuntimeException( e );
        }
        try {
            for ( long cid : primaryKey.columnIds ) {
                CatalogColumnPlacement ccp = Catalog.getInstance().getColumnPlacement( to.get( 0 ).storeId, cid );
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
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
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


    private RelRoot getSourceIterator( Statement statement, List<CatalogColumnPlacement> placements ) {
        // Get map of placements by store
        Map<String, List<CatalogColumnPlacement>> placementsByStore = new HashMap<>();
        for ( CatalogColumnPlacement p : placements ) {
            placementsByStore.putIfAbsent( p.getStoreUniqueName(), new LinkedList<>() );
            placementsByStore.get( p.getStoreUniqueName() ).add( p );
        }

        // Build Query
        RelOptCluster cluster = RelOptCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ) );
        RelBuilder builder = RelBuilder.create( statement, cluster );

        statement.getRouter().buildJoinedTableScan( builder, placements );
        return RelRoot.of( builder.build(), SqlKind.SELECT );
    }


    private List<CatalogColumnPlacement> selectSourcePlacements( CatalogTable table, List<CatalogColumn> columns, long excludingStoreId ) {
        // Find the store with the most column placements
        int storeIdWithMostPlacements = -1;
        int numOfPlacements = 0;
        for ( Entry<Integer, ImmutableList<Long>> entry : table.placementsByStore.entrySet() ) {
            if ( entry.getKey() != excludingStoreId && entry.getValue().size() > numOfPlacements ) {
                storeIdWithMostPlacements = entry.getKey();
                numOfPlacements = entry.getValue().size();
            }
        }

        List<Long> columnIds = new LinkedList<>();
        for ( CatalogColumn catalogColumn : columns ) {
            columnIds.add( catalogColumn.id );
        }

        // Take the store with most placements as base and add missing column placements
        List<CatalogColumnPlacement> placementList = new LinkedList<>();
        try {
            for ( long cid : table.columnIds ) {
                if ( columnIds.contains( cid ) ) {
                    if ( table.placementsByStore.get( storeIdWithMostPlacements ).contains( cid ) ) {
                        placementList.add( Catalog.getInstance().getColumnPlacement( storeIdWithMostPlacements, cid ) );
                    } else {
                        for ( CatalogColumnPlacement placement : Catalog.getInstance().getColumnPlacements( cid ) ) {
                            if ( placement.storeId != excludingStoreId ) {
                                placementList.add( placement );
                                break;
                            }
                        }
                    }
                }
            }
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        }

        return placementList;
    }

}
