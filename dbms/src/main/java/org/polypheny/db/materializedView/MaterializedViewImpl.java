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

package org.polypheny.db.materializedView;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.BiRel;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollationTraitDef;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rel.logical.LogicalViewTableScan;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.LimitIterator;

public class MaterializedViewImpl implements MaterializedViewManager {

    public MaterializedViewImpl() {

    }


    public void insertData( Transaction transaction, List<DataStore> stores, List<CatalogColumn> columns, RelRoot sourceRel ) {

        Statement sourceStatement = transaction.createStatement();
        Statement targetStatement = transaction.createStatement();
        List<CatalogColumnPlacement> columnPlacements = new LinkedList<>();

        List<Integer> ids = new ArrayList<>();
        for ( DataStore store : stores ) {
            ids.add( store.getAdapterId() );
        }

        //TODO IG: handle if you have more than one id
        for ( int id : ids ) {
            for ( CatalogColumn catalogColumn : columns ) {
                columnPlacements.add( Catalog.getInstance().getColumnPlacement( id, catalogColumn.id ) );
            }
        }

        RelOptCluster cluster = RelOptCluster.create(
                sourceStatement.getQueryProcessor().getPlanner(),
                new RexBuilder( sourceStatement.getTransaction().getTypeFactory() ) );

        prepareNode( sourceRel.rel, cluster, null );

        RelRoot targetRel = buildInsertStatement( targetStatement, columnPlacements );

        // Execute Query
        try {
            PolyphenyDbSignature signature = sourceStatement.getQueryProcessor().prepareQuery( sourceRel, sourceRel.validatedRowType, false );
            final Enumerable enumerable = signature.enumerable( sourceStatement.getDataContext() );
            //noinspection unchecked
            Iterator<Object> sourceIterator = enumerable.iterator();

            Map<Long, Integer> resultColMapping = new HashMap<>();
            for ( CatalogColumn catalogColumn : columns ) {
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


    public void prepareNode( RelNode viewLogicalRoot, RelOptCluster relOptCluster, RelCollation relCollation ) {
        if ( viewLogicalRoot instanceof AbstractRelNode ) {
            ((AbstractRelNode) viewLogicalRoot).setCluster( relOptCluster );

            List<RelCollation> relCollationList = new ArrayList<>();
            relCollationList.add( relCollation );
            RelTraitSet traitSetTest =
                    relOptCluster.traitSetOf( Convention.NONE )
                            .replaceIfs( RelCollationTraitDef.INSTANCE,
                                    () -> {
                                        if ( relCollation != null ) {
                                            return relCollationList;
                                        }
                                        return ImmutableList.of();
                                    } );

            ((AbstractRelNode) viewLogicalRoot).setTraitSet( traitSetTest );
        }
        if ( viewLogicalRoot instanceof BiRel ) {
            prepareNode( ((BiRel) viewLogicalRoot).getLeft(), relOptCluster, relCollation );
            prepareNode( ((BiRel) viewLogicalRoot).getRight(), relOptCluster, relCollation );
        } else if ( viewLogicalRoot instanceof SingleRel ) {
            prepareNode( ((SingleRel) viewLogicalRoot).getInput(), relOptCluster, relCollation );
        }
        if ( viewLogicalRoot instanceof LogicalViewTableScan ) {
            prepareNode( ((LogicalViewTableScan) viewLogicalRoot).getRelNode(), relOptCluster, relCollation );
        }
    }


    private RelRoot buildInsertStatement( Statement statement, List<CatalogColumnPlacement> to ) {

        List<String> qualifiedTableName = ImmutableList.of(
                PolySchemaBuilder.buildAdapterSchemaName(
                        to.get( 0 ).adapterUniqueName,
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


}
