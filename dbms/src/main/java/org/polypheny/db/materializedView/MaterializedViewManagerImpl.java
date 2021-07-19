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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.MaterializedViewCriteria;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.processing.DataMigrator;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.BiRel;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollationTraitDef;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.logical.LogicalViewTableScan;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;

@Slf4j
public class MaterializedViewManagerImpl extends MaterializedViewManager {

    @Getter
    private final Map<Long, MaterializedViewCriteria> materializedViewInfo;

    @Getter
    private final TransactionManager transactionManager;


    public MaterializedViewManagerImpl( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
        this.materializedViewInfo = new HashMap<>();
        MaterializedFreshnessLoop materializedFreshnessLoop = new MaterializedFreshnessLoop( this );
        Thread t = new Thread( materializedFreshnessLoop );
        t.start();
    }


    public Map<Long, MaterializedViewCriteria> updateMaterializedViewInfo() {
        List<Long> toRemove = new ArrayList<>();
        for ( Long id : materializedViewInfo.keySet() ) {
            if ( Catalog.getInstance().getTable( id ) == null ) {
                toRemove.add( id );
            }
        }
        toRemove.forEach( materializedViewInfo::remove );
        return materializedViewInfo;
    }


    @Override
    public void deleteMaterializedViewFromInfo( Long tableId ) {
        materializedViewInfo.remove( tableId );
    }


    @Override
    public void addData( Transaction transaction, List<DataStore> stores, List<CatalogColumn> columns, RelRoot sourceRel, long tableId, MaterializedViewCriteria materializedViewCriteria ) {
        materializedViewInfo.put( tableId, materializedViewCriteria );

        Statement sourceStatement = transaction.createStatement();
        Statement targetStatement = transaction.createStatement();
        List<CatalogColumnPlacement> columnPlacements = new LinkedList<>();
        DataMigrator dataMigrator = transaction.getDataMigrator();

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

        RelRoot targetRel = dataMigrator.buildInsertStatement( targetStatement, columnPlacements );

        dataMigrator.executeQuery( columns, sourceRel, sourceStatement, targetStatement, targetRel, true );

    }


    @Override
    public void updateData( Transaction transaction, List<DataStore> stores, List<CatalogColumn> columns, RelRoot sourceRel, RelCollation relCollation ) {
        Statement sourceStatement = transaction.createStatement();
        Statement targetStatement = transaction.createStatement();
        List<CatalogColumnPlacement> columnPlacements = new LinkedList<>();
        DataMigrator dataMigrator = transaction.getDataMigrator();

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

        prepareNode( sourceRel.rel, cluster, relCollation );

        RelRoot targetRel;

        //delete all data
        targetRel = dataMigrator.buildDeleteStatement( targetStatement, columnPlacements );
        dataMigrator.executeQuery( columns, sourceRel, sourceStatement, targetStatement, targetRel, true );

        //insert new data
        targetRel = dataMigrator.buildInsertStatement( targetStatement, columnPlacements );
        dataMigrator.executeQuery( columns, sourceRel, sourceStatement, targetStatement, targetRel, true );

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


}
