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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogMaterialized;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.MaterializedCriteria.CriteriaType;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
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
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.DeadlockException;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.transaction.TransactionManager;

@Slf4j
public class MaterializedManagerImpl extends MaterializedManager {

    @Getter
    private final Map<Long, MaterializedCriteria> materializedInfo;

    @Getter
    private final TransactionManager transactionManager;

    Map<PolyXid, Long> potentialInteresting;


    public MaterializedManagerImpl( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
        this.materializedInfo = new HashMap<>();
        this.potentialInteresting = new HashMap<PolyXid, Long>();
        //MaterializedFreshnessLoop materializedFreshnessLoop = new MaterializedFreshnessLoop( this );
        //Thread t = new Thread( materializedFreshnessLoop );
        //t.start();
    }


    public Map<Long, MaterializedCriteria> updateMaterializedViewInfo() {
        List<Long> toRemove = new ArrayList<>();
        for ( Long id : materializedInfo.keySet() ) {
            if ( Catalog.getInstance().getTable( id ) == null ) {
                toRemove.add( id );
            }
        }
        toRemove.forEach( this::deleteMaterializedViewFromInfo );
        return materializedInfo;
    }


    @Override
    public synchronized void deleteMaterializedViewFromInfo( Long tableId ) {
        materializedInfo.remove( tableId );
    }


    public synchronized void updateMaterializedTime( Long materializedId ) {
        if ( materializedInfo.containsKey( materializedId ) ) {
            materializedInfo.get( materializedId ).setLastUpdate( new Timestamp( System.currentTimeMillis() ) );
        }
    }


    public synchronized void updateMaterializedUpdate( Long materializedId, int updates ) {
        materializedInfo.get( materializedId ).setTimesUpdated( updates );
    }


    public synchronized void addMaterializedInfo( Long tableId, MaterializedCriteria matViewCritera ) {
        materializedInfo.put( tableId, matViewCritera );
    }


    @Override
    public void addTables( Transaction transaction, List<String> tableNames ) {
        if ( tableNames.size() > 1 ) {
            try {
                CatalogTable catalogTable = Catalog.getInstance().getTable( 1, tableNames.get( 0 ), tableNames.get( 1 ) );
                long id = catalogTable.id;
                if ( !catalogTable.getConnectedViews().isEmpty() ) {
                    potentialInteresting.put( transaction.getXid(), id );
                }
            } catch ( UnknownTableException e ) {
                throw new RuntimeException( "Not possible to getTable to update which Tables were changed.", e );
            }
        }
    }


    @Override
    public void updateCommitedXid( PolyXid xid ) {
        if ( potentialInteresting.containsKey( xid ) ) {
            materializedUpdate( potentialInteresting.remove( xid ) );
        }

    }


    public void materializedUpdate( Long potentialInteresting ) {
        Catalog catalog = Catalog.getInstance();
        CatalogTable catalogTable = catalog.getTable( potentialInteresting );
        List<Long> connectedViews = catalogTable.getConnectedViews();

        for ( Long id : connectedViews ) {
            CatalogTable view = catalog.getTable( id );
            if ( view.tableType == TableType.MATERIALIZEDVIEW ) {
                MaterializedCriteria materializedCriteria = materializedInfo.get( view.id );
                if ( materializedCriteria.getCriteriaType() == CriteriaType.UPDATE ) {
                    int numberUpdated = materializedCriteria.getTimesUpdated();
                    if ( numberUpdated == (materializedCriteria.getInterval() - 1) ) {
                        prepareToUpdate( view.id );
                        updateMaterializedUpdate( view.id, 0 );
                    } else {
                        updateMaterializedUpdate( view.id, numberUpdated + 1 );
                    }
                }
            }
        }
    }


    @Override
    public void manualUpdate( Transaction transaction, Long viewId ) {
        AdapterManager adapterManager = AdapterManager.getInstance();
        Catalog catalog = Catalog.getInstance();
        CatalogMaterialized catalogMaterialized = (CatalogMaterialized) catalog.getTable( viewId );
        Map<Integer, List<CatalogColumn>> columns = new HashMap<>();

        System.out.println( "Inside MANUAL UPDATE" );
        List<DataStore> dataStores = new ArrayList<>();
        for ( int id : catalogMaterialized.placementsByAdapter.keySet() ) {
            dataStores.add( adapterManager.getStore( id ) );
            List<CatalogColumn> catalogColumns = new ArrayList<>();
            if ( catalogMaterialized.placementsByAdapter.containsKey( id ) ) {

                catalogMaterialized.placementsByAdapter.get( id ).forEach( col ->
                        catalogColumns.add( catalog.getColumn( col ) )
                );
                columns.put( id, catalogColumns );
            }
        }

        updateData( transaction, dataStores, columns, RelRoot.of( catalogMaterialized.getDefinition(), SqlKind.SELECT ), catalogMaterialized );


    }


    public void prepareToUpdate( Long viewId ) {
        AdapterManager adapterManager = AdapterManager.getInstance();
        Catalog catalog = Catalog.getInstance();
        CatalogMaterialized catalogMaterialized = (CatalogMaterialized) catalog.getTable( viewId );
        Map<Integer, List<CatalogColumn>> columns = new HashMap<>();

        System.out.println( "Inside WhileLoop" );
        List<DataStore> dataStores = new ArrayList<>();
        for ( int id : catalogMaterialized.placementsByAdapter.keySet() ) {
            dataStores.add( adapterManager.getStore( id ) );
            List<CatalogColumn> catalogColumns = new ArrayList<>();
            if ( catalogMaterialized.placementsByAdapter.containsKey( id ) ) {

                catalogMaterialized.placementsByAdapter.get( id ).forEach( col ->
                        catalogColumns.add( catalog.getColumn( col ) )
                );
                columns.put( id, catalogColumns );
            }
        }


        String databaseName = catalog.getDatabase( catalogMaterialized.databaseId ).name;
        Transaction transaction;

        try {
            transaction = getTransactionManager().startTransaction( catalogMaterialized.ownerName, databaseName, true, "Materialized View" );

            try {
                // Get a exclusive global schema lock
                LockManager.INSTANCE.lock( LockManager.GLOBAL_LOCK, (TransactionImpl) transaction, LockMode.EXCLUSIVE );
            } catch ( DeadlockException e ) {
                throw new RuntimeException( e );
            }
            updateData( transaction, dataStores, columns, RelRoot.of( catalogMaterialized.getDefinition(), SqlKind.SELECT ), catalogMaterialized );
            commitTransaction( transaction );

        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            e.printStackTrace();
        }
    }


    public void commitTransaction( Transaction transaction ) {

        try {
            //locks are released within commit
            transaction.commit();
        } catch ( TransactionException e ) {
            log.error( "Caught exception while executing a query from the console", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        } finally {
            // Release lock
            LockManager.INSTANCE.unlock( LockManager.GLOBAL_LOCK, (TransactionImpl) transaction );
        }
    }


    @Override
    public void addData( Transaction transaction, List<DataStore> stores, Map<Integer, List<CatalogColumn>> columns, RelRoot sourceRel, CatalogMaterialized materializedView ) {
        addMaterializedInfo( materializedView.id, materializedView.getMaterializedCriteria() );

        Statement sourceStatement = transaction.createStatement();
        List<CatalogColumnPlacement> columnPlacements = new LinkedList<>();
        DataMigrator dataMigrator = transaction.getDataMigrator();

        List<Integer> ids = new ArrayList<>();
        for ( DataStore store : stores ) {
            ids.add( store.getAdapterId() );
        }

        RelCollation relCollation = materializedView.getRelCollation();

        RelOptCluster cluster = RelOptCluster.create(
                sourceStatement.getQueryProcessor().getPlanner(),
                new RexBuilder( sourceStatement.getTransaction().getTypeFactory() ) );

        prepareNode( sourceRel.rel, cluster, relCollation );

        for ( int id : ids ) {
            Statement targetStatement = transaction.createStatement();
            columnPlacements.clear();

            columns.get( id ).forEach( column -> columnPlacements.add( Catalog.getInstance().getColumnPlacement( id, column.id ) ) );

            RelRoot targetRel = dataMigrator.buildInsertStatement( targetStatement, columnPlacements );

            dataMigrator.executeQuery( columns.get( id ), sourceRel, sourceStatement, targetStatement, targetRel, true );
        }

    }


    public void updateData( Transaction transaction, List<DataStore> stores, Map<Integer, List<CatalogColumn>> columns, RelRoot sourceRel, CatalogMaterialized view ) {
        Statement sourceStatement = transaction.createStatement();
        Statement deleteStatement = transaction.createStatement();
        List<CatalogColumnPlacement> columnPlacements = new LinkedList<>();
        DataMigrator dataMigrator = transaction.getDataMigrator();

        List<Integer> ids = new ArrayList<>();
        for ( DataStore store : stores ) {
            ids.add( store.getAdapterId() );
        }

        RelCollation relCollation = view.getRelCollation();

        RelOptCluster cluster = RelOptCluster.create(
                sourceStatement.getQueryProcessor().getPlanner(),
                new RexBuilder( sourceStatement.getTransaction().getTypeFactory() ) );

        prepareNode( sourceRel.rel, cluster, relCollation );

        RelRoot targetRel;

        for ( int id : ids ) {
            columnPlacements.clear();
            Statement targetStatement = transaction.createStatement();

            columns.get( id ).forEach( column -> columnPlacements.add( Catalog.getInstance().getColumnPlacement( id, column.id ) ) );
            RelBuilder relBuilder = RelBuilder.create( deleteStatement );
            RelNode relNode = relBuilder.scan( view.name ).build();

            //delete all data
            targetRel = dataMigrator.buildDeleteStatement( targetStatement, columnPlacements );
            dataMigrator.executeQuery( columns.get( id ), RelRoot.of( relNode, SqlKind.SELECT ), deleteStatement, targetStatement, targetRel, true );

            //RelRoot sourceRel = getSourceRel( view, sourceStatement, dataMigrator );

            //insert new data
            targetRel = dataMigrator.buildInsertStatement( targetStatement, columnPlacements );
            dataMigrator.executeQuery( columns.get( id ), sourceRel, sourceStatement, targetStatement, targetRel, true );

        }

    }


    /*
    private RelRoot getSourceRel( CatalogMaterialized view, Statement sourceStatement, DataMigrator dataMigrator ) {
        List<CatalogColumnPlacement> catalogColumns = new ArrayList<>();
        for ( Long tableId : view.getUnderlyingTables().keySet() ) {
            Catalog catalog = Catalog.getInstance();
            CatalogTable catalogTable = catalog.getTable( tableId );
            for ( int colId : catalogTable.placementsByAdapter.keySet() ) {

                if ( catalogTable.placementsByAdapter.containsKey( colId ) ) {
                    catalogTable.placementsByAdapter.get( colId ).forEach( col ->
                            catalogColumns.add( catalog.getColumnPlacement( AdapterManager.getInstance().getStore( colId ).getAdapterId(), col ) )
                    );
                }
            }
        }
        return dataMigrator.getSourceIterator( sourceStatement, catalogColumns );
    }
*/

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
