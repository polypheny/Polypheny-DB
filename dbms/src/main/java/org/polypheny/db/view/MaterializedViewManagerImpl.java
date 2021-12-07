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

package org.polypheny.db.view;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogMaterializedView;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.MaterializedCriteria.CriteriaType;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
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
import org.polypheny.db.rel.logical.LogicalViewScan;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.DeadlockException;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TableAccessMap;
import org.polypheny.db.transaction.TableAccessMap.Mode;
import org.polypheny.db.transaction.TableAccessMap.TableIdentifier;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.background.BackgroundTask.TaskPriority;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;
import org.polypheny.db.util.background.BackgroundTaskManager;


@Slf4j
public class MaterializedViewManagerImpl extends MaterializedViewManager {

    @Getter
    private final Map<Long, MaterializedCriteria> materializedInfo;

    @Getter
    private final TransactionManager transactionManager;

    @Getter
    private final List<Long> intervalToUpdate;

    final Map<PolyXid, Long> potentialInteresting;


    public MaterializedViewManagerImpl( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
        this.materializedInfo = new ConcurrentHashMap<>();
        this.potentialInteresting = new HashMap<>();
        this.intervalToUpdate = Collections.synchronizedList( new ArrayList<>() );
        registerFreshnessLoop();
    }


    /**
     * Updates the materialized Info
     *
     * @return updated materializedInfo
     */
    public synchronized Map<Long, MaterializedCriteria> updateMaterializedViewInfo() {
        List<Long> toRemove = new ArrayList<>();
        for ( Long id : materializedInfo.keySet() ) {
            if ( Catalog.getInstance().getTable( id ) == null ) {
                toRemove.add( id );
            }
        }
        toRemove.forEach( this::deleteMaterializedViewFromInfo );
        return materializedInfo;
    }


    /**
     * Removes a materialized view from the materializedInfo
     *
     * @param materializedId id from materialized view
     */
    @Override
    public synchronized void deleteMaterializedViewFromInfo( Long materializedId ) {
        materializedInfo.remove( materializedId );
    }


    /**
     * Updates the materializedInfo and the CatalogMaterialized with the time of the last update
     *
     * @param materializedId id from materialized view
     */
    @Override
    public synchronized void updateMaterializedTime( Long materializedId ) {
        if ( materializedInfo.containsKey( materializedId ) ) {
            materializedInfo.get( materializedId ).setLastUpdate( new Timestamp( System.currentTimeMillis() ) );
            Catalog.getInstance().updateMaterializedViewRefreshTime( materializedId );
        }
    }


    /**
     * Is used by materialized views with freshness update.
     * Updates how many updates were carried out on the underlying tables
     *
     * @param materializedId id from materialized view
     * @param updates number of updates
     */
    public synchronized void updateMaterializedUpdate( Long materializedId, int updates ) {
        materializedInfo.get( materializedId ).setTimesUpdated( updates );
    }


    /**
     * Add materialized view to materializedInfo
     *
     * @param materializedId id from materialized view
     * @param matViewCriteria information about the materialized view
     */
    @Override
    public synchronized void addMaterializedInfo( Long materializedId, MaterializedCriteria matViewCriteria ) {
        materializedInfo.put( materializedId, matViewCriteria );
    }


    /**
     * If a change is committed to the transactionId and the tableId are saved as potential interesting for
     * materialized view with freshness update
     *
     * @param transaction transaction of the commit
     * @param tableNames table that was changed
     */
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


    /**
     * If a transaction is committed, it checks if it is connected to a materialized view
     * with freshness update, if it is the materialized view is updated
     *
     * @param xid of committed transaction
     */
    @Override
    public void updateCommittedXid( PolyXid xid ) {
        if ( potentialInteresting.containsKey( xid ) ) {
            materializedUpdate( potentialInteresting.remove( xid ) );
        }
    }


    /**
     * Checks if materialized view  with freshness update needs to be updated after a change on the underlying table
     *
     * @param potentialInteresting id of underlying table that was updated
     */
    public void materializedUpdate( Long potentialInteresting ) {
        Catalog catalog = Catalog.getInstance();
        CatalogTable catalogTable = catalog.getTable( potentialInteresting );
        List<Long> connectedViews = catalogTable.getConnectedViews();

        for ( Long id : connectedViews ) {
            CatalogTable view = catalog.getTable( id );
            if ( view.tableType == TableType.MATERIALIZED_VIEW ) {
                MaterializedCriteria materializedCriteria = materializedInfo.get( view.id );
                if ( materializedCriteria.getCriteriaType() == CriteriaType.UPDATE ) {
                    int numberUpdated = materializedCriteria.getTimesUpdated();
                    if ( numberUpdated == (materializedCriteria.getInterval() - 1) ) {
                        isUpdatingMaterialized = true;
                        prepareToUpdate( view.id );
                        updateMaterializedUpdate( view.id, 0 );
                        isUpdatingMaterialized = false;
                    } else {
                        updateMaterializedUpdate( view.id, numberUpdated + 1 );
                    }
                }
            }
        }
    }


    /**
     * Register the freshnessLoop as BackgroundTask to update Materialized Views
     */
    private void registerFreshnessLoop() {
        BackgroundTaskManager.INSTANCE.registerTask(
                MaterializedViewManagerImpl.this::updatingIntervalMaterialized,
                "Update materialized views with freshness type interval if required",
                TaskPriority.MEDIUM,
                (TaskSchedulingType) RuntimeConfig.MATERIALIZED_VIEW_LOOP.getEnum() );
    }


    /**
     * Update Materialized Views with freshness type interval if it is time to update them
     */
    private void updatingIntervalMaterialized() {
        Map<Long, MaterializedCriteria> materializedViewInfo;
        materializedViewInfo = ImmutableMap.copyOf( updateMaterializedViewInfo() );
        materializedViewInfo.forEach( ( k, v ) -> {
            if ( v.getCriteriaType() == CriteriaType.INTERVAL ) {
                if ( v.getLastUpdate().getTime() + v.getTimeInMillis() < System.currentTimeMillis() ) {
                    if ( !isDroppingMaterialized && !isCreatingMaterialized && !isUpdatingMaterialized ) {
                        prepareToUpdate( k );
                        updateMaterializedTime( k );
                    }
                }
            }
        } );
    }


    /**
     * Starts transition and acquires a global schema lock before the materialized view is updated
     *
     * @param materializedId of materialized view, which is updated
     */
    public void prepareToUpdate( Long materializedId ) {
        Catalog catalog = Catalog.getInstance();
        CatalogTable catalogTable = catalog.getTable( materializedId );

        try {
            Transaction transaction = getTransactionManager().startTransaction(
                    catalogTable.ownerName,
                    catalog.getDatabase( catalogTable.databaseId ).name,
                    false,
                    "Materialized View" );

            try {
                Statement statement = transaction.createStatement();
                // Get a shared global schema lock (only DDLs acquire a exclusive global schema lock)
                LockManager.INSTANCE.lock( LockManager.GLOBAL_LOCK, (TransactionImpl) statement.getTransaction(), LockMode.SHARED );
                // Get locks for individual tables
                TableAccessMap accessMap = new TableAccessMap( ((CatalogMaterializedView) catalogTable).getDefinition() );
                for ( TableIdentifier tableIdentifier : accessMap.getTablesAccessed() ) {
                    Mode mode = accessMap.getTableAccessMode( tableIdentifier );
                    if ( mode == Mode.READ_ACCESS ) {
                        LockManager.INSTANCE.lock( tableIdentifier, (TransactionImpl) statement.getTransaction(), LockMode.SHARED );
                    } else if ( mode == Mode.WRITE_ACCESS || mode == Mode.READWRITE_ACCESS ) {
                        LockManager.INSTANCE.lock( tableIdentifier, (TransactionImpl) statement.getTransaction(), LockMode.EXCLUSIVE );
                    }
                }
            } catch ( DeadlockException e ) {
                throw new RuntimeException( "DeadLock while locking for materialized view update", e );
            }
            updateData( transaction, materializedId );
            commitTransaction( transaction );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Not possible to create Transaction for Materialized View update", e );
        }
        updateMaterializedTime( materializedId );
    }


    /**
     * Is used if a materialized view is created in order to add the data from the underlying tables to the materialized view
     */
    @Override
    public void addData( Transaction transaction, List<DataStore> stores, Map<Integer, List<CatalogColumn>> columns, RelRoot sourceRel, CatalogMaterializedView materializedView ) {
        addMaterializedInfo( materializedView.id, materializedView.getMaterializedCriteria() );

        List<CatalogColumnPlacement> columnPlacements = new LinkedList<>();
        DataMigrator dataMigrator = transaction.getDataMigrator();

        for ( int id : materializedView.placementsByAdapter.keySet() ) {
            Statement sourceStatement = transaction.createStatement();
            prepareSourceRel( sourceStatement, materializedView.getRelCollation(), sourceRel.rel );
            Statement targetStatement = transaction.createStatement();
            columnPlacements.clear();

            columns.get( id ).forEach( column -> columnPlacements.add( Catalog.getInstance().getColumnPlacement( id, column.id ) ) );
            // If partitions should be allowed for materialized views this needs to be changed that all partitions are considered
            RelRoot targetRel = dataMigrator.buildInsertStatement( targetStatement, columnPlacements, Catalog.getInstance().getPartitionsOnDataPlacement( id, materializedView.id ).get( 0 ) );

            dataMigrator.executeQuery( columns.get( id ), sourceRel, sourceStatement, targetStatement, targetRel, true, materializedView.isOrdered() );
        }
    }


    /**
     * Deletes all the data from a materialized view and adds the newest data to the materialized view
     *
     * @param transaction that is used
     * @param materializedId id from materialized view
     */
    @Override
    public void updateData( Transaction transaction, Long materializedId ) {
        Catalog catalog = Catalog.getInstance();

        DataMigrator dataMigrator = transaction.getDataMigrator();

        List<CatalogColumnPlacement> columnPlacements = new LinkedList<>();
        Map<Integer, List<CatalogColumn>> columns = new HashMap<>();

        List<Integer> ids = new ArrayList<>();
        if ( catalog.checkIfExistsTable( materializedId ) && materializedInfo.containsKey( materializedId ) ) {
            CatalogMaterializedView catalogMaterializedView = (CatalogMaterializedView) catalog.getTable( materializedId );
            for ( int id : catalogMaterializedView.placementsByAdapter.keySet() ) {
                ids.add( id );
                List<CatalogColumn> catalogColumns = new ArrayList<>();
                if ( catalogMaterializedView.placementsByAdapter.containsKey( id ) ) {
                    catalogMaterializedView.placementsByAdapter.get( id ).forEach( col ->
                            catalogColumns.add( catalog.getColumn( col ) )
                    );
                    columns.put( id, catalogColumns );
                }
            }

            RelRoot targetRel;

            for ( int id : ids ) {
                Statement sourceStatement = transaction.createStatement();
                Statement deleteStatement = transaction.createStatement();
                Statement insertStatement = transaction.createStatement();
                prepareSourceRel( sourceStatement, catalogMaterializedView.getRelCollation(), catalogMaterializedView.getDefinition() );

                columnPlacements.clear();

                columns.get( id ).forEach( column -> columnPlacements.add( Catalog.getInstance().getColumnPlacement( id, column.id ) ) );

                // Build RelNode to build delete Statement from materialized view
                RelBuilder deleteRelBuilder = RelBuilder.create( deleteStatement );
                RelNode deleteRel = deleteRelBuilder.scan( catalogMaterializedView.name ).build();

                // Build RelNode to build insert Statement from materialized view
                RelBuilder insertRelBuilder = RelBuilder.create( insertStatement );
                RelNode insertRel = insertRelBuilder.push( catalogMaterializedView.getDefinition() ).build();

                Statement targetStatementDelete = transaction.createStatement();
                // Delete all data
                targetRel = dataMigrator.buildDeleteStatement(
                        targetStatementDelete,
                        columnPlacements,
                        Catalog.getInstance().getPartitionsOnDataPlacement( id, catalogMaterializedView.id ).get( 0 ) );
                dataMigrator.executeQuery(
                        columns.get( id ),
                        RelRoot.of( deleteRel, SqlKind.SELECT ),
                        deleteStatement,
                        targetStatementDelete,
                        targetRel,
                        true,
                        catalogMaterializedView.isOrdered() );

                Statement targetStatementInsert = transaction.createStatement();

                // Insert new data
                targetRel = dataMigrator.buildInsertStatement(
                        targetStatementInsert,
                        columnPlacements,
                        Catalog.getInstance().getPartitionsOnDataPlacement( id, catalogMaterializedView.id ).get( 0 ) );
                dataMigrator.executeQuery(
                        columns.get( id ),
                        RelRoot.of( insertRel, SqlKind.SELECT ),
                        sourceStatement,
                        targetStatementInsert,
                        targetRel,
                        true,
                        catalogMaterializedView.isOrdered() );
            }
        }
    }


    public void commitTransaction( Transaction transaction ) {
        try {
            // Locks are released within commit
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


    private void prepareSourceRel( Statement sourceStatement, RelCollation relCollation, RelNode sourceRel ) {
        RelOptCluster cluster = RelOptCluster.create(
                sourceStatement.getQueryProcessor().getPlanner(),
                new RexBuilder( sourceStatement.getTransaction().getTypeFactory() ) );

        prepareNode( sourceRel, cluster, relCollation );
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
        if ( viewLogicalRoot instanceof LogicalViewScan ) {
            prepareNode( ((LogicalViewScan) viewLogicalRoot).getRelNode(), relOptCluster, relCollation );
        }
    }

}
