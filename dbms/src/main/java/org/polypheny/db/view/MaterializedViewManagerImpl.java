/*
 * Copyright 2019-2022 The Polypheny Project
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.relational.LogicalRelViewScan;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogMaterializedView;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.MaterializedCriteria.CriteriaType;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.processing.DataMigrator;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.EntityAccessMap;
import org.polypheny.db.transaction.EntityAccessMap.EntityIdentifier;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;
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

    final Map<PolyXid, Long> updateCandidates;
    private Snapshot snapshot;


    public MaterializedViewManagerImpl( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
        this.materializedInfo = new ConcurrentHashMap<>();
        this.updateCandidates = new HashMap<>();
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
            if ( Catalog.getInstance().getSnapshot().getLogicalEntity( id ) == null ) {
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
            Catalog.getInstance().getLogicalRel( 0 ).updateMaterializedViewRefreshTime( materializedId );
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
     * If a change is committed to the transactionId and the tableId are saved as potential interesting
     * update candidates for materialized view with freshness updates
     *
     * @param transaction transaction of the commit
     * @param tableIds table that was changed
     */
    @Override
    public void addTables( Transaction transaction, List<Long> tableIds ) {
        if ( tableIds.size() > 1 ) {
            snapshot = Catalog.getInstance().getSnapshot();
            LogicalNamespace namespace = snapshot.getNamespace( tableIds.get( 0 ) );
            LogicalTable catalogTable = snapshot.getRelSnapshot( namespace.id ).getTable( tableIds.get( 1 ) );
            long id = catalogTable.id;
            if ( !catalogTable.getConnectedViews().isEmpty() ) {
                updateCandidates.put( transaction.getXid(), id );
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
        if ( updateCandidates.containsKey( xid ) ) {
            materializedUpdate( updateCandidates.remove( xid ) );
        }
    }


    /**
     * Checks if materialized view  with freshness update needs to be updated after a change on the underlying table
     *
     * @param potentialInteresting id of underlying table that was updated
     */
    public void materializedUpdate( Long potentialInteresting ) {
        Snapshot snapshot = Catalog.getInstance().getSnapshot();
        LogicalTable catalogTable = snapshot.getNamespaces( null ).stream().map( n -> snapshot.getRelSnapshot( n.id ).getTable( potentialInteresting ) ).filter( Objects::nonNull ).findFirst().orElse( null );
        List<Long> connectedViews = catalogTable.getConnectedViews();

        for ( long id : connectedViews ) {
            LogicalTable view = snapshot.getRelSnapshot( catalogTable.namespaceId ).getTable( id );
            if ( view.entityType == EntityType.MATERIALIZED_VIEW ) {
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
        LogicalTable catalogTable = catalog.getSnapshot().getLogicalEntity( materializedId ).unwrap( LogicalTable.class );

        try {
            Transaction transaction = getTransactionManager().startTransaction(
                    Catalog.defaultUserId,
                    false,
                    "Materialized View" );

            try {
                Statement statement = transaction.createStatement();
                Collection<Entry<EntityIdentifier, LockMode>> idAccessMap = new ArrayList<>();
                // Get a shared global schema lock (only DDLs acquire an exclusive global schema lock)
                idAccessMap.add( Pair.of( LockManager.GLOBAL_LOCK, LockMode.SHARED ) );
                // Get locks for individual tables
                EntityAccessMap accessMap = new EntityAccessMap( ((CatalogMaterializedView) catalogTable).getDefinition(), new HashMap<>() );
                idAccessMap.addAll( accessMap.getAccessedEntityPair() );
                LockManager.INSTANCE.lock( idAccessMap, (TransactionImpl) statement.getTransaction() );
            } catch ( DeadlockException e ) {
                throw new RuntimeException( "DeadLock while locking for materialized view update", e );
            }
            updateData( transaction, materializedId );
            commitTransaction( transaction );
        } catch ( GenericCatalogException | UnknownUserException | UnknownSchemaException e ) {
            throw new RuntimeException( "Not possible to create Transaction for Materialized View update", e );
        }
        updateMaterializedTime( materializedId );
    }


    /**
     * Is used if a materialized view is created in order to add the data from the underlying tables to the materialized view
     */
    @Override
    public void addData( Transaction transaction, List<DataStore> stores, Map<Long, List<LogicalColumn>> columns, AlgRoot algRoot, CatalogMaterializedView materializedView ) {
        addMaterializedInfo( materializedView.id, materializedView.getMaterializedCriteria() );

        List<CatalogColumnPlacement> columnPlacements = new LinkedList<>();
        DataMigrator dataMigrator = transaction.getDataMigrator();
        List<CatalogDataPlacement> dataPlacements = transaction.getSnapshot().getAllocSnapshot().getDataPlacements( materializedView.id );
        for ( CatalogDataPlacement placement : dataPlacements ) {
            Statement sourceStatement = transaction.createStatement();
            prepareSourceRel( sourceStatement, materializedView.getAlgCollation(), algRoot.alg );
            Statement targetStatement = transaction.createStatement();
            columnPlacements.clear();

            columns.get( placement.adapterId ).forEach( column -> columnPlacements.add( snapshot.getAllocSnapshot().getColumnPlacement( placement.adapterId, column.id ) ) );
            // If partitions should be allowed for materialized views this needs to be changed that all partitions are considered
            AlgRoot targetRel = dataMigrator.buildInsertStatement( targetStatement, columnPlacements, snapshot.getAllocSnapshot().getPartitionsOnDataPlacement( placement.adapterId, materializedView.id ).get( 0 ) );

            dataMigrator.executeQuery( columns.get( placement.adapterId ), algRoot, sourceStatement, targetStatement, targetRel, true, materializedView.isOrdered() );
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

        DataMigrator dataMigrator = transaction.getDataMigrator();

        List<CatalogColumnPlacement> columnPlacements = new LinkedList<>();
        Map<Long, List<LogicalColumn>> columns = new HashMap<>();

        List<Long> ids = new ArrayList<>();
        if ( snapshot.getLogicalEntity( materializedId ) != null && materializedInfo.containsKey( materializedId ) ) {
            CatalogMaterializedView catalogMaterializedView = snapshot.getLogicalEntity( materializedId ).unwrap( CatalogMaterializedView.class );
            List<CatalogDataPlacement> dataPlacements = snapshot.getAllocSnapshot().getDataPlacements( catalogMaterializedView.id );
            for ( CatalogDataPlacement placement : dataPlacements ) {
                ids.add( placement.adapterId );
                List<LogicalColumn> logicalColumns = new ArrayList<>();

                int localAdapterIndex = dataPlacements.indexOf( placement );
                snapshot.getAllocSnapshot().getDataPlacement( dataPlacements.stream().map( p -> p.adapterId ).collect( Collectors.toList() ).get( localAdapterIndex ), catalogMaterializedView.id )
                        .columnPlacementsOnAdapter.forEach( col ->
                                logicalColumns.add( snapshot.getRelSnapshot( catalogMaterializedView.namespaceId ).getColumn( col ) ) );
                columns.put( placement.adapterId, logicalColumns );
            }

            AlgRoot targetRel;

            for ( long id : ids ) {
                Statement sourceStatement = transaction.createStatement();
                Statement deleteStatement = transaction.createStatement();
                Statement insertStatement = transaction.createStatement();
                prepareSourceRel( sourceStatement, catalogMaterializedView.getAlgCollation(), catalogMaterializedView.getDefinition() );

                columnPlacements.clear();

                columns.get( id ).forEach( column -> columnPlacements.add( snapshot.getAllocSnapshot().getColumnPlacement( id, column.id ) ) );

                // Build {@link AlgNode} to build delete Statement from materialized view
                AlgBuilder deleteAlgBuilder = AlgBuilder.create( deleteStatement );
                AlgNode deleteRel = deleteAlgBuilder.scan( catalogMaterializedView.name ).build();

                // Build {@link AlgNode} to build insert Statement from materialized view
                AlgBuilder insertAlgBuilder = AlgBuilder.create( insertStatement );
                AlgNode insertRel = insertAlgBuilder.push( catalogMaterializedView.getDefinition() ).build();

                Statement targetStatementDelete = transaction.createStatement();
                // Delete all data
                targetRel = dataMigrator.buildDeleteStatement(
                        targetStatementDelete,
                        columnPlacements,
                        snapshot.getAllocSnapshot().getPartitionsOnDataPlacement( id, catalogMaterializedView.id ).get( 0 ) );
                dataMigrator.executeQuery(
                        columns.get( id ),
                        AlgRoot.of( deleteRel, Kind.SELECT ),
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
                        snapshot.getAllocSnapshot().getPartitionsOnDataPlacement( id, catalogMaterializedView.id ).get( 0 ) );
                dataMigrator.executeQuery(
                        columns.get( id ),
                        AlgRoot.of( insertRel, Kind.SELECT ),
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
            LockManager.INSTANCE.unlock( Collections.singletonList( LockManager.GLOBAL_LOCK ), (TransactionImpl) transaction );
        }
    }


    private void prepareSourceRel( Statement sourceStatement, AlgCollation algCollation, AlgNode sourceRel ) {
        AlgOptCluster cluster = AlgOptCluster.create(
                sourceStatement.getQueryProcessor().getPlanner(),
                new RexBuilder( sourceStatement.getTransaction().getTypeFactory() ), null, sourceStatement.getDataContext().getSnapshot() );

        prepareNode( sourceRel, cluster, algCollation );
    }


    public void prepareNode( AlgNode viewLogicalRoot, AlgOptCluster algOptCluster, AlgCollation algCollation ) {
        if ( viewLogicalRoot instanceof AbstractAlgNode ) {
            ((AbstractAlgNode) viewLogicalRoot).setCluster( algOptCluster );

            List<AlgCollation> algCollations = new ArrayList<>();
            algCollations.add( algCollation );
            AlgTraitSet traitSetTest =
                    algOptCluster.traitSetOf( Convention.NONE )
                            .replaceIfs(
                                    AlgCollationTraitDef.INSTANCE,
                                    () -> {
                                        if ( algCollation != null ) {
                                            return algCollations;
                                        }
                                        return ImmutableList.of();
                                    } );

            ((AbstractAlgNode) viewLogicalRoot).setTraitSet( traitSetTest );
        }
        if ( viewLogicalRoot instanceof BiAlg ) {
            prepareNode( ((BiAlg) viewLogicalRoot).getLeft(), algOptCluster, algCollation );
            prepareNode( ((BiAlg) viewLogicalRoot).getRight(), algOptCluster, algCollation );
        } else if ( viewLogicalRoot instanceof SingleAlg ) {
            prepareNode( ((SingleAlg) viewLogicalRoot).getInput(), algOptCluster, algCollation );
        }
        if ( viewLogicalRoot instanceof LogicalRelViewScan ) {
            prepareNode( ((LogicalRelViewScan) viewLogicalRoot).getAlgNode(), algOptCluster, algCollation );
        }
    }

}
