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

package org.polypheny.db.replication;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;
import org.polypheny.db.catalog.Catalog.PlacementState;
import org.polypheny.db.catalog.Catalog.ReplicationStrategy;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigBoolean;
import org.polypheny.db.config.ConfigInteger;
import org.polypheny.db.config.WebUiGroup;
import org.polypheny.db.replication.cdc.ChangeDataReplicationObject;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.transaction.TransactionManagerImpl;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.background.PausableThreadPoolExecutor;


@Slf4j
public class LazyReplicationEngine extends ReplicationEngine {


    public static final ConfigBoolean AUTOMATIC_DATA_REPLICATION = new ConfigBoolean(
            "replication/automaticLazyDataReplication",
            "Enables automatic lazy replication of data. "
                    + "If this is disabled the modified data is still captured and queued but not actively replicated. "
                    + "And need manual. Care that this can increase the memory footprint due to excessive bookkeeping.",
            true );

    public static final ConfigInteger REPLICATION_CORE_POOL_SIZE = new ConfigInteger(
            "replication/lazyReplicationCorePoolSize",
            "The number of threads to keep in the pool for processing data replication events, even if they are idle.",
            1 );

    public static final ConfigInteger REPLICATION_MAXIMUM_POOL_SIZE = new ConfigInteger(
            "replication/lazyReplicationMaximumPoolSize",
            "The maximum number of threads to allow in the pool used for processing data replication events.",
            4 );

    public static final ConfigInteger REPLICATION_POOL_KEEP_ALIVE_TIME = new ConfigInteger(
            "replication/lazyReplicationKeepAliveTime",
            "When the number of replication worker threads is greater than the core, this is the maximum time that excess idle threads will wait for new tasks before terminating.",
            10 );


    private static LazyReplicationEngine INSTANCE;

    private TransactionManager transactionManager = TransactionManagerImpl.getInstance();

    // Only needed to track changes on uniquely identifiable replicationData

    // TODO @HENNLO add monitoring page that lists number of pending updates per table and per partition

    // TODO @HENNLO as soon as policies are active configure db, schema or db if changes should be replicated by single operation or only ACID compliant with entire transaction
    // DISTRIBUTION STRATEGY


    // TODO @HENNLO if statement is cached also replicationData needs to be cached disregarding the target data placements since new statements can come into


    // Maps replicationDataId to the relevant captured Data that it only will be held in memory once
    // Will be cleaned out of memory once no replication depends on this object anymore
    private HashMap<Long, ChangeDataReplicationObject> replicationData = new HashMap<>();     // replicationDataIds -> replicationData


    // Maps Adapter-Partition (PartitionPlacement) to a queue. It's necessary that each partitionPlacement has its own queue
    // To decouple changes between placements
    // Otherwise one failing placement in the queue, would block the entire replication
    // Contains pending updates per partition Placement
    private HashMap<Pair, List<Long>> localPartitionPlacementQueue = new HashMap<>();    // (Partition-Adapter) -> List<replicationDataIds>

    // Needs a cyclic list iterator
    private final BlockingQueue globalReplicationDataQueue;

    private PausableThreadPoolExecutor threadPoolWorkers;
    private final int CORE_POOL_SIZE;
    private final int MAXIMUM_POOL_SIZE;
    private final int KEEP_ALIVE_TIME;


    public LazyReplicationEngine() {

        this.CORE_POOL_SIZE = REPLICATION_CORE_POOL_SIZE.getInt();
        this.MAXIMUM_POOL_SIZE = REPLICATION_MAXIMUM_POOL_SIZE.getInt();
        this.KEEP_ALIVE_TIME = REPLICATION_POOL_KEEP_ALIVE_TIME.getInt();

        this.globalReplicationDataQueue = new LinkedBlockingQueue();
        this.dataReplicator = new DataReplicatorImpl();

        initializeConfiguration();

        threadPoolWorkers = new PausableThreadPoolExecutor( CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, KEEP_ALIVE_TIME, TimeUnit.SECONDS, globalReplicationDataQueue );
    }


    @Override
    protected ReplicationStrategy getAssociatedReplicationStrategy() {
        return ReplicationStrategy.LAZY;
    }


    private void initializeConfiguration() {

        final WebUiGroup lazyReplicationSettingsGroup = new WebUiGroup( "lazyReplicationSettingsGroup", replicationSettingsPage.getId() );

        configManager.registerConfig( AUTOMATIC_DATA_REPLICATION );
        AUTOMATIC_DATA_REPLICATION.withUi( lazyReplicationSettingsGroup.getId(), 0 );
        AUTOMATIC_DATA_REPLICATION.addObserver( new LazyReplicationConfigListener() );

        configManager.registerConfig( REPLICATION_CORE_POOL_SIZE );
        REPLICATION_CORE_POOL_SIZE.withUi( lazyReplicationSettingsGroup.getId(), 1 );

        configManager.registerConfig( REPLICATION_MAXIMUM_POOL_SIZE );
        REPLICATION_MAXIMUM_POOL_SIZE.withUi( lazyReplicationSettingsGroup.getId(), 2 );

        configManager.registerConfig( REPLICATION_POOL_KEEP_ALIVE_TIME );
        REPLICATION_POOL_KEEP_ALIVE_TIME.withUi( lazyReplicationSettingsGroup.getId(), 3 );

        lazyReplicationSettingsGroup.withTitle( "Pending Data Replication Processing" );
        configManager.registerWebUiGroup( lazyReplicationSettingsGroup );
    }


    public static synchronized ReplicationEngine getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new LazyReplicationEngine();
        }

        return INSTANCE;
    }

    // TODO @HENNLO Check if partitionPlacement is marked as INFINITELY OUTDATED
    //  If so then dont apply all pending changes. Instead remove them and apply DATA Migration


    @Override
    public void replicateChanges() {
        System.out.println( this + " is now replicating changes " );
    }


    /**
     * Is used to manually trigger the replication of all pending updates of each placement for a specific table .
     *
     * @param tableId table to refresh
     */
    @Override
    public void replicateChanges( long tableId ) {
        throw new NotImplementedException( "This operation is not yet implemented" );
    }


    /**
     * Is used to manually trigger the replication of all pending updates of a specific data placement.
     *
     * @param tableId table to refresh
     * @param adapterId adapter to refresh
     */
    @Override
    public void replicateChanges( long tableId, int adapterId ) {
        throw new NotImplementedException( "This operation is not yet implemented" );
    }


    /**
     * Is used to manually trigger the replication of all pending updates on an entire adapter.
     * For example when the adapter was down.
     *
     * This should not be possible to be directly executed by users. Rather with automatic system maintenance.
     *
     * @param adapterId adapter to refresh
     */
    @Override
    public void replicateChanges( int adapterId ) {
        throw new NotImplementedException( "This operation is not yet implemented for this replication engine." );
    }


    /**
     * Places all replication Objects that originated within one Transaction to the replication engine.
     * The list order is equal to the modification order of each object. It is up to each specific Replication Engine the to distribute the changes
     *
     * LazyReplicationEngine distributes the captured changes operation by operation in correct serializable order.
     * Therefore, no Transaction enforcement is necessary
     *
     * @param replicationObjects Ordered list of modification changes that can be replicated to secondary placements
     */
    @Override
    public void queueReplicationData( @NonNull List<ChangeDataReplicationObject> replicationObjects ) {

        // This currently schedules and queues the changes per operation & per target placement
        for ( ChangeDataReplicationObject replicationObject : replicationObjects ) {

            log.info( "Begin queuing changes for data object: {} \n", replicationObject.getReplicationDataId() );

            // First enrich local queue and add last put element in queue
            // Create
            replicationData.put( replicationObject.getReplicationDataId(), replicationObject );
            for ( Entry<Long, Pair> individualReplicationEntry : replicationObject.getDependentReplicationIds().entrySet() ) {

                long replicationId = individualReplicationEntry.getKey();
                Pair<Long, Integer> partitionPlacementIdentifier = individualReplicationEntry.getValue();

                if ( !localPartitionPlacementQueue.containsKey( partitionPlacementIdentifier ) ) {
                    localPartitionPlacementQueue.put( partitionPlacementIdentifier, new ArrayList<>() );
                }
                // Queue pending replicationId in local list of partitionPlacement.
                localPartitionPlacementQueue.get( partitionPlacementIdentifier ).add( replicationId );
                queueIndividualReplication( replicationId, replicationObject.getReplicationDataId(), false );
                log.info( "\tQueued changes for placement: ( {} on {} ) using data {} ",
                        partitionPlacementIdentifier.left,
                        partitionPlacementIdentifier.right,
                        replicationObject.getReplicationDataId() );
            }
            log.info( "All changes have been queued for data object {} \n\n", replicationObject.getReplicationDataId() );
        }
        log.info( "All changes have been queued" );
    }


    private void queueIndividualReplication( long replicationId, long replicationDataId, boolean requeue ) {
        threadPoolWorkers.execute( new LazyReplicationWorker( replicationId, replicationDataId ) );
        // TODO @HENNLO Check number of requeues. After a certain threshold
        if ( requeue ) {
            log.info( "Replication {} has been requeued again", replicationId );
        }
    }



    /**
     * Is used to process queued replication events
     * To update the stores lazily.
     */
    class LazyReplicationWorker implements Runnable {

        @Getter
        private ChangeDataReplicationObject replicationObject;

        private long replicationId;
        private long replicationDataId;
        private Pair<Long, Integer> targetPartitionPlacementIdentifier;
        Transaction transaction = null;


        public LazyReplicationWorker( long replicationId, long replicationDataId ) {

            this.replicationObject = replicationData.get( replicationDataId );
            this.replicationId = replicationId;
            this.replicationDataId = replicationDataId;
            this.targetPartitionPlacementIdentifier = getTargetPartitionPlacementIdentifier();
        }


        @Override
        public void run() {
            if ( checkReplicationPrerequisites() ) {
                processReplicationQueue();
            }
        }


        private void processReplicationQueue() {
            log.info( "Processing replicationObject" );
            log.info( "Replicating object with id: {}:"
                            + "\n\tOperation: {}"
                            + "\n\tTable: {}"
                            + "\n\tTarget: {}"
                            + "\n\tValues: {}"
                    , replicationObject.getReplicationDataId(), replicationObject.getTableId(), replicationObject.getOperation(), replicationObject.getDependentReplicationIds(), replicationObject.getParameterValues() );

            CatalogTable catalogTable = catalog.getTable( replicationObject.getTableId() );

            boolean success = false;
            long modifications = 0;
            try {

                transaction = transactionManager.startTransaction( "pa", catalogTable.getDatabaseName(), false, "Data Replicator" );
                modifications = dataReplicator.replicateData( transaction, replicationObject, replicationId );

                transaction.commit();
                success = true;
            } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException | TransactionException e ) {
                log.error( "Error while replicating object ", e );
                if ( transaction != null ) {
                    try {
                        transaction.rollback();
                    } catch ( TransactionException ex ) {
                        log.error( "Error while rolling back the transaction", e );
                    }
                }
            }
            finalizeProcessing( success, modifications );
        }


        /**
         * Checks if the current replicaiton can be directly applied to the target or another target replication comes first.
         * Since this one might depend on it.
         * If it can execute it returns 'true' otherwise false and directly reschedules this replication. No further actions are necessary.
         *
         * @return If the replication can be executed or if it has to be postponed
         */
        private boolean checkReplicationPrerequisites() {
            //TODO @HENNLO  apply a lock here
            long dependentReplicationId = getNextReplicationId();
            if ( dependentReplicationId < 0 ) {
                // CLEANUP Resources since it has no further replications left.
                cleanUpLocalReplication( false );
                return false;
            } else if ( dependentReplicationId != replicationId ) {
                log.info( "The replication for replicationId '{}' cannot be executed. Depends on replication '{}' to be executed first. Re-queueing it.", replicationId, dependentReplicationId );
                queueIndividualReplication( replicationId, replicationDataId, true );
                return false;
            }
            return true;
            //TODO @HENNLO  release the lock here
        }


        /**
         * @return The next ReplicationId needed to be applied to a placement. If the list is empty '-1' is returned indicating that the queue is empty.
         */
        private long getNextReplicationId() {

            if ( localPartitionPlacementQueue.containsKey( targetPartitionPlacementIdentifier ) ) {
                return localPartitionPlacementQueue.get( targetPartitionPlacementIdentifier ).get( 0 );
            }
            return -1;
        }


        private Pair<Long, Integer> getTargetPartitionPlacementIdentifier() {
            if ( replicationObject.getDependentReplicationIds().containsKey( replicationId ) ) {
                return replicationObject.getDependentReplicationIds().get( replicationId );
            } else {
                log.warn( "The replication for replicationId '{}' cannot be executed. Since it is not associated with any replication object.", replicationId );
                throw new RuntimeException();
            }
        }


        private void cleanUpLocalReplication( boolean force ) {
            // TODO @HENNLO  apply a lock here
            // Make sure that the next replication inline is indeed the one this worker has processed.
            if ( getNextReplicationId() == replicationId || force ) {
                // Remove the replication from local execution queue
                localPartitionPlacementQueue.get( targetPartitionPlacementIdentifier ).remove( replicationId );

                // Check if this was the last replication to be applied for this specific target
                if ( localPartitionPlacementQueue.get( targetPartitionPlacementIdentifier ).size() <= 0 || force ) {
                    localPartitionPlacementQueue.remove( targetPartitionPlacementIdentifier );
                    log.info( "This was the last pending replication for the target  ( {} on {} ). Removing its local queue.",
                            targetPartitionPlacementIdentifier.left, targetPartitionPlacementIdentifier.right );
                }

                replicationObject.removeSingleReplicationFromDependency( replicationId );

                // Check if this was the last target that depends on the replication object
                if ( replicationObject.getDependentReplicationIds().isEmpty() ) {
                    replicationData.remove( replicationDataId );
                    log.info( "This was the last replication that depended on the replicationData '{}'. Removing it.", replicationDataId );
                }
            } else {
                log.error( "Replication: '{}' is not the next replication something went wrong", replicationId );
                throw new RuntimeException();
            }
            //TODO @HENNLO  RELEASE lock here
        }


        private void finalizeProcessing( boolean success, long modifications ) {
            if ( success ) {
                handleSuccessfulReplication( modifications );
            } else {
                handleFailedReplication();
            }
        }


        /**
         * Updates all changed partitionPlacements with the new update information
         *
         * @param modifications number of modifications that have been applied to the secondary
         */
        private void updateCatalogInformation( long modifications ) {
            catalog.updatePartitionPlacementProperties(
                    targetPartitionPlacementIdentifier.right,
                    targetPartitionPlacementIdentifier.left,
                    replicationObject.getCommitTimestamp(),
                    replicationObject.getParentTxId(),
                    transaction.getCommitTimestamp(),
                    replicationId,
                    modifications );
        }


        private void handleSuccessfulReplication( long modifications ) {
            updateCatalogInformation( modifications );
            cleanUpLocalReplication( false );

        }


        private void handleFailedReplication() {
            log.info( "The replication for replicationId {} has failed.", replicationId );
            if ( increaseReplicationFailCount( replicationId ) ) {
                log.info( "Re-queueing replicationId {} with remaining fail count: '{}'.",
                        replicationId,
                        REPLICATION_FAIL_COUNT_THRESHOLD.getInt() - replicationFailCount.get( replicationId ) );

                queueIndividualReplication( replicationId, replicationDataId, true );
            } else {
                log.info( "The replication for replicationId {} has reached its fail count: '{}'.", replicationId, REPLICATION_FAIL_COUNT_THRESHOLD.getInt() );
                log.info( "Marking the designated target ( {} on {} ) as INFINITELY OUTDATED.",
                        targetPartitionPlacementIdentifier.left,
                        targetPartitionPlacementIdentifier.right );
                log.info( "Forcefully removing all pending replications from the target ( {} on {} ).",
                        targetPartitionPlacementIdentifier.left,
                        targetPartitionPlacementIdentifier.right );
                cleanUpLocalReplication( true );

                catalog.updatePartitionPartitionPlacementState(
                        targetPartitionPlacementIdentifier.right,
                        targetPartitionPlacementIdentifier.left,
                        PlacementState.INFINITELY_OUTDATED );
            }
        }

    }


    public class LazyReplicationConfigListener implements ConfigListener {

        @Override
        public void onConfigChange( Config c ) {

            log.info( "\tChanged config: AUTOMATIC_DATA_REPLICATION to: {} ", AUTOMATIC_DATA_REPLICATION.getBoolean() );
            if ( AUTOMATIC_DATA_REPLICATION.getBoolean() ) {
                threadPoolWorkers.resume();
            } else {
                threadPoolWorkers.pause();
            }

        }


        @Override
        public void restart( Config c ) {
            log.info( "\tChanged config: AUTOMATIC_DATA_REPLICATION to: {} ", AUTOMATIC_DATA_REPLICATION.getBoolean() );
            if ( AUTOMATIC_DATA_REPLICATION.getBoolean() ) {
                threadPoolWorkers.resume();
            } else {
                threadPoolWorkers.pause();
            }
        }
    }

}
