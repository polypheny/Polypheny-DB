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
import org.polypheny.db.catalog.Catalog.ReplicationStrategy;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigBoolean;
import org.polypheny.db.config.ConfigInteger;
import org.polypheny.db.config.WebUiGroup;
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


    // Only needed to track changes on uniquely identifiable replicationData

    // TODO @HENNLO add monitoring page that lists number of pending updates per table and per partition

    // TODO @HENNLO as soon as policies are active configure db, schema or db if changes should be replicated by single operation or only ACID compliant with entire transaction
    // DISTRIBUTION STRATEGY

    // TODO @HENNLO make sure that RuntimeConfig.AUTOMATIC_DATA_REPLICATION.getBoolean() really only blocks distribution but queue is still enriched

    // TODO add basic LAZY Replication Engine
    // Then add serializable LazyReplicationEngine like this one
    // and add others with more lose constraints

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
                threadPoolWorkers.execute( new LazyReplicationWorker( replicationId, replicationObject.getReplicationDataId() ) );
                log.info( "\tQueued changes for placement: ( {} on {} ) using data {} ",
                        partitionPlacementIdentifier.left,
                        partitionPlacementIdentifier.right,
                        replicationObject.getReplicationDataId() );
            }
            log.info( "All changes have been queued for data object {} \n\n", replicationObject.getReplicationDataId() );
        }
        log.info( "All changes have been queued" );
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


        public LazyReplicationWorker( long replicationId, long replicationDataId ) {

            this.replicationObject = replicationData.get( replicationDataId );
            this.replicationId = replicationId;
            this.replicationDataId = replicationDataId;
        }


        @Override
        public void run() {
            processReplicationQueue();
        }


        private void processReplicationQueue() {
            log.info( "Processing replicationObject" );
            log.info( "Replicating object with id: {}:"
                            + "\n\tOperation: {}"
                            + "\n\tTable: {}"
                            + "\n\tTarget: {}"
                            + "\n\tValues: {}"
                    , replicationObject.getReplicationDataId(), replicationObject.getTableId(), replicationObject.getOperation(), replicationObject.getDependentReplicationIds(), replicationObject.getParameterValues() );
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
