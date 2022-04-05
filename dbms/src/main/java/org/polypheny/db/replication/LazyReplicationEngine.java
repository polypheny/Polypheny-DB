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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang.NotImplementedException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.util.Pair;


public class LazyReplicationEngine extends ReplicationEngine {

    // Only needed to track changes on uniquely identifiable replicationData

    // TODO @HENNLO add monitoring page that lists number of pending updates per table and per partition

    // TODO @HENNLO as soon as policies are active configure db, schema or db if changes should be replicated by single operation or only ACID compliant with entire transaction
    // DISTRIBUTION STRATEGY

    // TODO @HENNLO make sure that RuntimeConfig.AUTOMATIC_DATA_REPLICATION.getBoolean() really only blocks distribution but queue is still enriched

    // TODO add basic LAZY Replication Engine
    // Then add serializable LazyReplicationEngine like this one
    // and add others with more lose constraints


    // Maps replicationDataId to the relevant captured Data that it only will be held in memory once
    private HashMap<Long, ChangeDataReplicationObject> replicationData = new HashMap<>();     // replicationDataIds -> replicationData

    // Maps Adapter-Partition (PartitionPlacement) to a queue. It's necessary that each partitionPlacement has its own queue
    // To decouple changes between placements
    // Otherwise one failing placement in the queue, would block the entire replication
    // Contains pending updates per partition Placement
    private HashMap<Pair, List<Long>> localPartitionPlacementQueue = new HashMap<>();    // (Adapter-Partition) -> List<replicationDataIds>

    // Needs a cyclic list iterator
    private final BlockingQueue globalReplicationDataQueue;


    private ThreadPoolExecutor threadPoolWorkers;
    private final int CORE_POOL_SIZE;
    private final int MAXIMUM_POOL_SIZE;
    private final int KEEP_ALIVE_TIME;


    public LazyReplicationEngine() {
        this.CORE_POOL_SIZE = RuntimeConfig.REPLICATION_CORE_POOL_SIZE.getInteger();
        this.MAXIMUM_POOL_SIZE = RuntimeConfig.REPLICATION_MAXIMUM_POOL_SIZE.getInteger();
        this.KEEP_ALIVE_TIME = RuntimeConfig.REPLICATION_POOL_KEEP_ALIVE_TIME.getInteger();

        this.globalReplicationDataQueue = new LinkedBlockingQueue();

        RuntimeConfig.REPLICATION_CORE_POOL_SIZE.setRequiresRestart( true );
        RuntimeConfig.REPLICATION_MAXIMUM_POOL_SIZE.setRequiresRestart( true );
        RuntimeConfig.REPLICATION_POOL_KEEP_ALIVE_TIME.setRequiresRestart( true );

        threadPoolWorkers = new ThreadPoolExecutor( CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, KEEP_ALIVE_TIME, TimeUnit.SECONDS, globalReplicationDataQueue );
    }


    @Override
    protected void setInstance() {
        INSTANCE = this;
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

        // This currently queues the changes per operation

        for ( ChangeDataReplicationObject replicationObject : replicationObjects ) {

            // First enrich local queue and add last put element in queue
            // Create
            replicationData.put( replicationObject.getReplicationDataId(), replicationObject );
            //TODO @HENNLO switch adapter and partiotin key since partiton is looked up more frequently and needs faster access times
            for ( Pair<Integer, Long> partitionPlacementIdentifier : replicationObject.getTargetPartitionPlacements() ) {

                if ( !localPartitionPlacementQueue.containsKey( partitionPlacementIdentifier ) ) {
                    localPartitionPlacementQueue.put( partitionPlacementIdentifier, new ArrayList<>() );
                }
                // Queue pending replicationId in local list of partitionPlacement.
                localPartitionPlacementQueue.get( partitionPlacementIdentifier ).add( replicationObject.getReplicationDataId() );
            }

            threadPoolWorkers.execute( new LazyReplicationWorker( replicationObject ) );
        }
    }


    /**
     * Is used to process queued replication events
     * To update the stores lazily.
     */
    class LazyReplicationWorker implements Runnable {

        @Getter
        private ChangeDataReplicationObject replicationObject;


        public LazyReplicationWorker( ChangeDataReplicationObject replicationObject ) {
            this.replicationObject = replicationObject;
        }


        @Override
        public void run() {
            // Changes should still be captured but not actively replicated to help in case of overload situations inside Polypheny-DB.
            if ( RuntimeConfig.AUTOMATIC_DATA_REPLICATION.getBoolean() ) {
                processReplicationQueue();
            }
        }


        private void processReplicationQueue() {
            System.out.println( "Processing item replicationObject" );
        }

    }

}
