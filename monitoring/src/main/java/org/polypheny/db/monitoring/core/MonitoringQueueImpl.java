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

package org.polypheny.db.monitoring.core;

import java.util.HashMap;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.monitoring.dtos.MonitoringData;
import org.polypheny.db.monitoring.dtos.MonitoringJob;
import org.polypheny.db.monitoring.dtos.MonitoringPersistentData;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.background.BackgroundTask;
import org.polypheny.db.util.background.BackgroundTaskManager;

/**
 * MonitoringQueue implementation which stores the monitoring jobs in a
 * concurrentQueue and will process them with a background worker task.
 */
@Slf4j
public class MonitoringQueueImpl implements MonitoringQueue {

    // region private fields

    /**
     * monitoring queue which will queue all the incoming jobs.
     */
    private final Queue<MonitoringJob> monitoringJobQueue = new ConcurrentLinkedQueue<>();

    private final Lock processingQueueLock = new ReentrantLock();

    /**
     * The registered job type pairs. The pairs are always of type
     * ( Class<MonitoringEventData> , Class<MonitoringPersistentData>)
     */
    private final HashMap<Pair<Class, Class>, MonitoringQueueWorker> jobQueueWorkers = new HashMap();

    private String backgroundTaskId;

    // endregion

    // region ctors


    /**
     * Ctor which automatically will start the background task based on the given boolean
     *
     * @param startBackGroundTask Indicates whether the background task for consuming the queue will be started.
     */
    public MonitoringQueueImpl( boolean startBackGroundTask ) {
        log.info( "write queue service" );
        if ( startBackGroundTask ) {
            this.startBackgroundTask();
        }
    }


    /**
     * Ctor will automatically start the background task for consuming the queue.
     */
    public MonitoringQueueImpl() {
        this( true );
    }

    // endregion

    // region public methods


    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if ( backgroundTaskId != null ) {
            BackgroundTaskManager.INSTANCE.removeBackgroundTask( backgroundTaskId );
        }
    }


    @Override
    public void queueEvent( MonitoringData eventData ) {
        if ( eventData == null ) {
            throw new IllegalArgumentException( "Empty event data" );
        }

        val job = this.createMonitorJob( eventData );
        if ( job.isPresent() ) {
            this.monitoringJobQueue.add( job.get() );
        }
    }


    @Override
    public void queueJob( MonitoringJob job ) {
        if ( job.getMonitoringPersistentData() == null ) {
            val createdJob = this.createMonitorJob( job.getMonitoringData() );
            if ( createdJob.isPresent() ) {
                this.monitoringJobQueue.add( createdJob.get() );
            }
        } else if ( job.getMonitoringData() != null ) {
            this.monitoringJobQueue.add( job );
        }
    }


    @Override
    public <TEvent extends MonitoringData, TPersistent extends MonitoringPersistentData>
    void registerQueueWorker( Pair<Class<TEvent>, Class<TPersistent>> classPair, MonitoringQueueWorker<TEvent, TPersistent> worker ) {
        if ( classPair == null || worker == null ) {
            throw new IllegalArgumentException( "Parameter is null" );
        }

        if ( this.jobQueueWorkers.containsKey( classPair ) ) {
            throw new IllegalArgumentException( "Consumer already registered" );
        }

        val key = new Pair<Class, Class>( classPair.left, classPair.right );
        this.jobQueueWorkers.put( key, worker );
    }

    // endregion

    // region private helper methods


    /**
     * will try to create a MonitoringJob which incoming eventData object
     * and newly created but empty MonitoringPersistentData object.
     *
     * @return Will return an Optional MonitoringJob
     */
    private Optional<MonitoringJob> createMonitorJob( MonitoringData eventData ) {
        val pair = this.getTypesForEvent( eventData );
        if ( pair.isPresent() ) {
            try {
                val job = new MonitoringJob( eventData, (MonitoringPersistentData) pair.get().right.newInstance() );
                return Optional.of( job );
            } catch ( InstantiationException e ) {
                log.error( "Could not instantiate monitoring job" );
            } catch ( IllegalAccessException e ) {
                log.error( "Could not instantiate monitoring job" );
            }
        }

        return Optional.empty();
    }


    private Optional<Pair<Class, Class>> getTypesForEvent( MonitoringData eventData ) {
        // use the registered worker to find the eventData and return optional key of the entry.
        return this.jobQueueWorkers.keySet().stream().filter( elem -> elem.left.isInstance( eventData ) ).findFirst();
    }


    private void startBackgroundTask() {
        if ( backgroundTaskId == null ) {
            backgroundTaskId = BackgroundTaskManager.INSTANCE.registerTask(
                    this::processQueue,
                    "Send monitoring jobs to job consumers",
                    BackgroundTask.TaskPriority.LOW,
                    BackgroundTask.TaskSchedulingType.EVERY_TEN_SECONDS
            );
        }
    }


    private void processQueue() {
        log.debug( "Start processing queue" );
        this.processingQueueLock.lock();

        Optional<MonitoringJob> job;

        try {
            // while there are jobs to consume:
            while ( (job = this.getNextJob()).isPresent() ) {
                log.debug( "get new monitoring job" + job.get().getId().toString() );

                // get the worker
                MonitoringJob finalJob = job.get();
                val workerKey = new Pair( finalJob.getMonitoringData().getClass(), finalJob.getMonitoringPersistentData().getClass() );
                val worker = jobQueueWorkers.get( workerKey );

                if ( worker != null ) {
                    val result = worker.handleJob( finalJob );
                    // TODO: call subscriber
                    // First subscriber need to be registered in the queue
                } else {
                    log.error( "no worker for event registered" );
                }
            }
        } finally {
            this.processingQueueLock.unlock();
        }
    }


    private Optional<MonitoringJob> getNextJob() {
        if ( monitoringJobQueue.peek() != null ) {
            return Optional.of( monitoringJobQueue.poll() );
        }
        return Optional.empty();
    }

    // endregion
}
