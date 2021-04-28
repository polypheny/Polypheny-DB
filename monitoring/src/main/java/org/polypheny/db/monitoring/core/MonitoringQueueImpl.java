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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.monitoring.events.MonitoringEvent;
import org.polypheny.db.monitoring.events.MonitoringMetric;
import org.polypheny.db.monitoring.persistence.MonitoringRepository;
import org.polypheny.db.monitoring.subscriber.MonitoringMetricSubscriber;
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
    private final Queue<MonitoringEvent> monitoringJobQueue = new ConcurrentLinkedQueue<>();

    private final Lock processingQueueLock = new ReentrantLock();

    private final HashMap<Class, List<MonitoringMetricSubscriber>> subscribers = new HashMap();

    private final MonitoringRepository repository;

    private String backgroundTaskId;

    // endregion

    // region ctors


    /**
     * Ctor which automatically will start the background task based on the given boolean
     *
     * @param startBackGroundTask Indicates whether the background task for consuming the queue will be started.
     */
    public MonitoringQueueImpl( boolean startBackGroundTask, MonitoringRepository repository ) {
        log.info( "write queue service" );

        if ( repository == null ) {
            throw new IllegalArgumentException( "repo parameter is null" );
        }

        this.repository = repository;

        if ( startBackGroundTask ) {
            this.startBackgroundTask();
        }
    }


    /**
     * Ctor will automatically start the background task for consuming the queue.
     */
    public MonitoringQueueImpl( MonitoringRepository repository ) {
        this( true, repository );
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
    public void queueEvent( MonitoringEvent event ) {
        if ( event == null ) {
            throw new IllegalArgumentException( "Empty event data" );
        }

        this.monitoringJobQueue.add( event );
    }


    @Override
    public <T extends MonitoringMetric> void subscribeMetric( Class<T> metricClass, MonitoringMetricSubscriber<T> subscriber ) {
        if ( this.subscribers.containsKey( metricClass ) ) {
            this.subscribers.get( metricClass ).add( subscriber );
        } else {
            this.subscribers.putIfAbsent( metricClass, Arrays.asList( subscriber ) );
        }
    }


    @Override
    public <T extends MonitoringMetric> void unsubscribeMetric( Class<T> metricClass, MonitoringMetricSubscriber<T> subscriber ) {
        this.subscribers.get( metricClass ).remove( subscriber );
    }

    // endregion

    // region private helper methods


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

        Optional<MonitoringEvent> event;

        try {
            // while there are jobs to consume:
            while ( (event = this.getNextJob()).isPresent() ) {
                log.debug( "get new monitoring job" + event.get().id().toString() );
                val metrics = event.get().analyze();

                for ( val metric : metrics ) {
                    this.repository.persistMetric( metric );
                    this.notifySubscribers( metric );
                }


            }
        } finally {
            this.processingQueueLock.unlock();
        }
    }


    private void notifySubscribers( MonitoringMetric metric ) {

        val classSubscribers = this.subscribers.get( metric.getClass() );
        if ( classSubscribers != null ) {
            classSubscribers.forEach( s -> s.update( metric ) );
        }


    }


    private Optional<MonitoringEvent> getNextJob() {
        if ( monitoringJobQueue.peek() != null ) {
            return Optional.of( monitoringJobQueue.poll() );
        }
        return Optional.empty();
    }

    // endregion
}
