/*
 * Copyright 2019-2024 The Polypheny Project
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.MonitoringEvent;
import org.polypheny.db.monitoring.repository.MonitoringRepository;
import org.polypheny.db.monitoring.repository.PersistentMonitoringRepository;


/**
 * MonitoringQueue implementation which stores the monitoring jobs in a concurrent queue and processes them with a
 * background worker task.
 */
@Slf4j
public class MonitoringQueueImpl implements MonitoringQueue {

    private final PersistentMonitoringRepository persistentRepository;
    private final MonitoringRepository statisticRepository;
    private MonitoringThreadPoolExecutor threadPoolWorkers;

    private final BlockingQueue<Runnable> eventQueue = new LinkedBlockingQueue<>();

    private final int CORE_POOL_SIZE;
    private final int MAXIMUM_POOL_SIZE;
    private final int KEEP_ALIVE_TIME;

    private boolean backgroundProcessingActive;
    private int threadCount;


    /**
     * Ctor which automatically will start the background task based on the given boolean
     *
     * @param backgroundProcessingActive Indicates whether the background task for consuming the queue will be started.
     */
    public MonitoringQueueImpl(
            boolean backgroundProcessingActive,
            @NonNull PersistentMonitoringRepository persistentRepository,
            @NonNull MonitoringRepository statisticRepository ) {
        this.persistentRepository = persistentRepository;
        this.statisticRepository = statisticRepository;
        this.backgroundProcessingActive = backgroundProcessingActive;

        this.CORE_POOL_SIZE = RuntimeConfig.MONITORING_CORE_POOL_SIZE.getInteger();
        this.MAXIMUM_POOL_SIZE = RuntimeConfig.MONITORING_MAXIMUM_POOL_SIZE.getInteger();
        this.KEEP_ALIVE_TIME = RuntimeConfig.MONITORING_POOL_KEEP_ALIVE_TIME.getInteger();

        if ( !this.backgroundProcessingActive ) {
            threadPoolWorkers = new MonitoringThreadPoolExecutor(
                    CORE_POOL_SIZE,
                    MAXIMUM_POOL_SIZE,
                    KEEP_ALIVE_TIME,
                    TimeUnit.SECONDS,
                    eventQueue );
            return;
        }

        RuntimeConfig.MONITORING_CORE_POOL_SIZE.setRequiresRestart( true );
        RuntimeConfig.MONITORING_MAXIMUM_POOL_SIZE.setRequiresRestart( true );
        RuntimeConfig.MONITORING_POOL_KEEP_ALIVE_TIME.setRequiresRestart( true );

        threadPoolWorkers = new MonitoringThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAXIMUM_POOL_SIZE,
                KEEP_ALIVE_TIME,
                TimeUnit.SECONDS,
                eventQueue );

        // Instantiated thread count
        this.threadCount = threadPoolWorkers.getPoolSize();

        // Create a scheduled, separate thread which gets new thread count every 500 milliseconds
        Timer timer = new Timer();
        timer.scheduleAtFixedRate( new TimerTask() {
            @Override
            public synchronized void run() {
                int newThreadCount = threadPoolWorkers.getPoolSize();
                if ( newThreadCount != threadCount ) {
                    threadCount = newThreadCount;
                    if ( log.isDebugEnabled() ) {
                        log.debug( "Thread count is now: {}", threadCount );
                    }
                }
            }
        }, 500, 500 );
    }


    /**
     * Ctor will automatically start the background task for consuming the queue.
     */
    public MonitoringQueueImpl(
            @NonNull PersistentMonitoringRepository persistentRepository,
            @NonNull MonitoringRepository statisticRepository ) {
        this( true, persistentRepository, statisticRepository );
    }


    @Override
    public synchronized void queueEvent( @NonNull MonitoringEvent event ) {
        if ( backgroundProcessingActive ) {
            threadPoolWorkers.execute( new MonitoringWorker( event ) );
        } else {
            eventQueue.add( new MonitoringWorker( event ) );
        }
    }


    /**
     * Display current number of elements in queue
     *
     * @return Current number of elements in Queue
     */
    @Override
    public synchronized long getNumberOfElementsInQueue() {
        return threadPoolWorkers.getQueue().size();
    }


    @Override
    public synchronized List<HashMap<String, String>> getInformationOnElementsInQueue() {
        List<HashMap<String, String>> infoList = new ArrayList<>();
        List<MonitoringEvent> queueElements = new ArrayList<>();

        threadPoolWorkers.getQueue().stream().limit( 100 ).toList()
                .forEach(
                        task -> queueElements.add(
                                ((MonitoringWorker) task).getEvent()
                        )
                );

        for ( MonitoringEvent event : queueElements ) {
            HashMap<String, String> infoRow = new HashMap<>();
            infoRow.put( "type", event.getClass().toString() );
            infoRow.put( "id", event.getId().toString() );
            infoRow.put( "timestamp", event.getRecordedTimestamp().toString() );

            infoList.add( infoRow );
        }
        return infoList;
    }


    @Override
    public long getNumberOfProcessedEvents() {
        return threadPoolWorkers.getCompletedTaskCount();
    }


    /**
     * Overrides beforeExecute and afterExecute of ThreadPoolExecutor to check the number of threads
     * and logs new thread count if there is a change.
     */
    static class MonitoringThreadPoolExecutor extends ThreadPoolExecutor {


        private int threadCount;


        public MonitoringThreadPoolExecutor(
                int corePoolSize,
                int maximumPoolSize,
                long keepAliveTime,
                TimeUnit unit,
                BlockingQueue<Runnable> workQueue ) {
            super( corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue );
            this.threadCount = this.getPoolSize();
        }


        @Override
        protected void beforeExecute( Thread t, Runnable r ) {
            if ( this.threadCount != this.getPoolSize() ) {
                this.threadCount = this.getPoolSize();
                if ( log.isDebugEnabled() ) {
                    log.debug( "Thread count for monitoring queue: {}", this.threadCount );
                }
            }

            super.beforeExecute( t, r );
        }


        @Override
        protected void afterExecute( Runnable r, Throwable t ) {
            super.afterExecute( r, t );

            if ( this.threadCount != this.getPoolSize() ) {
                this.threadCount = this.getPoolSize();
                if ( log.isDebugEnabled() ) {
                    log.debug( "Thread count for monitoring queue: {}", this.threadCount );
                }
            }
        }

    }


    @Getter
    class MonitoringWorker implements Runnable {

        private final MonitoringEvent event;


        public MonitoringWorker( MonitoringEvent event ) {
            this.event = event;
        }


        @Override
        public void run() {
            processQueue();
        }


        private void processQueue() {
            if ( event != null ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "get new monitoring job {}", event.getId().toString() );
                }

                // Returns list of metrics which was produced by this particular event
                final List<MonitoringDataPoint> dataPoints = event.analyze();
                if ( !dataPoints.isEmpty() ) {
                    // Sends all extracted metrics to subscribers
                    for ( MonitoringDataPoint dataPoint : dataPoints ) {
                        persistentRepository.dataPoint( dataPoint );
                        // Statistics are only collected if Active Tracking is switched on
                        if ( RuntimeConfig.ACTIVE_TRACKING.getBoolean() ) {
                            statisticRepository.dataPoint( dataPoint );
                        }
                    }
                }
            }
        }

    }

}
