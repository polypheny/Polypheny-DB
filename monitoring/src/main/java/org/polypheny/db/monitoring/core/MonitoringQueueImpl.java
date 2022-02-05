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

package org.polypheny.db.monitoring.core;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.MonitoringEvent;
import org.polypheny.db.monitoring.repository.MonitoringRepository;
import org.polypheny.db.monitoring.repository.PersistentMonitoringRepository;
import org.polypheny.db.util.background.BackgroundTask;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;
import org.polypheny.db.util.background.BackgroundTaskManager;


/**
 * MonitoringQueue implementation which stores the monitoring jobs in a concurrent queue and processes them with a
 * background worker task.
 */
@Slf4j
public class MonitoringQueueImpl implements MonitoringQueue {

    // Monitoring queue which will queue all the incoming jobs.
    private final Queue<MonitoringEvent> monitoringJobQueue = new ConcurrentLinkedQueue<>();

    private final Set<UUID> queueIds = Sets.newConcurrentHashSet();
    private final Lock processingQueueLock = new ReentrantLock();
    private final PersistentMonitoringRepository persistentRepository;
    private final MonitoringRepository statisticRepository;

    private String backgroundTaskId;

    /**
     * Processed events since restart.
     */
    private long processedEvents;
    private long processedEventsTotal;


    /**
     * Ctor which automatically will start the background task based on the given boolean
     *
     * @param startBackGroundTask Indicates whether the background task for consuming the queue will be started.
     */
    public MonitoringQueueImpl(
            boolean startBackGroundTask,
            @NonNull PersistentMonitoringRepository persistentRepository,
            @NonNull MonitoringRepository statisticRepository ) {
        this.persistentRepository = persistentRepository;
        this.statisticRepository = statisticRepository;

        if ( startBackGroundTask ) {
            this.startBackgroundTask();
        }
    }


    /**
     * Ctor will automatically start the background task for consuming the queue.
     */
    public MonitoringQueueImpl(
            @NonNull PersistentMonitoringRepository persistentRepository,
            @NonNull MonitoringRepository statisticRepository ) {
        this( true, persistentRepository, statisticRepository );
    }


    public void terminateQueue() {
        if ( backgroundTaskId != null ) {
            BackgroundTaskManager.INSTANCE.removeBackgroundTask( backgroundTaskId );
        }
    }


    @Override
    public void queueEvent( @NonNull MonitoringEvent event ) {
        if ( !queueIds.contains( event.getId() ) ) {
            queueIds.add( event.getId() );
            this.monitoringJobQueue.add( event );
        }
    }


    /**
     * Display current number of elements in queue
     *
     * @return Current number of elements in Queue
     */
    @Override
    public long getNumberOfElementsInQueue() {
        return queueIds.size();
    }


    @Override
    public List<HashMap<String, String>> getInformationOnElementsInQueue() {
        List<HashMap<String, String>> infoList = new ArrayList<>();

        for ( MonitoringEvent event : monitoringJobQueue.stream().limit( 100 ).collect( Collectors.toList() ) ) {
            HashMap<String, String> infoRow = new HashMap<>();
            infoRow.put( "type", event.getClass().toString() );
            infoRow.put( "id", event.getId().toString() );
            infoRow.put( "timestamp", event.getRecordedTimestamp().toString() );

            infoList.add( infoRow );
        }
        return infoList;
    }


    @Override
    public long getNumberOfProcessedEvents( boolean all ) {
        if ( all ) {
            return processedEventsTotal;
        }
        // Returns only processed events since last restart
        return processedEvents;
    }


    private void startBackgroundTask() {
        if ( backgroundTaskId == null ) {
            backgroundTaskId = BackgroundTaskManager.INSTANCE.registerTask(
                    this::processQueue,
                    "Send monitoring jobs to job consumers",
                    BackgroundTask.TaskPriority.LOW,
                    (TaskSchedulingType) RuntimeConfig.QUEUE_PROCESSING_INTERVAL.getEnum()
            );
        }
    }


    private void processQueue() {
        log.debug( "Start processing queue" );
        this.processingQueueLock.lock();

        MonitoringEvent event;

        try {
            // While there are jobs to consume:
            int countEvents = 0;
            while ( (event = this.getNextJob()) != null && countEvents < RuntimeConfig.QUEUE_PROCESSING_ELEMENTS.getInteger() ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "get new monitoring job {}", event.getId().toString() );
                }

                // Returns list of metrics which was produced by this particular event
                final List<MonitoringDataPoint> dataPoints = event.analyze();
                if ( dataPoints.isEmpty() ) {
                    continue;
                }

                // Sends all extracted metrics to subscribers
                for ( MonitoringDataPoint dataPoint : dataPoints ) {
                    this.persistentRepository.dataPoint( dataPoint );
                    // Statistics are only collected if Active Tracking is switched on
                    if ( RuntimeConfig.ACTIVE_TRACKING.getBoolean() && RuntimeConfig.DYNAMIC_QUERYING.getBoolean() ) {
                        this.statisticRepository.dataPoint( dataPoint );
                    }
                }

                countEvents++;
                queueIds.remove( event.getId() );
            }
            processedEvents += countEvents;
            processedEventsTotal += countEvents;
        } finally {
            this.processingQueueLock.unlock();
        }
    }


    private MonitoringEvent getNextJob() {
        if ( monitoringJobQueue.peek() != null ) {
            return monitoringJobQueue.poll();
        }
        return null;
    }

}
