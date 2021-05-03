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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import lombok.NonNull;
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
    private final MonitoringRepository repository;
    // number of elements beeing processed from the queue to the backend per "batch"
    private final int QUEUE_PROCESSING_ELEMENTS = 50;
    private HashMap<Class, List<MonitoringMetricSubscriber>> subscribers = new HashMap();
    private String backgroundTaskId;
    //For ever
    private long processedEventsTotal;

    //Since restart
    private long processedEvents;


    //additional field that gets aggregated as soon as new subscription is in place
    //to better retrieve a distinct list of subscribers
    private Set<MonitoringMetricSubscriber> allSubscribers = new HashSet<>();

    // endregion

    // region ctors


    /**
     * Ctor which automatically will start the background task based on the given boolean
     *
     * @param startBackGroundTask Indicates whether the background task for consuming the queue will be started.
     */
    public MonitoringQueueImpl( boolean startBackGroundTask, @NonNull MonitoringRepository repository ) {
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
    public MonitoringQueueImpl( @NonNull MonitoringRepository repository ) {
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
    public void queueEvent( @NonNull MonitoringEvent event ) {
        this.monitoringJobQueue.add( event );
    }


    @Override
    public <T extends MonitoringMetric> void subscribeMetric( Class<T> metricClass, MonitoringMetricSubscriber<T> subscriber ) {
        //Can be added all the time since we are using a set
        //Its faster than using  list and an if
        allSubscribers.add( subscriber );

        if ( this.subscribers.containsKey( metricClass ) ) {
            this.subscribers.get( metricClass ).add( subscriber );
        } else {
            this.subscribers.putIfAbsent( metricClass, Arrays.asList( subscriber ) );
        }
    }


    @Override
    public <T extends MonitoringMetric> boolean unsubscribeMetric( Class<T> metricClass, MonitoringMetricSubscriber<T> subscriber ) {

        List<MonitoringMetricSubscriber> tempSubs;

        if ( this.subscribers.containsKey( metricClass ) ) {
            tempSubs = new ArrayList<>( this.subscribers.get( metricClass ) );
            tempSubs.remove( subscriber );
            this.subscribers.put( metricClass, tempSubs );
        }

        // If this was the last occurence of the Subscriber in any Subscription remove him from ALL list
        // TODO: Für was brauchst du das Feld allSubscribers. Ist doch nur aufwändig die 2 mal zu halten...?
        if ( !hasActiveSubscription( subscriber ) ) {
            allSubscribers.remove( subscriber );
            return true;
        }

        //returns false only if it wasn't last subscription
        return false;
    }


    @Override
    public void unsubscribeFromAllMetrics( @NonNull MonitoringMetricSubscriber subscriber ) {
        // TODO: Macht für mich irgendwie auch nicht so sinn. Ein Subsriber hat in den meisten fällen sowieso nur eine metric aboniert,
        //  ansonsten müsste der das Interface MonitoringMetricSubscriber mehrfach implementieren.
        //  Wäre natürlich möglich aber fäne ich ein wenig komisch.

        for ( Entry entry : subscribers.entrySet() ) {
            if ( subscribers.get( entry.getKey() ).contains( subscriber ) ) {
                unsubscribeMetric( (Class) entry.getKey(), subscriber );
            }
        }

    }


    @Override
    public List<MonitoringEvent> getElementsInQueue() {
        // TODO: Würde ich definitiv nicht so machen. Wenn du im UI die Anzahl Events
        //   wissen willst dann unbedingt nur die Anzahl rausgeben. Sonst gibt du die ganzen Instanzen raus und
        //   könntest die Queue zum übelsten missbrauchen ;-)

        List<MonitoringEvent> eventsInQueue = new ArrayList<>();

        for ( MonitoringEvent event : monitoringJobQueue ) {
            eventsInQueue.add( event );
        }

        System.out.println( "Contents in Queue: " + monitoringJobQueue );

        return eventsInQueue;
    }


    @Override
    public long getNumberOfProcessedEvents( boolean all ) {
        // TODO: Wird hier noch das persistiert? Könnten wir selbst als Metric aufbauen und persistieren ;-)
        if ( all ) {
            return processedEventsTotal;
        }
        //returns only processed events since last restart
        return processedEvents;
    }


    @Override
    public List<MonitoringMetricSubscriber> getActiveSubscribers() {
        // TODO: würde ich auch nur die Anzahl rausgeben, könnte auch ziemlich misbraucht werden...
        return allSubscribers.stream().collect( Collectors.toList() );
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
            int countEvents = 0;
            while ( (event = this.getNextJob()).isPresent() && countEvents < QUEUE_PROCESSING_ELEMENTS ) {
                log.debug( "get new monitoring job" + event.get().getId().toString() );

                //returns list of metrics which was produced by this particular event
                val metrics = event.get().analyze();

                //Sends all extracted metrics to subscribers
                for ( val metric : metrics ) {
                    this.repository.persistMetric( metric );
                    this.notifySubscribers( metric );
                }

                countEvents++;
            }
            processedEvents += countEvents;
            processedEventsTotal += processedEvents;
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


    /**
     * Mainly used as a helper to identify if subscriber has active subscriptions left or can be completely removed from Broker
     *
     * @param subscriber
     * @return if Subscriber ist still registered to events
     */
    private boolean hasActiveSubscription( MonitoringMetricSubscriber subscriber ) {

        for ( Entry currentSub : subscribers.entrySet() ) {
            if ( subscribers.get( currentSub.getKey() ).contains( subscriber ) ) {
                return true;
            }
        }

        return false;
    }

    // endregion
}
