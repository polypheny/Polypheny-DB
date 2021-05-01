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

import java.util.List;
import java.util.Queue;
import org.polypheny.db.monitoring.events.MonitoringEvent;
import org.polypheny.db.monitoring.events.MonitoringMetric;
import org.polypheny.db.monitoring.subscriber.MonitoringMetricSubscriber;

/**
 * Monitoring queue interface which will
 * queue the incoming MonitoringEvents in a queue.
 * Moreover, queue workers can be registered.
 */
public interface MonitoringQueue {

    /**
     * Monitoring events objects implementing MonitoringEventData will be queued.
     * If the MonitoringEventData Class is registered,
     *
     * @param eventData the event data which will be queued.
     */
    void queueEvent( MonitoringEvent eventData );

    /** Essential usage to display current contents of queue
     *
     * @return All current elements in Queue
     */
    List<MonitoringEvent> getElementsInQueue();

    long getNumberOfProcessedEvents(boolean all);

    List<MonitoringMetricSubscriber> getActiveSubscribers();

    <T extends MonitoringMetric>
    void subscribeMetric( Class<T> metricClass, MonitoringMetricSubscriber<T> subscriber );

    /**
     *
     * @param metricClass
     * @param subscriber
     * @param <T>
     * @return true if there a subscriptions left. And false if that was the last subscription
     */
    <T extends MonitoringMetric>
    boolean unsubscribeMetric( Class<T> metricClass, MonitoringMetricSubscriber<T> subscriber );


    void unsubscribeFromAllMetrics( MonitoringMetricSubscriber subscriber );

}
