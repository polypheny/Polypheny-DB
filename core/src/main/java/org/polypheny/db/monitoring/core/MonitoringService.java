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

import java.sql.Timestamp;
import java.util.List;
import org.polypheny.db.monitoring.events.MonitoringEvent;
import org.polypheny.db.monitoring.events.MonitoringMetric;
import org.polypheny.db.monitoring.subscriber.MonitoringMetricSubscriber;

/**
 * Main interface for working with the MonitoringService environment. Jobs can be registered, monitored
 * and subscriber can the registered based on MonitoringEventData
 */
public interface MonitoringService {

    <T extends MonitoringMetric> void subscribeMetric(Class<T> metricClass, MonitoringMetricSubscriber<T> subscriber );

    <T extends MonitoringMetric> void unsubscribeMetric( Class<T> metricClass, MonitoringMetricSubscriber<T> subscriber );

    void unsubscribeFromAllMetrics( MonitoringMetricSubscriber subscriber );

    /**
     * monitor event which will be queued immediately and get processed by a registered queue worker.
     *
     * @param eventData The event data object.
     * @param <T> The type parameter for the event, which will implement MonitoringEventData
     */
    <T extends MonitoringEvent> void monitorEvent( T eventData );

    /**
     * Get all data for given monitoring persistent type.
     *
     * @param metricClass
     * @param <T>
     * @return
     */
    <T extends MonitoringMetric> List<T> getAllMetrics( Class<T> metricClass );

    /**
     * Get data before specified timestamp for given monitoring persistent type.
     *
     * @param metricClass
     * @param <T>
     * @return
     */
    <T extends MonitoringMetric> List<T> getMetricsBefore( Class<T> metricClass, Timestamp timestamp );

    /**
     * Get data after specified timestamp for given monitoring persistent type.
     *
     * @param metricClass
     * @param <T>
     * @return
     */
    <T extends MonitoringMetric> List<T> getMetricsAfter( Class<T> metricClass, Timestamp timestamp );

}

