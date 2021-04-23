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

import org.polypheny.db.monitoring.dtos.MonitoringData;
import org.polypheny.db.monitoring.dtos.MonitoringJob;
import org.polypheny.db.monitoring.dtos.MonitoringPersistentData;
import org.polypheny.db.monitoring.subscriber.MonitoringEventSubscriber;

/**
 * Main interface for working with the MonitoringService environment. Jobs can be registered, monitored
 * and subscriber can the registered based on MonitoringEventData
 */
public interface MonitoringService {

    <TPersistent extends MonitoringPersistentData> void subscribeEvent( Class<TPersistent> eventDataClass, MonitoringEventSubscriber<TPersistent> subscriber );

    <TPersistent extends MonitoringPersistentData> void unsubscribeEvent( Class<TPersistent> eventDataClass, MonitoringEventSubscriber<TPersistent> subscriber );

    /**
     * monitor event which will be queued immediately and get processed by a registered queue worker.
     *
     * @param eventData The event data object.
     * @param <TEvent> The type parameter for the event, which will implement MonitoringEventData
     */
    <TEvent extends MonitoringData> void monitorEvent( TEvent eventData );

    <TEvent extends MonitoringData, TPersistent extends MonitoringPersistentData>
    void monitorJob( MonitoringJob<TEvent, TPersistent> job );

    /**
     * For monitoring events and processing them, they need first to be registered.
     * A registration has always two type parameters for the event class type and
     * the persistent type.
     *
     * @param eventDataClass
     * @param monitoringJobClass
     * @param <TEvent>
     * @param <TPersistent>
     */
    <TEvent extends MonitoringData, TPersistent extends MonitoringPersistentData> void
    registerEventType( Class<TEvent> eventDataClass, Class<TPersistent> monitoringJobClass );

    /**
     * For monitoring events and processing them, they need first to be registered.
     * A registration has always two type parameters for the event class type and
     * the persistent type. Moreover, a worker for the data types need to be registered.
     *
     * @param eventDataClass
     * @param monitoringJobClass
     * @param worker
     * @param <TEvent>
     * @param <TPersistent>
     */
    <TEvent extends MonitoringData, TPersistent extends MonitoringPersistentData> void
    registerEventType( Class<TEvent> eventDataClass, Class<TPersistent> monitoringJobClass, MonitoringQueueWorker<TEvent, TPersistent> worker );

}

