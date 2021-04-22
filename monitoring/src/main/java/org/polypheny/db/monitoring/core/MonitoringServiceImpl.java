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

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.monitoring.Ui.MonitoringServiceUi;
import org.polypheny.db.monitoring.dtos.MonitoringEventData;
import org.polypheny.db.monitoring.persistence.MonitoringPersistentData;
import org.polypheny.db.monitoring.persistence.ReadOnlyMonitoringRepository;
import org.polypheny.db.monitoring.subscriber.MonitoringEventSubscriber;
import org.polypheny.db.util.Pair;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MonitoringServiceImpl implements MonitoringService {
    private MonitoringQueue monitoringQueue;
    private ReadOnlyMonitoringRepository readOnlyMonitoringRepository;
    private MonitoringServiceUi monitoringServiceUi;


    private final List<Pair<Class, Class>> registeredMonitoringPair = new ArrayList<>();


    public MonitoringServiceImpl(
            MonitoringQueue monitoringQueue,
            ReadOnlyMonitoringRepository readOnlyMonitoringRepository,
            MonitoringServiceUi monitoringServiceUi) {
        if (monitoringQueue == null)
            throw new IllegalArgumentException("empty monitoring write queue service");

        if (readOnlyMonitoringRepository == null)
            throw new IllegalArgumentException("empty read-only repository");

        if (monitoringServiceUi == null)
            throw new IllegalArgumentException("empty monitoring ui service");

        this.monitoringQueue = monitoringQueue;
        this.readOnlyMonitoringRepository = readOnlyMonitoringRepository;
        this.monitoringServiceUi = monitoringServiceUi;
    }

    @Override
    public void monitorEvent(MonitoringEventData eventData) {
        if (!this.registeredMonitoringPair.stream().anyMatch(pair -> pair.left.isInstance(eventData))) {
            throw new IllegalArgumentException("Event Class is not yet registered");
        }

        this.monitoringQueue.queueEvent(eventData);
    }

    @Override
    public <T extends MonitoringEventData> void subscribeEvent(Class<T> eventDataClass, MonitoringEventSubscriber<T> subscriber) {

    }

    @Override
    public <T extends MonitoringEventData> void unsubscribeEvent(Class<T> eventDataClass, MonitoringEventSubscriber<T> subscriber) {

    }

    @Override
    public <TEvent extends MonitoringEventData, TPersistent extends MonitoringPersistentData> void
    registerEventType(Class<TEvent> eventDataClass, Class<TPersistent> eventPersistentDataClass) {
        Pair<Class, Class> pair = new Pair(eventDataClass, eventPersistentDataClass);

        if (eventDataClass != null && !this.registeredMonitoringPair.contains(pair)) {
            this.registeredMonitoringPair.add(pair);
        }
    }

    @Override
    public <TEvent extends MonitoringEventData, TPersistent extends MonitoringPersistentData> void
    registerEventType(Class<TEvent> eventDataClass, Class<TPersistent> eventPersistentDataClass, MonitoringQueueWorker<TEvent, TPersistent> consumer) {
        Pair<Class<TEvent>, Class<TPersistent>> pair = new Pair(eventDataClass, eventPersistentDataClass);

        if (eventDataClass != null && !this.registeredMonitoringPair.contains(pair)) {
            this.registerEventType(eventDataClass, eventPersistentDataClass);
            this.monitoringQueue.registerQueueWorker(pair, consumer);
            this.monitoringServiceUi.registerPersistentClass(eventPersistentDataClass);
        }
    }


}
