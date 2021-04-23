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
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.monitoring.dtos.MonitoringData;
import org.polypheny.db.monitoring.dtos.MonitoringPersistentData;
import org.polypheny.db.monitoring.persistence.ReadOnlyMonitoringRepository;
import org.polypheny.db.monitoring.subscriber.MonitoringEventSubscriber;
import org.polypheny.db.monitoring.ui.MonitoringServiceUi;
import org.polypheny.db.util.Pair;

@Slf4j
public class MonitoringServiceImpl implements MonitoringService {

    // region private fields

    private final MonitoringQueue monitoringQueue;
    private final ReadOnlyMonitoringRepository readOnlyMonitoringRepository;
    private final MonitoringServiceUi monitoringServiceUi;

    private final List<Pair<Class, Class>> registeredMonitoringPair = new ArrayList<>();

    // endregion

    // region ctors


    public MonitoringServiceImpl(
            MonitoringQueue monitoringQueue,
            ReadOnlyMonitoringRepository readOnlyMonitoringRepository,
            MonitoringServiceUi monitoringServiceUi ) {
        if ( monitoringQueue == null ) {
            throw new IllegalArgumentException( "empty monitoring write queue service" );
        }

        if ( readOnlyMonitoringRepository == null ) {
            throw new IllegalArgumentException( "empty read-only repository" );
        }

        if ( monitoringServiceUi == null ) {
            throw new IllegalArgumentException( "empty monitoring ui service" );
        }

        this.monitoringQueue = monitoringQueue;
        this.readOnlyMonitoringRepository = readOnlyMonitoringRepository;
        this.monitoringServiceUi = monitoringServiceUi;
    }

    // endregion

    // region public methods


    @Override
    public void monitorEvent( MonitoringData eventData ) {
        if ( this.registeredMonitoringPair.stream().noneMatch( pair -> pair.left.isInstance( eventData ) ) ) {
            throw new IllegalArgumentException( "Event Class is not yet registered" );
        }

        this.monitoringQueue.queueEvent( eventData );
    }


    @Override
    public <TPersistent extends MonitoringPersistentData> void subscribeEvent( Class<TPersistent> eventDataClass, MonitoringEventSubscriber<TPersistent> subscriber ) {
        this.monitoringQueue.subscribeEvent( eventDataClass, subscriber );
    }


    @Override
    public <TPersistent extends MonitoringPersistentData> void unsubscribeEvent( Class<TPersistent> eventDataClass, MonitoringEventSubscriber<TPersistent> subscriber ) {
        this.monitoringQueue.unsubscribeEvent( eventDataClass, subscriber );
    }


    @Override
    public <TEvent extends MonitoringData, TPersistent extends MonitoringPersistentData> void
    registerEventType( Class<TEvent> eventDataClass, Class<TPersistent> eventPersistentDataClass ) {
        Pair<Class, Class> pair = new Pair( eventDataClass, eventPersistentDataClass );

        if ( eventDataClass != null && !this.registeredMonitoringPair.contains( pair ) ) {
            this.registeredMonitoringPair.add( pair );
        }
    }


    @Override
    public <TEvent extends MonitoringData, TPersistent extends MonitoringPersistentData> void
    registerEventType( Class<TEvent> eventDataClass, Class<TPersistent> eventPersistentDataClass, MonitoringQueueWorker<TEvent, TPersistent> worker ) {
        Pair<Class<TEvent>, Class<TPersistent>> pair = new Pair( eventDataClass, eventPersistentDataClass );

        if ( eventDataClass != null && !this.registeredMonitoringPair.contains( pair ) ) {
            this.registerEventType( eventDataClass, eventPersistentDataClass );
            this.monitoringQueue.registerQueueWorker( pair, worker );
            this.monitoringServiceUi.registerPersistentClass( eventPersistentDataClass );
        }
    }

    // endregion
}
