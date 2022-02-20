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

import java.util.HashMap;
import java.util.List;
import org.polypheny.db.monitoring.events.MonitoringEvent;


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

    /**
     * Display current number of elements in queue
     *
     * @return Current number of elements in Queue
     */
    long getNumberOfElementsInQueue();

    List<HashMap<String, String>> getInformationOnElementsInQueue();

    long getNumberOfProcessedEvents();

}
