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

package org.polypheny.db.monitoring.events;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;


/**
 * Marker interface for the data type, which can be monitored.
 * A MonitoringData implementation should always have a corresponding
 * MonitoringPersistentData implementation.
 */
public interface MonitoringEvent {

    UUID getId();

    Timestamp getRecordedTimestamp();

    /**
     * Defined Class Types which will be generated from the event.
     * The analyze method will create the list of metrics.
     *
     * @return List of all mandatory metrics associated with the Event
     */
    <T extends MonitoringDataPoint> List<Class<T>> getMetrics();

    /**
     * Defined Class Types which will optionally be generated from the event.
     * The analyze method will attach the optional metrics.
     *
     * @return List of all optional metrics associated with the Event
     */
    <T extends MonitoringDataPoint> List<Class<T>> getOptionalMetrics();

    /**
     * The analyze method will analyze the Monitoring Event and create metric out of the data.
     *
     * @return The generates metrics.
     */
    List<MonitoringDataPoint> analyze();

}
