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

package org.polypheny.db.monitoring.ui;

import org.polypheny.db.monitoring.events.MonitoringDataPoint;

/**
 * UI abstraction service for monitoring.
 */
public interface MonitoringServiceUi {

    void initializeInformationPage();

    /**
     * Will add new section to monitoring information page for the specified
     * MonitoringPersistentData type and register the refresh function to read from repository.
     *
     * @param metricClass
     * @param <T>
     */
    <T extends MonitoringDataPoint> void registerDataPointForUi( Class<T> metricClass );


}
