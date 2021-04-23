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

package org.polypheny.db.monitoring.dtos;

import java.util.UUID;
import lombok.Getter;
import lombok.Setter;


/**
 * The generic MonitoringJob which has two generic parameters and corresponding fields with getter and setter.
 *
 * @param <TEvent> The jobs monitoring data which will be processed to MonitoringPersistentData.
 * @param <TPersistent> the jobs persistent data.
 */
public class MonitoringJob<TEvent extends MonitoringData, TPersistent extends MonitoringPersistentData> {

    @Getter
    private final UUID id = UUID.randomUUID();
    @Getter
    private final long timestamp = System.currentTimeMillis();
    @Getter
    @Setter
    private TEvent monitoringData;
    @Getter
    @Setter
    private TPersistent monitoringPersistentData;


    public MonitoringJob( TEvent monitoringData, TPersistent eventPersistentData ) {
        this.monitoringData = monitoringData;
        this.monitoringPersistentData = eventPersistentData;
    }


    public MonitoringJob() {
    }

}
