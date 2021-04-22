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

import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.monitoring.persistence.MonitoringPersistentData;

import java.util.UUID;

@Getter
@Setter
public class MonitoringJob<TEvent extends MonitoringEventData, TPersistent extends MonitoringPersistentData> {
    private final UUID id = UUID.randomUUID();

    public MonitoringJob(TEvent eventData, TPersistent eventPersistentData) {
        this.eventData = eventData;
        this.persistentData = eventPersistentData;
    }

    public UUID Id() {
        return id;
    }

    public TEvent eventData;
    public TPersistent persistentData;
}
