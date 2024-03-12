/*
 * Copyright 2019-2024 The Polypheny Project
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
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;


public abstract class BaseEvent implements MonitoringEvent {

    @Getter
    private final UUID id = UUID.randomUUID();
    @Setter
    protected String eventType;

    private final long recordedTimestamp;


    public BaseEvent() {
        setEventType( eventType );
        recordedTimestamp = getCurrentTimestamp();
    }


    @Override
    public Timestamp getRecordedTimestamp() {
        return new Timestamp( recordedTimestamp );
    }


    private long getCurrentTimestamp() {
        return System.currentTimeMillis();
    }

}
