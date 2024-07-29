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

package org.polypheny.db.monitoring.events.metrics;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.UUID;
import lombok.Builder;
import lombok.Value;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.MonitoringType;


@Builder
@Value
public class DdlDataPoint implements MonitoringDataPoint, Serializable {

    private static final long serialVersionUID = 268576586444646401L;
    UUID Id;
    Timestamp recordedTimestamp;
    boolean isCommitted;
    long tableId;
    MonitoringType monitoringType;
    long namespaceId;
    long columnId;


    @Override
    public UUID id() {
        return Id;
    }


    @Override
    public Timestamp timestamp() {
        return recordedTimestamp;
    }


    @Override
    public DataPointType getDataPointType() {
        return DataPointType.DDL;
    }


}
