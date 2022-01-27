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

package org.polypheny.db.monitoring.events;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.UUID;


/**
 * Marker interface for the persistent metric type, which can be monitored.
 * A MonitoringEvent will be analyzed and create metric objects.
 */
public interface MonitoringDataPoint extends Serializable {

    UUID id();

    Timestamp timestamp();

    DataPointType getDataPointType();

    boolean isCommitted();

    enum DataPointType {
        DML,
        DQL,
        DDL,
        QueryDataPointImpl,
        QueryPostCostImpl
    }

}
