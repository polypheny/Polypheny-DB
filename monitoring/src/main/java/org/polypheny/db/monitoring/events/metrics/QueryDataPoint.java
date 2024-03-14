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

import java.io.Serial;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Builder;
import lombok.Value;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;


@Builder
@Value
public class QueryDataPoint implements MonitoringDataPoint, Serializable {

    @Serial
    private static final long serialVersionUID = 9063080671064507812L;

    List<String> tables = new ArrayList<>();
    HashMap<String, Object> dataElements = new HashMap<>();
    UUID Id;
    Timestamp recordedTimestamp;
    String monitoringType;
    String description;
    long executionTime;
    boolean isSubQuery;
    boolean isCommitted;
    int rowCount;
    List<String> fieldNames;
    Integer indexSize;
    Map<Long, Long> availableColumnsWithTable = new HashMap<>();


    @Override
    public UUID id() {
        return this.Id;
    }


    @Override
    public Timestamp timestamp() {
        return this.recordedTimestamp;
    }


    @Override
    public DataPointType getDataPointType() {
        return DataPointType.DQL;
    }

}
