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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.MonitoringType;


@Getter
@Setter
@Builder
@NoArgsConstructor(access = AccessLevel.PUBLIC)
@AllArgsConstructor(access = AccessLevel.MODULE)
public class DmlDataPoint implements MonitoringDataPoint, Serializable {

    private static final long serialVersionUID = 8159995420459385039L;

    @Builder.Default
    private final Set<Long> tables = new HashSet<>();
    private final Map<String, Object> dataElements = new HashMap<>();
    private UUID Id;
    private Timestamp recordedTimestamp;
    private MonitoringType monitoringType;
    private String description;
    private long executionTime;
    private boolean isSubQuery;
    protected boolean isCommitted;
    private long rowCount;
    private List<String> fieldNames;
    private List<Long> accessedPartitions;
    private String queryClass;
    private String physicalQueryClass;
    @Builder.Default
    private final Map<Long, List<?>> changedValues = new HashMap<>();
    @Builder.Default
    private final Map<Long, Long> availableColumnsWithTable = new HashMap<>();


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
        return DataPointType.DML;
    }

}
