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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.transaction.Statement;

@Getter
@Setter
@NoArgsConstructor
public class QueryEvent extends BaseEvent implements MonitoringEvent {

    private String monitoringType;
    private RelRoot routed;
    private PolyphenyDbSignature signature;
    private Statement statement;
    private List<List<Object>> rows;
    private String description;
    private List<String> fieldNames;
    private long recordedTimestamp;
    private long executionTime;
    private int rowCount;
    private boolean isAnalyze;
    private boolean isSubQuery;
    private String durations;


    @Override
    public UUID id() {
        return super.getId();
    }


    @Override
    public Timestamp timestamp() {
        return new Timestamp( recordedTimestamp );
    }


    @Override
    public <T extends MonitoringMetric> List<Class<T>> getMetrics() {
        return Arrays.asList( (Class<T>) QueryMetric.class );
    }


    @Override
    public <T extends MonitoringMetric> List<Class<T>> getOptionalMetrics() {
        return Collections.emptyList();
    }


    @Override
    public List<MonitoringMetric> analyze() {
        return Arrays.asList( QueryEventAnalyzer.analyze( this ) );
    }

}
