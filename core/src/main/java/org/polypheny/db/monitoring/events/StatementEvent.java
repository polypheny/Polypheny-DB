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


import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.processing.LogicalRelQueryAnalyzeShuttle;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.transaction.Statement;


/**
 * Basis class needed for every statement type like, QUERY, DML, DDL
 */
@Setter
@Getter
public abstract class StatementEvent extends BaseEvent {

    protected RelRoot routed;
    protected PolyphenyDbSignature signature;
    protected Statement statement;
    protected List<List<Object>> rows;
    protected String description;
    protected List<String> fieldNames;
    protected long executionTime;
    protected int rowCount;
    protected boolean isAnalyze;
    protected boolean isSubQuery;
    protected String durations;
    protected Map<Long, List<Long>> accessedPartitions;
    protected LogicalRelQueryAnalyzeShuttle analyzeRelShuttle;
    protected String relCompareString;
    protected String queryId;



    @Override
    public abstract <T extends MonitoringDataPoint> List<Class<T>> getMetrics();


    @Override
    public <T extends MonitoringDataPoint> List<Class<T>> getOptionalMetrics() {
        return Collections.emptyList();
    }


    @Override
    public abstract List<MonitoringDataPoint> analyze();

}
