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

import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.transaction.Statement;

@Getter
@Setter
@NoArgsConstructor
public class QueryData implements MonitoringData {

    public String monitoringType;
    public RelRoot routed;
    public PolyphenyDbSignature signature;
    public Statement statement;
    public List<List<Object>> rows;
    private String description;
    private List<String> fieldNames;
    private long recordedTimestamp;
    private long executionTime;
    private int rowCount;
    private boolean isAnalyze;
    private boolean isSubQuery;
    private String durations;

}
