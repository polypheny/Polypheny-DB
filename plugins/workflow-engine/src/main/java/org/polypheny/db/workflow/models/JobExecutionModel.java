/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.models;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import lombok.Value;

@Value
public class JobExecutionModel {

    String message;
    Map<String, JsonNode> variables;

    // for convencience, some fields of the monitor are included as top level fields
    boolean success;
    String startTime; // ISO 8601:  "2025-01-17T14:30:00Z"
    ExecutionMonitorModel statistics;


    public JobExecutionModel( String message, Map<String, JsonNode> variables, ExecutionMonitorModel statistics ) {
        this.message = message;
        this.variables = variables;
        this.success = statistics.getIsSuccess();
        this.startTime = statistics.getStartTime();
        this.statistics = statistics;
    }

}
