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
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import lombok.Value;

@Value
public class JobExecutionModel {

    String message;
    Map<String, JsonNode> variables;

    JobResult result;
    String startTime; // ISO 8601:  "2025-01-17T14:30:00Z"
    ExecutionMonitorModel statistics;


    public JobExecutionModel( JobResult result, String message, Map<String, JsonNode> variables, ExecutionMonitorModel statistics ) {
        this.message = message;
        this.variables = variables;
        this.result = result;
        this.startTime = statistics.getStartTime();
        this.statistics = statistics;
    }


    public JobExecutionModel( String originalMessage, String skipReason ) {
        this.message = originalMessage + " (" + skipReason + ")";
        this.variables = null;
        this.result = JobResult.SKIPPED;
        this.startTime = DateTimeFormatter.ISO_INSTANT.format( Instant.now() );
        this.statistics = null;
    }


    public enum JobResult {
        SUCCESS,
        FAILED, // executed, but not successfully
        SKIPPED // execution could not be started
    }

}
