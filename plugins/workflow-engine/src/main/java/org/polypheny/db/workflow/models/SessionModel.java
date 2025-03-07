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

package org.polypheny.db.workflow.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;

@Value
@AllArgsConstructor
public class SessionModel {

    SessionModelType type;
    UUID sessionId;
    int connectionCount;
    String lastInteraction; // ISO 8601:  "2025-01-17T14:30:00Z"
    int activityCount;
    WorkflowState state;

    // USER_SESSION & JOB_SESSION fields:
    @JsonInclude(JsonInclude.Include.NON_NULL)
    UUID workflowId;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Integer version;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    WorkflowDefModel workflowDef;

    // JOB_SESSION fields:
    @JsonInclude(JsonInclude.Include.NON_NULL)
    List<JobExecutionModel> executionHistory;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    UUID jobId;


    public SessionModel( SessionModelType type, UUID sId, int connectionCount, String lastInteraction, int activityCount, WorkflowState state ) {
        // API_SESSION constructor
        this.type = type;
        this.sessionId = sId;
        this.connectionCount = connectionCount;
        this.lastInteraction = lastInteraction;
        this.activityCount = activityCount;
        this.state = state;

        this.workflowId = null;
        this.version = null;
        this.workflowDef = null;
        this.executionHistory = null;
        this.jobId = null;
    }


    public SessionModel( SessionModelType type, UUID sessionId, int connectionCount, String lastInteraction, int activityCount, WorkflowState state, UUID workflowId, Integer version, WorkflowDefModel workflowDef ) {
        this.type = type;
        this.sessionId = sessionId;
        this.connectionCount = connectionCount;
        this.lastInteraction = lastInteraction;
        this.activityCount = activityCount;
        this.state = state;
        this.workflowId = workflowId;
        this.version = version;
        this.workflowDef = workflowDef;

        this.executionHistory = null;
        this.jobId = null;
    }


    public enum SessionModelType {
        USER_SESSION,
        API_SESSION,
        NESTED_SESSION,
        JOB_SESSION
    }

}
