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

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.polypheny.db.workflow.jobs.JobTrigger.TriggerType;

@Value
@AllArgsConstructor
public class JobModel {

    UUID jobId;
    TriggerType type;
    UUID workflowId;
    int version;
    boolean enableOnStartup;
    String name;


    @JsonInclude(JsonInclude.Include.NON_NULL)
    UUID sessionId; // null if job is not enabled

    // SCHEDULED
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String schedule;


    public JobModel( UUID jobId, TriggerType type, UUID workflowId, int version, boolean enableOnStartup, String name, String schedule ) {
        this( jobId, type, workflowId, version, enableOnStartup, name, null, schedule );
    }


    public JobModel withSessionId( UUID sessionId ) {
        return new JobModel( jobId, type, workflowId, version, enableOnStartup, name, sessionId, schedule );
    }

}
