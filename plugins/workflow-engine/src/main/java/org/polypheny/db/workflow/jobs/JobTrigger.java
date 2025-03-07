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

package org.polypheny.db.workflow.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.workflow.jobs.JobManager.WorkflowJobException;
import org.polypheny.db.workflow.models.JobModel;

@Slf4j
@Getter
public abstract class JobTrigger {

    private static final JobManager jobManager = JobManager.getInstance();
    final UUID jobId;
    final TriggerType type;
    final UUID workfowId;
    final int version; // workflow version
    final boolean enableOnStartup;
    final String name;


    public JobTrigger( UUID jobId, TriggerType type, UUID workfowId, int version, boolean enableOnStartup, String name ) {
        this.jobId = jobId;
        this.type = type;
        this.workfowId = workfowId;
        this.version = version;
        this.enableOnStartup = enableOnStartup;
        this.name = name;
    }


    public abstract void onEnable();

    public abstract void onDisable();

    public abstract JobModel toModel();


    boolean trigger( String message, @Nullable Map<String, JsonNode> variables ) {
        // call jobmanager
        try {
            jobManager.trigger( jobId, message, variables );
        } catch ( Exception e ) {
            log.warn( "Unable to trigger job " + jobId, e );
            return false;
        }
        return true;
    }


    public static JobTrigger fromModel( JobModel model ) throws WorkflowJobException {
        return switch ( model.getType() ) {
            case SCHEDULED -> new ScheduledJob( model );
            default -> throw new NotImplementedException( "Unsupported job type: " + model.getType() );
        };
    }


    public enum TriggerType {
        SCHEDULED
    }

}
