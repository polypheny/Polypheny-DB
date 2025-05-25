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
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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
    final int maxRetries;
    final boolean performance; // if true, all performance improvements are enforced
    final Map<String, JsonNode> variables;

    private int retries = 0;
    private Map<String, JsonNode> lastVariables;
    private static final int RETRY_DELAY_MILLIS = 5000;
    private static final int RETRY_LIMIT = 10;


    public JobTrigger(
            UUID jobId, TriggerType type, UUID workfowId, int version, boolean enableOnStartup, String name,
            int maxRetries, boolean performance, Map<String, JsonNode> variables ) {
        this.jobId = jobId;
        this.type = type;
        this.workfowId = workfowId;
        this.version = version;
        this.enableOnStartup = enableOnStartup;
        this.name = name;
        this.maxRetries = Math.max( maxRetries, 0 );
        this.performance = performance;
        this.variables = variables;
        if ( maxRetries > RETRY_LIMIT ) {
            throw new IllegalArgumentException( "More than " + RETRY_LIMIT + " retries are not permitted" );
        }
    }


    public abstract void onEnable();

    public abstract void onDisable();


    public void onExecutionFinished( boolean isSuccess ) {
        if ( maxRetries > 0 ) {
            if ( isSuccess ) {
                retries = 0;
                lastVariables = null;
            } else if ( retries < maxRetries ) {
                retries++;
                try {
                    Thread.sleep( RETRY_DELAY_MILLIS );
                } catch ( InterruptedException e ) {
                    throw new RuntimeException( e );
                }
                try {
                    Map<String, JsonNode> vars = new HashMap<>( lastVariables );
                    vars.put( "job_retry", IntNode.valueOf( retries ) );
                    jobManager.trigger( jobId, "Retry " + retries, vars, this::onExecutionFinished );
                } catch ( Exception e ) {
                    log.warn( "Unable to trigger job " + jobId, e );
                }
            } else {
                retries = 0;
                lastVariables = null;
                log.warn( "Maximum retries reached without success." );
            }
        }
    }


    public abstract JobModel toModel();


    boolean trigger( String message ) {
        if ( retries > 0 ) {
            log.warn( "Execution skipped as a retry is in progress for job " + jobId );
            return false;
        }
        Map<String, JsonNode> vars = new HashMap<>( variables == null ? Map.of() : variables );
        vars.put( "job_trigger_time", TextNode.valueOf( DateTimeFormatter.ISO_INSTANT.format( Instant.now() ) ) );
        try {
            jobManager.trigger( jobId, message, vars, this::onExecutionFinished );
            this.lastVariables = vars;
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
