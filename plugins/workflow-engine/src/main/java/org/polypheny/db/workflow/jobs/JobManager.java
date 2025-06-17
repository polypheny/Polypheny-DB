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
import io.javalin.http.HttpCode;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.workflow.dag.WorkflowImpl;
import org.polypheny.db.workflow.models.JobModel;
import org.polypheny.db.workflow.models.WorkflowModel;
import org.polypheny.db.workflow.repo.WorkflowRepo;
import org.polypheny.db.workflow.repo.WorkflowRepo.WorkflowRepoException;
import org.polypheny.db.workflow.repo.WorkflowRepoImpl;
import org.polypheny.db.workflow.session.JobSession;
import org.polypheny.db.workflow.session.SessionManager;

@Slf4j
public class JobManager {

    private static JobManager INSTANCE = null;
    private final WorkflowRepo repo = WorkflowRepoImpl.getInstance();
    private final SessionManager sessionManager = SessionManager.getInstance();
    private final Map<UUID, UUID> jobToSession = new ConcurrentHashMap<>(); // to be more consistent with the session model, we differ between jobId and sessionId


    private JobManager() {
    }


    public static JobManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new JobManager();
        }
        return INSTANCE;
    }


    public void onStartup() throws WorkflowRepoException {
        assert jobToSession.isEmpty() : "onStartup cannot be called when jobs have already been started";
        for ( JobModel model : repo.getJobs().values() ) {
            if ( model.isEnableOnStartup() ) {
                try {
                    enable( model.getJobId() );
                } catch ( Exception e ) {
                    log.error( "Failed to enable job {}", model, e );
                }
            }
        }
    }


    public Map<UUID, JobModel> getJobs() throws WorkflowRepoException {
        Map<UUID, JobModel> jobs = new HashMap<>();
        for ( JobModel model : repo.getJobs().values() ) {
            jobs.put( model.getJobId(), model.withSessionId( jobToSession.get( model.getJobId() ) ) );
        }
        return jobs;
    }


    public UUID setJob( JobModel model ) throws WorkflowJobException, WorkflowRepoException {
        UUID jobId = model.getJobId();
        if ( isEnabled( jobId ) ) {
            throw new WorkflowJobException( "Cannot modify an active job", HttpCode.CONFLICT );
        }
        if ( !repo.doesExist( model.getWorkflowId(), model.getVersion() ) ) {
            throw new WorkflowJobException( "The specified workflow version does not exist: " + model.getWorkflowId() + " v" + model.getVersion(), HttpCode.NOT_FOUND );
        }
        repo.setJob( JobTrigger.fromModel( model ).toModel() ); // this also validates the JobTrigger
        return jobId;
    }


    public void deleteJob( UUID jobId ) throws WorkflowJobException, WorkflowRepoException {
        if ( isEnabled( jobId ) ) {
            disable( jobId );
        }
        repo.removeJob( jobId );
    }


    public UUID enable( UUID jobId ) throws WorkflowJobException {
        if ( isEnabled( jobId ) ) {
            throw new WorkflowJobException( "Cannot enable already active job", HttpCode.CONFLICT );
        }

        try {
            JobTrigger trigger = JobTrigger.fromModel( repo.getJob( jobId ) );
            WorkflowModel model = repo.readVersion( trigger.getWorkfowId(), trigger.getVersion() );
            UUID sessionId = sessionManager.registerJobSession( WorkflowImpl.fromModel( model ), trigger );
            jobToSession.put( jobId, sessionId );
            return sessionId;
        } catch ( WorkflowRepoException e ) {
            throw new WorkflowJobException( e );
        } catch ( WorkflowJobException e ) {
            throw e;
        } catch ( Exception e ) {
            throw new WorkflowJobException( "Unable to instantiate workflow: " + e.getMessage() );
        }
    }


    public void disable( UUID jobId ) throws WorkflowJobException {
        if ( !isEnabled( jobId ) ) {
            throw new WorkflowJobException( "Cannot disable inactive job", HttpCode.CONFLICT );
        }
        sessionManager.terminateSession( jobToSession.get( jobId ) );
        jobToSession.remove( jobId );
    }


    public void trigger(
            UUID jobId, String message, @Nullable Map<String, JsonNode> variables,
            @Nullable Consumer<Boolean> onExecutionFinished ) throws Exception {
        if ( !isEnabled( jobId ) ) {
            throw new WorkflowJobException( "Only active jobs can be triggered", HttpCode.CONFLICT );
        }
        JobSession session = getSession( jobId );
        session.triggerExecution( message, variables, onExecutionFinished );

    }


    public void manuallyTrigger( UUID jobId ) throws WorkflowJobException {
        try {
            trigger( jobId, "Manually triggered", null, null );
        } catch ( WorkflowJobException e ) {
            throw e;
        } catch ( Exception e ) {
            throw new WorkflowJobException( "Unable to trigger job: " + e.getMessage() );
        }
    }


    public boolean isEnabled( UUID jobId ) {
        return jobToSession.containsKey( jobId );
    }


    private JobSession getSession( UUID jobId ) throws WorkflowJobException {
        UUID sessionId = jobToSession.get( jobId );
        if ( sessionId == null ) {
            throw new WorkflowJobException( "Session does not exist for job: " + jobId, HttpCode.CONFLICT );
        }
        return sessionManager.getJobSessionOrThrow( sessionId );
    }


    @Getter
    public static class WorkflowJobException extends Exception {

        private final HttpCode errorCode;


        public WorkflowJobException( String message, HttpCode errorCode ) {
            super( message );
            this.errorCode = errorCode;
        }


        public WorkflowJobException( String message ) {
            this( message, HttpCode.INTERNAL_SERVER_ERROR );
        }


        public WorkflowJobException( WorkflowRepoException e ) {
            this( e.getMessage(), e.getErrorCode() );
        }

    }

}
