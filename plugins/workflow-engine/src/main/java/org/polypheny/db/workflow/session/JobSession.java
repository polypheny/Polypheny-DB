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

package org.polypheny.db.workflow.session;

import static org.polypheny.db.workflow.models.SessionModel.SessionModelType.JOB_SESSION;

import com.fasterxml.jackson.databind.JsonNode;
import io.javalin.http.HttpCode;
import io.javalin.websocket.WsMessageContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Triple;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.jobs.JobManager.WorkflowJobException;
import org.polypheny.db.workflow.jobs.JobTrigger;
import org.polypheny.db.workflow.models.JobExecutionModel;
import org.polypheny.db.workflow.models.SessionModel;
import org.polypheny.db.workflow.models.WorkflowDefModel;
import org.polypheny.db.workflow.models.requests.WsRequest.GetCheckpointRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.UpdateActivityRequest;
import org.polypheny.db.workflow.models.responses.WsResponse.CheckpointDataResponse;
import org.polypheny.db.workflow.models.responses.WsResponse.RenderingUpdateResponse;
import org.polypheny.db.workflow.models.responses.WsResponse.StateUpdateResponse;

public class JobSession extends AbstractSession {

    private static final int MAX_HISTORY_SIZE = 50;
    @Getter
    private final JobTrigger trigger;
    private final WorkflowDefModel workflowDef;
    private final List<JobExecutionModel> executionHistory = new ArrayList<>();
    private String currentMessage;
    private Map<String, JsonNode> currentVariables;


    public JobSession( UUID sessionId, Workflow wf, JobTrigger trigger, WorkflowDefModel workflowDef ) {
        super( JOB_SESSION, wf, sessionId, trigger.getWorkfowId(), trigger.getVersion(), null );
        this.trigger = trigger;
        this.workflowDef = workflowDef;
        this.trigger.onEnable();
    }


    @Override
    public void handleRequest( GetCheckpointRequest request, WsMessageContext ctx ) {
        Triple<Result<?, ?>, Integer, Long> preview = getCheckpointData( request.activityId, request.outputIndex, null );
        ctx.send( new CheckpointDataResponse( request.msgId, preview.left, preview.middle, preview.right ) ); // we do NOT broadcast the result
    }


    @Override
    public void handleRequest( UpdateActivityRequest request ) {
        throwIfNotIdle();

        if ( request.rendering != null && request.settings == null && request.config == null ) {
            ActivityWrapper activity = workflow.updateActivity( request.targetId, null, null, request.rendering, sm );
            broadcastMessage( new RenderingUpdateResponse( request.msgId, activity ) );
        } else {
            throw new GenericRuntimeException( "Only rendering updates are permitted in API Sessions" );
        }
    }


    public synchronized void triggerExecution( String message, @Nullable Map<String, JsonNode> variables ) throws Exception {
        Objects.requireNonNull( message );
        if ( workflow.getState() != WorkflowState.IDLE ) {
            throw new WorkflowJobException( "Workflow is currently not idle: " + workflow.getState(), HttpCode.CONFLICT );
        }
        reset( null );
        if ( variables != null ) {
            Map<String, JsonNode> workflowVars = new HashMap<>( workflow.getVariables() );
            workflowVars.putAll( variables );
            workflow.updateVariables( workflowVars );
        }
        startExecution( null );
        currentMessage = message;
        currentVariables = variables;
    }


    public void interrupt() throws WorkflowJobException {
        if ( workflow.getState() != WorkflowState.EXECUTING ) {
            throw new WorkflowJobException( "Workflow is currently not being executed: " + workflow.getState(), HttpCode.CONFLICT );
        }
        interruptExecution();
    }


    public void reset( @Nullable UUID targetId ) throws WorkflowJobException {
        if ( workflow.getState() != WorkflowState.IDLE ) {
            throw new WorkflowJobException( "Workflow is currently not idle: " + workflow.getState(), HttpCode.CONFLICT );
        }
        workflow.reset( targetId, sm );
        broadcastMessage( new StateUpdateResponse( null, workflow ) );
    }


    @Override
    public void terminate() {
        super.terminate();
        executionHistory.clear();
        trigger.onDisable();
    }


    private void updateHistory() {
        if ( currentMessage != null && workflow.getState() == WorkflowState.IDLE ) {
            executionHistory.add( new JobExecutionModel( currentMessage, currentVariables, executionMonitor.toModel() ) );
            if ( executionHistory.size() > MAX_HISTORY_SIZE ) {
                executionHistory.remove( 0 );
            }
            currentMessage = null;
            currentVariables = null;
        }
    }


    @Override
    public SessionModel toModel() {
        updateHistory();
        return new SessionModel( getType(), sessionId, getSubscriberCount(),
                lastInteraction.toString(), workflow.getActivityCount(), workflow.getState(),
                trigger.getWorkfowId(), trigger.getVersion(), workflowDef,
                executionHistory, trigger.getJobId() );
    }

}
