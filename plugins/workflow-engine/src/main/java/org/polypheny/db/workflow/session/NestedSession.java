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

package org.polypheny.db.workflow.session;

import static org.polypheny.db.workflow.models.SessionModel.SessionModelType.NESTED_SESSION;

import com.fasterxml.jackson.databind.JsonNode;
import io.javalin.websocket.WsMessageContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Triple;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.dag.activities.impl.special.NestedInputActivity;
import org.polypheny.db.workflow.dag.activities.impl.special.NestedOutputActivity;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.variables.ReadableVariableStore;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.models.SessionModel;
import org.polypheny.db.workflow.models.WorkflowDefModel;
import org.polypheny.db.workflow.models.requests.WsRequest.GetCheckpointRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.UpdateActivityRequest;
import org.polypheny.db.workflow.models.responses.WsResponse.CheckpointDataResponse;
import org.polypheny.db.workflow.models.responses.WsResponse.RenderingUpdateResponse;
import org.polypheny.db.workflow.models.responses.WsResponse.StateUpdateResponse;

public class NestedSession extends AbstractSession {

    @Getter
    private final UUID wId;
    @Getter
    private final int openedVersion;
    private final WorkflowDefModel workflowDef;
    private final Map<String, JsonNode> initialWorkflowVars;


    public NestedSession( UUID sessionId, Workflow wf, UUID workflowId, int openedVersion, WorkflowDefModel workflowDef, Set<Pair<UUID, Integer>> parentWorkflowIds ) {
        super( NESTED_SESSION, wf, sessionId, workflowId, openedVersion, parentWorkflowIds );
        assert !parentWorkflowIds.contains( Pair.of( workflowId, openedVersion ) ) : "Detected cycle in nested workflows.";
        this.wId = workflowId;
        this.openedVersion = openedVersion;
        this.workflowDef = workflowDef;
        this.initialWorkflowVars = wf.getVariables();
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


    public void linkInputs( List<CheckpointReader> readers ) {
        Set<ActivityWrapper> activities = workflow.getActivities().stream().filter( w -> w.getActivity() instanceof NestedInputActivity ).collect( Collectors.toSet() );
        if ( activities.size() != readers.stream().filter( Objects::nonNull ).count() ) {
            throw new GenericRuntimeException( "Number of input activities in nested workflow and connected inputs do not match." );
        }
        for ( ActivityWrapper wrapper : activities ) {
            if ( !wrapper.getSettingsPreview().keysPresent( NestedInputActivity.INDEX_KEY ) ) {
                throw new GenericRuntimeException( "Index for nested input must be statically defined." );
            }
            int i = wrapper.getSettingsPreview().getInt( NestedInputActivity.INDEX_KEY );
            if ( i >= readers.size() ) {
                throw new GenericRuntimeException( "Detected non-consecutive index: " + i );
            }
            nestedManager.setInput( wrapper.getId(), readers.get( i ) );
        }
    }


    public void execute( ReadableVariableStore variables ) {
        Map<String, JsonNode> workflowVars = new HashMap<>( workflow.getVariables() );
        workflowVars.putAll( variables.getWorkflowVariables() );
        workflow.updateVariables( workflowVars );
        nestedManager.setInDynamicVars( variables.getDynamicVariables() );
        nestedManager.setInEnvVars( variables.getEnvVariables() );
        startExecution( null );
    }


    public double getProgress() {
        if ( getWorkflowState() != WorkflowState.EXECUTING ) {
            return 1;
        }
        double sum = 0;
        for ( ActivityWrapper wrapper : workflow.getActivities() ) {
            ActivityState state = wrapper.getState();
            if ( state.isExecuted() || state == ActivityState.SKIPPED ) {
                sum += 1;
            } else if ( state == ActivityState.EXECUTING ) {
                sum += executionMonitor.getProgress( wrapper.getId() );
            }
        }
        int count = workflow.getActivityCount();
        if ( count == 0 ) {
            return 1;
        }
        return sum / count;
    }


    public int getExitCode() {
        throwIfNotIdle();
        return executionMonitor.isOverallSuccess() ? 0 : 1;
    }


    public List<CheckpointReader> getOutputs() {
        ActivityWrapper wrapper = workflow.getActivities().stream().filter( w -> w.getActivity() instanceof NestedOutputActivity )
                .findFirst().orElse( null );
        if ( wrapper == null ) {
            return List.of();
        }
        if ( !wrapper.getState().isSuccess() ) {
            assert getExitCode() != 0;
            throw new GenericRuntimeException( "Output was not successfully executed" );
        }

        Set<DataEdge> dataEdges = workflow.getInEdges( wrapper.getId() ).stream().filter( e -> e instanceof DataEdge )
                .map( e -> (DataEdge) e ).collect( Collectors.toSet() );
        CheckpointReader[] arr = new CheckpointReader[NestedOutputActivity.MAX_OUTPUTS];
        for ( DataEdge edge : dataEdges ) {
            arr[edge.getToPort()] = sm.readCheckpoint( edge.getFrom().getId(), edge.getFromPort() ); // reader needs to be closed by caller
        }
        return Arrays.asList( arr );
    }


    public Map<String, JsonNode> getDynamicOutputVariables() {
        return Objects.requireNonNullElse( nestedManager.getOutDynamicVars(), Map.of() );
    }


    public void interrupt() {
        // unlike other sessions, we don't fail if already finished
        if ( getWorkflowState() == WorkflowState.EXECUTING ) {
            scheduler.interruptExecution( sessionId );
        }
    }


    public void reset() {
        workflow.reset( null, sm );
        workflow.updateVariables( initialWorkflowVars );
        nestedManager.clearInputs();
        broadcastMessage( new StateUpdateResponse( null, workflow ) );
    }


    public boolean isFor( UUID workflowId, int version ) {
        return this.wId.equals( workflowId ) && this.openedVersion == version;
    }


    @Override
    public SessionModel toModel() {
        return new SessionModel( getType(), sessionId, getSubscriberCount(), lastInteraction.toString(),
                workflow.getActivityCount(), workflow.getState(), wId, openedVersion, workflowDef );
    }

}
