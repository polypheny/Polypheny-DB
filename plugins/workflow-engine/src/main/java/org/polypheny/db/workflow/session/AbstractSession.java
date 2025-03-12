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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.websocket.WsMessageContext;
import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Triple;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.engine.monitoring.ExecutionMonitor;
import org.polypheny.db.workflow.engine.scheduler.GlobalScheduler;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.StorageManagerImpl;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.ExecutionMonitorModel;
import org.polypheny.db.workflow.models.SessionModel;
import org.polypheny.db.workflow.models.SessionModel.SessionModelType;
import org.polypheny.db.workflow.models.WorkflowConfigModel;
import org.polypheny.db.workflow.models.WorkflowModel;
import org.polypheny.db.workflow.models.requests.WsRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.CloneActivityRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.CreateActivityRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.CreateEdgeRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.DeleteActivityRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.DeleteEdgeRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.ExecuteRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.GetCheckpointRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.InterruptRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.ResetRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.UpdateActivityRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.UpdateConfigRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.UpdateVariablesRequest;
import org.polypheny.db.workflow.models.responses.ActivityStatsResponse;
import org.polypheny.db.workflow.models.responses.WsResponse;
import org.polypheny.db.workflow.models.responses.WsResponse.ResponseType;

@Slf4j
public abstract class AbstractSession {

    @Getter
    final SessionModelType type;
    final Workflow workflow;
    @Getter
    final UUID sessionId;
    final StorageManager sm;
    final ObjectMapper mapper = new ObjectMapper();
    private final Set<Session> subscribers = new HashSet<>();
    final GlobalScheduler scheduler;
    final NestedSessionManager nestedManager;

    ExecutionMonitor executionMonitor; // corresponds to the last started execution
    Instant lastInteraction = Instant.now();
    private boolean terminationFailed;


    protected AbstractSession( SessionModelType type, Workflow workflow, UUID sessionId ) {
        this( type, workflow, sessionId, null, 0, null );
    }


    protected AbstractSession( SessionModelType type, Workflow workflow, UUID sessionId, @Nullable UUID workflowId, int version, @Nullable Set<Pair<UUID, Integer>> parentWorkflowIds ) {
        this.type = type;
        this.workflow = workflow;
        this.sessionId = sessionId;
        this.sm = new StorageManagerImpl( sessionId, workflow.getConfig().getPreferredStores() );
        this.scheduler = GlobalScheduler.getInstance();

        Set<Pair<UUID, Integer>> workflowIds = parentWorkflowIds == null ? new HashSet<>() : new HashSet<>( parentWorkflowIds );
        if ( workflowId != null ) {
            workflowIds.add( Pair.of( workflowId, version ) );
        }
        this.nestedManager = new NestedSessionManager( workflowIds, this.type == SessionModelType.NESTED_SESSION );
    }


    public void terminate() {
        nestedManager.terminateAll();
        if ( workflow.getState() == WorkflowState.EXECUTING ) {
            scheduler.interruptExecution( sessionId );
        }
        try {
            if ( terminationFailed ) {
                log.warn( "Graceful termination of session {} failed. Rolling back transactions...", sessionId );
                scheduler.forceInterruptExecution( sessionId );
                sm.close();
            }
            if ( scheduler.awaitExecutionFinish( sessionId, 30 ) ) {
                sm.close();
            } else {
                terminationFailed = true;
                throw new GenericRuntimeException( "Timed out waiting for execution to finish. Try again to forcefully shut down the session." );
            }
        } catch ( GenericRuntimeException e ) {
            throw e;
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
    }


    /**
     * Subscribe to any updates.
     *
     * @param session the UI websocket session to be registered
     */
    public void subscribe( Session session ) {
        subscribers.add( session );
    }


    public void unsubscribe( Session session ) {
        subscribers.remove( session );
    }


    public int getSubscriberCount() {
        return subscribers.size();
    }


    public abstract SessionModel toModel();


    public ActivityModel getActivityModel( UUID activityId, boolean includeState ) {
        return workflow.getActivity( activityId ).toModel( includeState );
    }


    public ActivityStatsResponse getActivityStats( UUID activityId ) {
        return new ActivityStatsResponse( workflow.getActivity( activityId ) );
    }


    public WorkflowModel getWorkflowModel( boolean includeState ) {
        return workflow.toModel( includeState );
    }


    public WorkflowConfigModel getWorkflowConfig() {
        return workflow.getConfig();
    }


    public Map<String, JsonNode> getVariables() {
        return workflow.getVariables();
    }


    public ExecutionMonitorModel getMonitorModel() {
        return executionMonitor == null ? null : executionMonitor.toModel();
    }


    public void updateLastInteraction() {
        this.lastInteraction = Instant.now();
    }


    void broadcastMessage( WsResponse msg ) {
        try {
            String json = mapper.writeValueAsString( msg );
            if ( msg.type != ResponseType.PROGRESS_UPDATE ) {
                //log.info( "Broadcasting message: " + json );
            }
            for ( Session subscriber : subscribers ) {
                try {
                    subscriber.getRemote().sendString( json );
                } catch ( IOException e ) {
                    subscriber.close();
                }
            }
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        }
    }


    void startExecution( @Nullable UUID targetActivity ) {
        throwIfNotIdle();
        if ( targetActivity != null ) {
            workflow.reset( targetActivity, sm );
        }
        try {
            executionMonitor = GlobalScheduler.getInstance().startExecution( workflow, sm, nestedManager, targetActivity, this::broadcastMessage );
        } catch ( Exception e ) {
            throw new IllegalStateException( "Unable to start workflow execution", e );
        }
    }


    Triple<Result<?, ?>, Integer, Long> getCheckpointData( UUID activityId, int outputIndex, @Nullable Integer maxTuples ) {
        ActivityWrapper wrapper = workflow.getActivityOrThrow( activityId );
        if ( wrapper.getState() != ActivityState.SAVED ) {
            throw new IllegalStateException( "Only checkpoints of saved activities can be requested" );
        }

        if ( !sm.hasCheckpoint( activityId, outputIndex ) ) {
            throw new GenericRuntimeException( "The specified checkpoint does not exist" );
        }

        try ( CheckpointReader reader = sm.readCheckpoint( activityId, outputIndex ) ) {
            return reader.getPreview( maxTuples );
        }
    }


    void interruptExecution() {
        throwIfNotExecuting();
        scheduler.interruptExecution( sessionId );
    }


    public SessionModel getNestedModelOrNull( UUID activityId ) {
        NestedSession nested = nestedManager.getSessionForActivity( activityId );
        if ( nested == null ) {
            return null;
        }
        return nested.toModel();
    }


    public void handleRequest( CreateActivityRequest request ) {
        throwUnsupported( request );
    }


    public void handleRequest( DeleteActivityRequest request ) {
        throwUnsupported( request );
    }


    public void handleRequest( UpdateActivityRequest request ) {
        throwUnsupported( request );
    }


    public void handleRequest( CloneActivityRequest request ) {
        throwUnsupported( request );
    }


    public void handleRequest( CreateEdgeRequest request ) {
        throwUnsupported( request );
    }


    public void handleRequest( DeleteEdgeRequest request ) {
        throwUnsupported( request );
    }


    public void handleRequest( ExecuteRequest request ) {
        throwUnsupported( request );
    }


    public void handleRequest( InterruptRequest request ) {
        throwUnsupported( request );
    }


    public void handleRequest( ResetRequest request ) {
        throwUnsupported( request );
    }


    public void handleRequest( UpdateConfigRequest request ) {
        throwUnsupported( request );
    }


    public void handleRequest( UpdateVariablesRequest request ) {
        throwUnsupported( request );
    }


    public void handleRequest( GetCheckpointRequest request, WsMessageContext ctx ) {
        throwUnsupported( request );
    }


    void throwIfNotIdle() {
        if ( workflow.getState() != WorkflowState.IDLE ) {
            throw new IllegalStateException( "Workflow is currently not in an idle state." );
        }
    }


    void throwIfNotExecuting() {
        if ( workflow.getState() != WorkflowState.EXECUTING ) {
            throw new IllegalStateException( "Workflow is currently not being executed." );
        }
    }


    void throwUnsupported( WsRequest request ) {
        throw new UnsupportedOperationException( "This session type does not support " + request.type + " requests." );
    }


    public WorkflowState getWorkflowState() {
        return workflow.getState();
    }

}
