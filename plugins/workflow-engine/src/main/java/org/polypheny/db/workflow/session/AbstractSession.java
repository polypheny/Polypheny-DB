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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.engine.monitoring.ExecutionMonitor;
import org.polypheny.db.workflow.engine.scheduler.GlobalScheduler;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.StorageManagerImpl;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.SessionModel;
import org.polypheny.db.workflow.models.WorkflowConfigModel;
import org.polypheny.db.workflow.models.WorkflowModel;
import org.polypheny.db.workflow.models.requests.WsRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.CreateActivityRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.CreateEdgeRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.DeleteActivityRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.DeleteEdgeRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.ExecuteRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.InterruptRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.ResetRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.UpdateActivityRequest;
import org.polypheny.db.workflow.models.requests.WsRequest.UpdateConfigRequest;
import org.polypheny.db.workflow.models.responses.WsResponse;
import org.polypheny.db.workflow.models.responses.WsResponse.ResponseType;

@Slf4j
public abstract class AbstractSession {

    final Workflow workflow;
    @Getter
    final UUID sessionId;
    final StorageManager sm;
    final ObjectMapper mapper = new ObjectMapper();
    private final Set<Session> subscribers = new HashSet<>();
    final GlobalScheduler scheduler;
    ExecutionMonitor executionMonitor; // corresponds to the last started execution


    protected AbstractSession( Workflow workflow, UUID sessionId ) {
        this.workflow = workflow;
        this.sessionId = sessionId;
        this.sm = new StorageManagerImpl( sessionId, workflow.getConfig().getPreferredStores() );
        this.scheduler = GlobalScheduler.getInstance();
    }


    public abstract void terminate();


    /**
     * Subscribe to any updates.
     *
     * @param session the UI websocket session to be registered
     */
    public void subscribe( Session session ) {
        System.out.println( "subscribed" );
        subscribers.add( session );
    }


    public void unsubscribe( Session session ) {
        System.out.println( "unsubscribed" );
        subscribers.remove( session );
    }


    public int getSubscriberCount() {
        return subscribers.size();
    }


    public abstract SessionModel toModel();


    public ActivityModel getActivityModel( UUID activityId ) {
        return workflow.getActivity( activityId ).toModel( true );
    }


    public WorkflowModel getWorkflowModel( boolean includeState ) {
        return workflow.toModel( includeState );
    }


    public WorkflowConfigModel getWorkflowConfig() {
        return workflow.getConfig();
    }


    void broadcastMessage( WsResponse msg ) {
        try {
            String json = mapper.writeValueAsString( msg );
            if ( msg.type != ResponseType.PROGRESS_UPDATE ) {
                log.info( "Broadcasting message: " + json );
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
            executionMonitor = GlobalScheduler.getInstance().startExecution( workflow, sm, targetActivity, this::broadcastMessage );
        } catch ( Exception e ) {
            // TODO: implement correct error handling when execution cannot be started
            throw new IllegalStateException( "Unable to start workflow execution", e );
        }
    }


    void interruptExecution() {
        throwIfNotExecuting();
        scheduler.interruptExecution( sessionId );
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

}
