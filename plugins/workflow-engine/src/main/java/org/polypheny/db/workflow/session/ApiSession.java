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

import io.javalin.http.HttpCode;
import io.javalin.websocket.WsMessageContext;
import java.util.UUID;
import javax.annotation.Nullable;
import org.polypheny.db.util.Triple;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.db.workflow.WorkflowApi.WorkflowApiException;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.models.SessionModel;
import org.polypheny.db.workflow.models.SessionModel.SessionModelType;
import org.polypheny.db.workflow.models.requests.WsRequest.GetCheckpointRequest;
import org.polypheny.db.workflow.models.responses.CheckpointResponse;
import org.polypheny.db.workflow.models.responses.WsResponse.CheckpointDataResponse;
import org.polypheny.db.workflow.models.responses.WsResponse.StateUpdateResponse;

public class ApiSession extends AbstractSession {


    public ApiSession( UUID sessionId, Workflow wf ) {
        super( wf, sessionId );
    }


    @Override
    public void handleRequest( GetCheckpointRequest request, WsMessageContext ctx ) {
        Triple<Result<?, ?>, Integer, Long> preview = getCheckpointData( request.activityId, request.outputIndex, null );
        ctx.send( new CheckpointDataResponse( request.msgId, preview.left, preview.middle, preview.right ) ); // we do NOT broadcast the result
    }


    public CheckpointResponse getCheckpoint( UUID activityId, int outputIdx, @Nullable Integer maxTuples ) throws WorkflowApiException {
        try {
            Triple<Result<?, ?>, Integer, Long> triple = getCheckpointData( activityId, outputIdx, maxTuples );
            return new CheckpointResponse( triple.getLeft(), triple.getMiddle(), triple.getRight() );
        } catch ( Exception e ) {
            throw new WorkflowApiException( e.getMessage(), HttpCode.BAD_REQUEST );
        }
    }


    public void execute( @Nullable UUID targetId ) throws WorkflowApiException {
        if ( workflow.getState() != WorkflowState.IDLE ) {
            throw new WorkflowApiException( "Workflow is currently not idle: " + workflow.getState(), HttpCode.CONFLICT );
        }
        startExecution( targetId );
    }


    public void interrupt() throws WorkflowApiException {
        if ( workflow.getState() != WorkflowState.EXECUTING ) {
            throw new WorkflowApiException( "Workflow is currently not being executed: " + workflow.getState(), HttpCode.CONFLICT );
        }
        interruptExecution();
    }


    public void reset( @Nullable UUID targetId ) throws WorkflowApiException {
        if ( workflow.getState() != WorkflowState.IDLE ) {
            throw new WorkflowApiException( "Workflow is currently not idle: " + workflow.getState(), HttpCode.CONFLICT );
        }
        workflow.reset( targetId, sm );
        broadcastMessage( new StateUpdateResponse( null, workflow ) );
    }

    // TODO: allow moving around workflow nodes?


    @Override
    public SessionModel toModel() {
        return new SessionModel( SessionModelType.API_SESSION, sessionId, getSubscriberCount(),
                lastInteraction.toString(), workflow.getActivityCount(), workflow.getState() );
    }

}
