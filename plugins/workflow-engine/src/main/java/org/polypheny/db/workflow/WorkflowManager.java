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

package org.polypheny.db.workflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.http.ContentType;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import java.util.UUID;
import org.polypheny.db.webui.ConfigService.HandlerType;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.workflow.models.WorkflowModel;
import org.polypheny.db.workflow.models.requests.CreateSessionRequest;
import org.polypheny.db.workflow.models.requests.SaveSessionRequest;
import org.polypheny.db.workflow.repo.WorkflowRepo;
import org.polypheny.db.workflow.repo.WorkflowRepo.WorkflowRepoException;
import org.polypheny.db.workflow.repo.WorkflowRepoImpl;
import org.polypheny.db.workflow.session.SessionManager;
import org.polypheny.db.workflow.session.WorkflowWebSocket;

public class WorkflowManager {

    private final SessionManager sessionManager;
    private final WorkflowRepo repo;
    public final String PATH = "/workflows";
    private final ObjectMapper mapper = new ObjectMapper();


    public WorkflowManager() {
        repo = WorkflowRepoImpl.getInstance();
        sessionManager = SessionManager.getInstance();
        registerEndpoints();

        createDummySession();
    }


    private void createDummySession() {
        try {
            UUID id = repo.createWorkflow( "Dummy Workflow" );
            int version = repo.writeVersion( id, "Initial version", WorkflowModel.getSample() );
            sessionManager.createUserSession( id, version );
        } catch ( WorkflowRepoException e ) {
            e.printStackTrace();
        }

    }


    private void registerEndpoints() {
        HttpServer server = HttpServer.getInstance();

        server.addWebsocketRoute( PATH + "/websocket/{sessionId}", new WorkflowWebSocket() );

        server.addSerializedRoute( PATH + "/sessions", this::getSessions, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}", this::getSession, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow", this::getActiveWorkflow, HandlerType.GET );
        server.addSerializedRoute( PATH + "/workflows", this::getWorkflows, HandlerType.GET );

        server.addSerializedRoute( PATH + "/sessions", this::createSession, HandlerType.POST );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/save", this::saveSession, HandlerType.POST );

        server.addSerializedRoute( PATH + "/sessions/{sessionId}", this::terminateSession, HandlerType.DELETE );
    }


    private void getSessions( final Context ctx ) {
        process( ctx, sessionManager::getSessionModels );
    }


    private void getSession( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        process( ctx, () -> sessionManager.getSessionModel( sessionId ) );
    }


    private void getActiveWorkflow( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        process( ctx, () -> sessionManager.getActiveWorkflowModel( sessionId ) );
    }


    private void getWorkflows( final Context ctx ) {
        process( ctx, repo::getWorkflowDefs );
    }


    private void createSession( final Context ctx ) {
        CreateSessionRequest request = ctx.bodyAsClass( CreateSessionRequest.class );
        process( ctx, () -> sessionManager.createUserSession( request.getName() ) );
    }


    private void saveSession( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        SaveSessionRequest request = ctx.bodyAsClass( SaveSessionRequest.class );
        process( ctx, () -> {
            sessionManager.saveUserSession( sessionId, request.getMessage() );
            return "success";
        } );
    }


    private void terminateSession( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        process( ctx, () -> {
            sessionManager.terminateSession( sessionId );
            return "success";
        } );
    }


    private void process( Context ctx, ResultSupplier s ) {
        try {
            sendResult( ctx, s.get() );
        } catch ( WorkflowRepoException e ) {
            // TODO: better error handling
            ctx.status( HttpCode.INTERNAL_SERVER_ERROR );
            ctx.json( e );
        }
    }


    private void sendResult( Context ctx, Object model ) {
        ctx.contentType( ContentType.JSON );
        try {
            ctx.result( mapper.writeValueAsString( model ) );
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        }
    }


    @FunctionalInterface
    private interface ResultSupplier {

        Object get() throws WorkflowRepoException;

    }

}
