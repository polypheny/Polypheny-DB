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
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.javalin.http.ContentType;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.transaction.TransactionManagerImpl;
import org.polypheny.db.webui.ConfigService.HandlerType;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.StorageManagerImpl;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.RelReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.RenderModel;
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

        createDummyExecutionTest();
        createExecuteDummyWorkflowTest();

        // waiting with test to ensure everything has started
        /*new java.util.Timer().schedule(
                new java.util.TimerTask() {
                    @Override
                    public void run() {
                        //createDummyCheckpoint();
                    }
                },
                5000
        );*/
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


    private void createDummyExecutionTest() {
        HttpServer server = HttpServer.getInstance();
        server.addSerializedRoute( PATH + "/dummy/{tableName}", ctx -> {
            System.out.println( "handling dummy..." );
            String tableName = ctx.pathParam( "tableName" );
            LogicalTable table = Catalog.snapshot().rel().getTable( Catalog.defaultNamespaceId, tableName ).orElseThrow();

            try ( StorageManager sm = new StorageManagerImpl( UUID.randomUUID(), Map.of() ) ) {

                UUID activityId = UUID.randomUUID();

                try ( RelWriter writer = sm.createRelCheckpoint( activityId, 0, table.getTupleType(), false, null );
                        RelReader reader = new RelReader( table, TransactionManagerImpl.getInstance().startTransaction( Catalog.defaultUserId, table.namespaceId, false, StorageManager.ORIGIN ) ) ) {
                    System.out.println( "Reading and writing..." );

                    long start = System.currentTimeMillis();

                    /*Iterator<PolyValue[]> it = reader.getIterator();
                    System.out.println( "start iteration..." );
                    while ( it.hasNext() ) {
                        System.out.println( Arrays.toString( it.next() ) );
                    }*/
                    writer.write( reader.getIterator() );

                    long finish = System.currentTimeMillis();
                    long timeElapsed = finish - start;
                    System.out.println( "Time in ms: " + timeElapsed );
                }

                sendResult( ctx, "success!" );

            } catch ( Exception e ) {
                e.printStackTrace();
            }


        }, HandlerType.GET );
    }


    /**
     * Add a route that tests the relExtract activity.
     * A target relational entity can be specified via path parameters.
     * The entity is read by the relExtract activity and the result is written to a checkpoint.
     */
    private void createExecuteDummyWorkflowTest() {
        HttpServer server = HttpServer.getInstance();
        server.addSerializedRoute( PATH + "/executeDummy/{namespaceName}/{tableName}", ctx -> {
            System.out.println( "handling dummy execution..." );

            JsonMapper mapper = new JsonMapper();
            ObjectNode setting = mapper.createObjectNode();
            setting.put( "namespace", ctx.pathParam( "namespaceName" ) );
            setting.put( "name", ctx.pathParam( "tableName" ) );

            try ( StorageManager sm = new StorageManagerImpl( UUID.randomUUID(), Map.of() ) ) {
                UUID activityId = UUID.randomUUID();

                ActivityWrapper wrapper = ActivityWrapper.fromModel( new ActivityModel(
                        "relExtract",
                        activityId,
                        Map.of( "table", setting ),
                        ActivityConfigModel.of(),
                        RenderModel.of(),
                        ActivityState.IDLE
                ) );

                System.out.println( wrapper.getActivity().previewOutTypes( List.of(), wrapper.resolveAvailableSettings() ) );

                long start = System.currentTimeMillis();
                wrapper.getActivity().execute( List.of(), wrapper.resolveSettings(), new ExecutionContextImpl( wrapper, sm ) );

                sm.commitTransaction( activityId );
                System.out.println( "Extract time in ms: " + (System.currentTimeMillis() - start) );

                UUID activityId2 = UUID.randomUUID();
                ActivityWrapper loadWrapper = ActivityWrapper.fromModel( new ActivityModel(
                        "relLoad",
                        activityId2,
                        Map.of( "table", setting ),
                        ActivityConfigModel.of(),
                        RenderModel.of(),
                        ActivityState.IDLE
                ) );

                start = System.currentTimeMillis();
                try ( CheckpointReader reader = sm.readCheckpoint( activityId, 0 ) ) {
                    loadWrapper.getActivity().execute( List.of( reader ), loadWrapper.resolveSettings(), new ExecutionContextImpl( loadWrapper, sm ) );
                }
                sm.commitTransaction( activityId2 );
                System.out.println( "Load time in ms: " + (System.currentTimeMillis() - start) );

                sendResult( ctx, "success!" );
            } catch ( Exception e ) {
                e.printStackTrace();
            }
        }, HandlerType.GET );

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
