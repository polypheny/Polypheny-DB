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
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.util.Sources;
import org.polypheny.db.webui.ConfigService.HandlerType;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.workflow.dag.activities.ActivityRegistry;
import org.polypheny.db.workflow.engine.storage.StorageManagerImpl;
import org.polypheny.db.workflow.jobs.JobManager;
import org.polypheny.db.workflow.jobs.JobManager.WorkflowJobException;
import org.polypheny.db.workflow.models.JobModel;
import org.polypheny.db.workflow.models.WorkflowModel;
import org.polypheny.db.workflow.models.requests.CreateSessionRequest;
import org.polypheny.db.workflow.models.requests.ImportWorkflowRequest;
import org.polypheny.db.workflow.models.requests.RenameWorkflowRequest;
import org.polypheny.db.workflow.models.requests.SaveSessionRequest;
import org.polypheny.db.workflow.repo.WorkflowRepo;
import org.polypheny.db.workflow.repo.WorkflowRepo.WorkflowRepoException;
import org.polypheny.db.workflow.repo.WorkflowRepoImpl;
import org.polypheny.db.workflow.session.SessionManager;
import org.polypheny.db.workflow.session.WorkflowWebSocket;

@Slf4j
public class WorkflowManager {

    private final SessionManager sessionManager;
    private final WorkflowRepo repo;
    private final JobManager jobManager;
    private final WorkflowApi apiManager;
    public static final String PATH = "/workflows";
    public static final String DEFAULT_CHECKPOINT_ADAPTER = "hsqldb_disk";
    private static final ObjectMapper mapper = new ObjectMapper();


    public WorkflowManager() {
        repo = WorkflowRepoImpl.getInstance();
        sessionManager = SessionManager.getInstance();
        registerEndpoints();
        apiManager = new WorkflowApi( sessionManager );
        apiManager.registerEndpoints( HttpServer.getInstance() );
        jobManager = JobManager.getInstance();

        if ( PolyphenyDb.mode == RunMode.TEST ) {
            return;
        }
        // waiting to ensure everything has started
        new java.util.Timer().schedule(
                new java.util.TimerTask() {
                    @Override
                    public void run() {
                        StorageManagerImpl.clearAll(); // remove old namespaces and checkpoints
                        registerAdapter(); // TODO: only register adapter when the first workflow is opened
                        addSampleWorkflows();
                        try {
                            jobManager.onStartup();
                        } catch ( Exception e ) {
                            log.error( "Error on job startup", e );
                        }
                    }
                },
                1000
        );
    }


    public void shutdown() {
        sessionManager.terminateAll();
    }


    private void addSampleWorkflows() {
        URL workflowDir = this.getClass().getClassLoader().getResource( "workflows/" );
        File[] files = Sources.of( workflowDir )
                .file()
                .listFiles( ( d, name ) -> name.endsWith( ".json" ) );
        if ( files == null ) {
            return;
        }

        for ( File file : files ) {
            String fileName = file.getName();
            fileName = fileName.substring( 0, fileName.length() - ".json".length() );
            try {
                WorkflowModel workflow = mapper.readValue( file, WorkflowModel.class );
                if ( repo.doesNameExist( fileName ) ) {
                    continue;
                }
                String group = "Sample Workflows for Debugging";
                if ( fileName.startsWith( "Demo " ) ) {
                    group = "Demonstration";
                } else if ( fileName.startsWith( "Evaluation " ) ) {
                    group = "Evaluation";
                    fileName = fileName.substring( "Evaluation ".length() );
                }
                repo.importWorkflow( fileName, group, workflow );
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }
    }


    private void registerAdapter() {
        if ( Catalog.getInstance().getAdapters().values().stream().anyMatch( a -> a.uniqueName.equals( DEFAULT_CHECKPOINT_ADAPTER ) ) ) {
            return;
        }

        AdapterTemplate storeTemplate = Catalog.snapshot().getAdapterTemplate( "HSQLDB", AdapterType.STORE ).orElseThrow();
        Map<String, String> settings = new HashMap<>( storeTemplate.getDefaultSettings() );
        settings.put( "trxControlMode", "locks" );
        settings.put( "type", "File" );
        settings.put( "tableType", "Cached" );

        DdlManager.getInstance().createStore( DEFAULT_CHECKPOINT_ADAPTER, storeTemplate.getAdapterName(), AdapterType.STORE, settings, storeTemplate.getDefaultMode() );
    }


    private void registerEndpoints() {
        HttpServer server = HttpServer.getInstance();

        server.addWebsocketRoute( PATH + "/webSocket/{sessionId}", new WorkflowWebSocket() );

        server.addSerializedRoute( PATH + "/sessions", this::getSessions, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}", this::getSession, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow", this::getActiveWorkflow, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow/config", this::getWorkflowConfig, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow/variables", this::getWorkflowVariables, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow/monitor", this::getExecutionMonitor, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow/{activityId}", this::getActivity, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow/{activityId}/nested", this::getNestedSession, HandlerType.GET );
        server.addSerializedRoute( PATH + "/workflows", this::getWorkflowDefs, HandlerType.GET );
        server.addSerializedRoute( PATH + "/workflows/{workflowId}/{version}", this::getWorkflow, HandlerType.GET );
        server.addSerializedRoute( PATH + "/registry", this::getActivityRegistry, HandlerType.GET );
        server.addSerializedRoute( PATH + "/jobs", this::getJobs, HandlerType.GET );

        server.addSerializedRoute( PATH + "/sessions", this::createSession, HandlerType.POST );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/save", this::saveSession, HandlerType.POST );
        server.addSerializedRoute( PATH + "/workflows", this::importWorkflow, HandlerType.POST );
        server.addSerializedRoute( PATH + "/workflows/{workflowId}/{version}", this::openWorkflow, HandlerType.POST );
        server.addSerializedRoute( PATH + "/workflows/{workflowId}/{version}/copy", this::copyWorkflow, HandlerType.POST );
        server.addSerializedRoute( PATH + "/jobs", this::setJob, HandlerType.POST );
        server.addSerializedRoute( PATH + "/jobs/{jobId}/enable", this::enableJob, HandlerType.POST );
        server.addSerializedRoute( PATH + "/jobs/{jobId}/disable", this::disableJob, HandlerType.POST );
        server.addSerializedRoute( PATH + "/jobs/{jobId}/trigger", this::triggerJob, HandlerType.POST );

        server.addSerializedRoute( PATH + "/workflows/{workflowId}", this::renameWorkflow, HandlerType.PATCH );

        server.addSerializedRoute( PATH + "/sessions/{sessionId}", this::terminateSession, HandlerType.DELETE );
        server.addSerializedRoute( PATH + "/workflows/{workflowId}", this::deleteWorkflow, HandlerType.DELETE );
        server.addSerializedRoute( PATH + "/workflows/{workflowId}/{version}", this::deleteVersion, HandlerType.DELETE );
        server.addSerializedRoute( PATH + "/jobs/{jobId}", this::deleteJob, HandlerType.DELETE );
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
        process( ctx, () -> sessionManager.getSessionOrThrow( sessionId ).getWorkflowModel( true ) );
    }


    private void getWorkflowConfig( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        process( ctx, () -> sessionManager.getSessionOrThrow( sessionId ).getWorkflowConfig() );
    }


    private void getWorkflowVariables( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        process( ctx, () -> sessionManager.getSessionOrThrow( sessionId ).getVariables() );
    }


    private void getExecutionMonitor( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        process( ctx, () -> sessionManager.getSessionOrThrow( sessionId ).getMonitorModel() );
    }


    private void getActivity( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        UUID activityId = UUID.fromString( ctx.pathParam( "activityId" ) );
        process( ctx, () -> sessionManager.getSessionOrThrow( sessionId ).getActivityModel( activityId, true ) );
    }


    private void getNestedSession( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        UUID activityId = UUID.fromString( ctx.pathParam( "activityId" ) );
        process( ctx, () -> sessionManager.getSessionOrThrow( sessionId ).getNestedModelOrNull( activityId ) );
    }


    private void getWorkflowDefs( final Context ctx ) {
        process( ctx, repo::getWorkflowDefs );
    }


    private void getWorkflow( final Context ctx ) {
        UUID workflowId = UUID.fromString( ctx.pathParam( "workflowId" ) );
        int version = Integer.parseInt( ctx.pathParam( "version" ) );
        process( ctx, () -> repo.readVersion( workflowId, version ) );
    }


    private void getActivityRegistry( final Context ctx ) {
        process( ctx, ActivityRegistry::getRegistry );
    }


    private void getJobs( final Context ctx ) {
        process( ctx, jobManager::getJobs );
    }


    private void createSession( final Context ctx ) {
        // -> creates a new workflow
        CreateSessionRequest request = ctx.bodyAsClass( CreateSessionRequest.class );
        process( ctx, () -> sessionManager.createUserSession( request.getName(), request.getGroup() ) );
    }


    private void saveSession( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        SaveSessionRequest request = ctx.bodyAsClass( SaveSessionRequest.class );
        process( ctx, () -> sessionManager.saveUserSession( sessionId, request.getMessage() ) );
    }


    private void importWorkflow( final Context ctx ) {
        // upload a WorkflowModel to create a new workflow
        ImportWorkflowRequest request = ctx.bodyAsClass( ImportWorkflowRequest.class );
        process( ctx, () -> repo.importWorkflow( request.getName(), request.getGroup(), request.getWorkflow() ) );
    }


    private void openWorkflow( final Context ctx ) {
        // -> opens existing workflow
        UUID workflowId = UUID.fromString( ctx.pathParam( "workflowId" ) );
        int version = Integer.parseInt( ctx.pathParam( "version" ) );
        process( ctx, () -> sessionManager.createUserSession( workflowId, version ) );
    }


    private void copyWorkflow( final Context ctx ) {
        // -> opens existing workflow
        UUID workflowId = UUID.fromString( ctx.pathParam( "workflowId" ) );
        int version = Integer.parseInt( ctx.pathParam( "version" ) );
        RenameWorkflowRequest request = ctx.bodyAsClass( RenameWorkflowRequest.class );
        process( ctx, () -> repo.createWorkflowFromVersion( workflowId, version, request.getName(), request.getGroup() ) );
    }


    private void renameWorkflow( final Context ctx ) {
        UUID workflowId = UUID.fromString( ctx.pathParam( "workflowId" ) );
        RenameWorkflowRequest request = ctx.bodyAsClass( RenameWorkflowRequest.class );
        process( ctx, () -> {
            if ( request.getName() != null ) {
                repo.renameWorkflow( workflowId, request.getName() );
            }
            if ( request.getGroup() != null ) {
                repo.updateWorkflowGroup( workflowId, request.getGroup() );
            }
            return "success";
        } );
    }


    private void setJob( final Context ctx ) {
        JobModel model = ctx.bodyAsClass( JobModel.class );
        process( ctx, () -> jobManager.setJob( model ) );
    }


    private void enableJob( final Context ctx ) {
        UUID jobId = UUID.fromString( ctx.pathParam( "jobId" ) );
        process( ctx, () -> jobManager.enable( jobId ) );
    }


    private void disableJob( final Context ctx ) {
        UUID jobId = UUID.fromString( ctx.pathParam( "jobId" ) );
        process( ctx, () -> {
            jobManager.disable( jobId );
            return "success";
        } );
    }


    private void triggerJob( final Context ctx ) {
        UUID jobId = UUID.fromString( ctx.pathParam( "jobId" ) );
        process( ctx, () -> {
            jobManager.manuallyTrigger( jobId );
            return "success";
        } );
    }


    private void deleteVersion( final Context ctx ) {
        UUID workflowId = UUID.fromString( ctx.pathParam( "workflowId" ) );
        int version = Integer.parseInt( ctx.pathParam( "version" ) );
        process( ctx, () -> {
            repo.deleteVersion( workflowId, version );
            return "success";
        } );
    }


    private void deleteWorkflow( final Context ctx ) {
        UUID workflowId = UUID.fromString( ctx.pathParam( "workflowId" ) );
        process( ctx, () -> {
            if ( sessionManager.isWorkflowOpened( workflowId ) ) {
                throw new WorkflowRepoException( "Cannot delete workflow while it is opened in a session", HttpCode.FORBIDDEN );
            }
            repo.deleteWorkflow( workflowId );
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


    private void deleteJob( final Context ctx ) {
        UUID jobId = UUID.fromString( ctx.pathParam( "jobId" ) );
        process( ctx, () -> {
            jobManager.deleteJob( jobId );
            return "success";
        } );
    }


    private void process( Context ctx, ResultSupplier s ) {
        try {
            sendResult( ctx, s.get() );
        } catch ( WorkflowRepoException e ) {
            ctx.status( e.getErrorCode() );
            ctx.json( e.getMessage() );
        } catch ( Exception e ) {
            ctx.status( HttpCode.INTERNAL_SERVER_ERROR );
            ctx.json( e.getMessage() );
            e.printStackTrace();
        }
    }


    public static void sendResult( Context ctx, Object model ) {
        ctx.contentType( ContentType.JSON );
        try {
            ctx.result( mapper.writeValueAsString( model ) );
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        }
    }


    @FunctionalInterface
    private interface ResultSupplier {

        Object get() throws WorkflowRepoException, WorkflowJobException;

    }

}
