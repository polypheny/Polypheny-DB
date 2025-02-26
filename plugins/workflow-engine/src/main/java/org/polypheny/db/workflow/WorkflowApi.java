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

package org.polypheny.db.workflow;

import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import java.util.UUID;
import lombok.Getter;
import org.polypheny.db.webui.ConfigService.HandlerType;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.workflow.dag.activities.ActivityRegistry;
import org.polypheny.db.workflow.models.WorkflowModel;
import org.polypheny.db.workflow.repo.WorkflowRepo;
import org.polypheny.db.workflow.session.SessionManager;

public class WorkflowApi {

    private final SessionManager sessionManager;
    private final WorkflowRepo repo;
    public static final String PATH = WorkflowManager.PATH + "/api";


    public WorkflowApi( SessionManager sessionManager, WorkflowRepo repo ) {
        this.sessionManager = sessionManager;
        this.repo = repo;
    }


    public void registerEndpoints( HttpServer server ) {
        // Endpoints are often similar to those in WorkflowManager, but are limited to API sessions.
        // Other endpoints were websocket messages in a UserSession.
        server.addSerializedRoute( PATH + "/sessions", this::getSessions, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}", this::getSession, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/state", this::getSessionState, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow", this::getWorkflow, HandlerType.GET ); // queryParam: state = false
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow/config", this::getWorkflowConfig, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow/variables", this::getWorkflowVariables, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow/statistics", this::getExecutionMonitor, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow/{activityId}", this::getActivity, HandlerType.GET ); // queryParam: state = true
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow/{activityId}/statistics", this::getActivityStats, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/workflow/{activityId}/{outIndex}", this::getIntermediaryResult, HandlerType.GET );  // queryParam: limit = null
        server.addSerializedRoute( PATH + "/registry", this::getActivityRegistry, HandlerType.GET );
        server.addSerializedRoute( PATH + "/registry/{activityType}", this::getActivityDef, HandlerType.GET );

        server.addSerializedRoute( PATH + "/sessions", this::createSession, HandlerType.POST );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/execute", this::execute, HandlerType.POST ); // queryParam: target = null
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/reset", this::reset, HandlerType.POST ); // queryParam: target = null
        server.addSerializedRoute( PATH + "/sessions/{sessionId}/interrupt", this::interrupt, HandlerType.POST );

        server.addSerializedRoute( PATH + "/sessions", this::terminateSessions, HandlerType.DELETE );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}", this::terminateSession, HandlerType.DELETE );

    }


    private void getSessions( final Context ctx ) {
        process( ctx, sessionManager::getApiSessionModels );
    }


    private void getSession( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        process( ctx, () -> sessionManager.getApiSessionModel( sessionId ) );
    }


    private void getSessionState( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        process( ctx, () -> sessionManager.getApiSessionOrThrow( sessionId ).getWorkflowState() );
    }


    private void getWorkflow( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        boolean includeState = getQueryParam( ctx, "state", false );
        process( ctx, () -> sessionManager.getApiSessionOrThrow( sessionId ).getWorkflowModel( includeState ) );
    }


    private void getWorkflowConfig( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        process( ctx, () -> sessionManager.getApiSessionOrThrow( sessionId ).getWorkflowConfig() );
    }


    private void getWorkflowVariables( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        process( ctx, () -> sessionManager.getApiSessionOrThrow( sessionId ).getVariables() );
    }


    private void getExecutionMonitor( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        process( ctx, () -> sessionManager.getApiSessionOrThrow( sessionId ).getMonitorModel() );
    }


    private void getActivity( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        UUID activityId = UUID.fromString( ctx.pathParam( "activityId" ) );
        boolean includeState = getQueryParam( ctx, "state", true );
        process( ctx, () -> sessionManager.getApiSessionOrThrow( sessionId ).getActivityModel( activityId, includeState ) );
    }


    private void getActivityStats( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        UUID activityId = UUID.fromString( ctx.pathParam( "activityId" ) );
        process( ctx, () -> sessionManager.getApiSessionOrThrow( sessionId ).getActivityStats( activityId ) );
    }


    private void getIntermediaryResult( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        UUID activityId = UUID.fromString( ctx.pathParam( "activityId" ) );
        int outIndex = Integer.parseInt( ctx.pathParam( "outIndex" ) );
        String limitParam = ctx.queryParam( "limit" );
        Integer maxTuples = limitParam == null ? null : Integer.parseInt( limitParam );
        process( ctx, () -> sessionManager.getApiSessionOrThrow( sessionId ).getCheckpoint( activityId, outIndex, maxTuples ) );
    }


    private void getActivityRegistry( final Context ctx ) {
        process( ctx, ActivityRegistry::getRegistry );
    }


    private void getActivityDef( final Context ctx ) {
        String activityType = ctx.pathParam( "activityType" );
        process( ctx, () -> {
            try {
                return ActivityRegistry.get( activityType );
            } catch ( Exception e ) {
                throw new WorkflowApiException( "Specified activityType does not exist: " + activityType, HttpCode.NOT_FOUND );
            }
        } );
    }


    private void createSession( final Context ctx ) {
        WorkflowModel workflowModel = ctx.bodyAsClass( WorkflowModel.class );
        process( ctx, () -> sessionManager.createApiSession( workflowModel ) );
    }


    private void execute( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        String targetParam = ctx.queryParam( "target" );
        UUID target = targetParam == null ? null : UUID.fromString( targetParam );
        process( ctx, () -> {
            sessionManager.getApiSessionOrThrow( sessionId ).execute( target );
            return "success";
        } );
    }


    private void interrupt( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        process( ctx, () -> {
            sessionManager.getApiSessionOrThrow( sessionId ).interrupt();
            return "success";
        } );
    }


    private void reset( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        String targetParam = ctx.queryParam( "target" );
        UUID target = targetParam == null ? null : UUID.fromString( targetParam );
        process( ctx, () -> {
            sessionManager.getApiSessionOrThrow( sessionId ).reset( target );
            return "success";
        } );
    }


    private void terminateSessions( final Context ctx ) {
        process( ctx, sessionManager::terminateApiSessions );
    }


    private void terminateSession( final Context ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        process( ctx, () -> {
            sessionManager.getApiSessionOrThrow( sessionId ); // ensure it's an API session
            sessionManager.terminateSession( sessionId );
            return "success";
        } );
    }


    private void process( Context ctx, ResultSupplier s ) {
        try {
            WorkflowManager.sendResult( ctx, s.get() );
        } catch ( WorkflowApiException e ) {
            ctx.status( e.getErrorCode() );
            ctx.json( e.getMessage() );
        } catch ( Exception e ) {
            ctx.status( HttpCode.INTERNAL_SERVER_ERROR );
            ctx.json( e.getMessage() );
            e.printStackTrace();
        }
    }


    private boolean getQueryParam( Context ctx, String key, boolean defaultValue ) {
        String value = ctx.queryParam( key );
        if ( value == null ) {
            return defaultValue;
        }
        return Boolean.parseBoolean( value );
    }


    @FunctionalInterface
    private interface ResultSupplier {

        Object get() throws WorkflowApiException;

    }


    @Getter
    public static class WorkflowApiException extends Exception {

        private final HttpCode errorCode;


        public WorkflowApiException( String message, HttpCode errorCode ) {
            super( message );
            this.errorCode = errorCode;
        }


        public WorkflowApiException( String message ) {
            this( message, HttpCode.INTERNAL_SERVER_ERROR );
        }

    }

}
