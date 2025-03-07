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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.workflow.WorkflowApi.WorkflowApiException;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.WorkflowImpl;
import org.polypheny.db.workflow.jobs.JobTrigger;
import org.polypheny.db.workflow.models.SessionModel;
import org.polypheny.db.workflow.models.WorkflowModel;
import org.polypheny.db.workflow.repo.WorkflowRepo;
import org.polypheny.db.workflow.repo.WorkflowRepo.WorkflowRepoException;
import org.polypheny.db.workflow.repo.WorkflowRepoImpl;

@Slf4j
public class SessionManager {

    private static SessionManager INSTANCE = null;

    private final Map<UUID, UserSession> userSessions = new ConcurrentHashMap<>();
    private final Map<UUID, ApiSession> apiSessions = new ConcurrentHashMap<>();
    private final Map<UUID, NestedSession> nestedSessions = new ConcurrentHashMap<>(); // unlike the map in the nestedSessionManager, the key is a sessionId
    private final Map<UUID, JobSession> jobSessions = new ConcurrentHashMap<>(); // note that sessionId != jobId
    private final WorkflowRepo repo = WorkflowRepoImpl.getInstance();


    private SessionManager() {
    }


    public static SessionManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new SessionManager();
        }
        return INSTANCE;
    }


    public UUID createUserSession( String newWorkflowName, String group ) throws WorkflowRepoException {
        UUID wId = repo.createWorkflow( newWorkflowName, group );
        UUID sessionId = registerUserSession( new WorkflowImpl(), wId, 0 );
        saveUserSession( sessionId, "Initial Version" );
        return sessionId;
    }


    public UUID createUserSession( UUID wId, int version ) throws WorkflowRepoException {
        WorkflowModel model = repo.readVersion( wId, version );
        try {
            return registerUserSession( WorkflowImpl.fromModel( model ), wId, version );
        } catch ( Exception e ) {
            throw new WorkflowRepoException( "Unable to instantiate workflow: " + e.getMessage(), e );
        }
    }


    public UUID createApiSession( WorkflowModel model ) throws WorkflowApiException {
        try {
            return registerApiSession( WorkflowImpl.fromModel( model ) );
        } catch ( Exception e ) {
            throw new WorkflowApiException( "Unable to instantiate workflow: " + e.getMessage(), HttpCode.BAD_REQUEST );
        }
    }


    public Map<UUID, SessionModel> getUserSessionModels() {
        return toSessionModelMap( userSessions );
    }


    public Map<UUID, SessionModel> getApiSessionModels() {
        return toSessionModelMap( apiSessions );
    }


    public Map<UUID, SessionModel> getJobSessionModels() {
        return toSessionModelMap( jobSessions );
    }


    public Map<UUID, SessionModel> getNestedSessionModels() {
        return toSessionModelMap( nestedSessions );
    }


    public Map<UUID, SessionModel> getSessionModels() {
        Map<UUID, SessionModel> models = getUserSessionModels();
        models.putAll( getApiSessionModels() );
        models.putAll( getNestedSessionModels() );
        models.putAll( getJobSessionModels() );
        return models;
    }


    public SessionModel getSessionModel( UUID sId ) {
        return getSessionOrThrow( sId ).toModel();
    }


    public SessionModel getApiSessionModel( UUID sessionId ) throws WorkflowApiException {
        return getApiSessionOrThrow( sessionId ).toModel();
    }


    public void terminateSession( UUID sId ) {
        AbstractSession session = getSessionOrThrow( sId );
        if ( session instanceof NestedSession ) {
            throw new GenericRuntimeException( "Nested sessions can only get terminated via their parent session." );
        }
        session.terminate();
        removeSession( sId );
    }


    public void terminateAll() {
        Set<UUID> sessionIds = userSessions.keySet();
        sessionIds.addAll( apiSessions.keySet() );
        sessionIds.addAll( jobSessions.keySet() );
        // nested sessions are terminated recursively
        for ( UUID sId : sessionIds ) {
            try {
                terminateSession( sId );
            } catch ( Exception e ) {
                log.warn( "Unable to terminate session: {}", sId, e );
            }
        }
    }


    public Map<String, Integer> terminateApiSessions() {
        int successCount = 0;
        int failedCount = 0;
        for ( UUID sId : apiSessions.keySet() ) {
            try {
                terminateSession( sId );
                successCount++;
            } catch ( Exception e ) {
                log.warn( "Unable to terminate session: {}", sId, e );
                failedCount++;
            }
        }
        return Map.of( "successCount", successCount, "failedCount", failedCount );
    }


    public int saveUserSession( UUID sId, String versionDesc ) throws WorkflowRepoException {
        UserSession session = getUserSessionOrThrow( sId );
        int version = repo.writeVersion( session.getWId(), versionDesc, session.getWorkflowModel( false ) );
        session.setOpenedVersion( version );
        return version;
    }


    public AbstractSession getSession( UUID sId ) {
        if ( userSessions.containsKey( sId ) ) {
            return userSessions.get( sId );
        } else if ( apiSessions.containsKey( sId ) ) {
            return apiSessions.get( sId );
        } else if ( nestedSessions.containsKey( sId ) ) {
            return nestedSessions.get( sId );
        } else if ( jobSessions.containsKey( sId ) ) {
            return jobSessions.get( sId );
        }
        return null;
    }


    public AbstractSession getSessionOrThrow( UUID sId ) {
        AbstractSession session = getSession( sId );
        if ( session == null ) {
            throw new IllegalArgumentException( "Unknown session with id: " + sId );
        }
        return session;
    }


    public ApiSession getApiSessionOrThrow( UUID sId ) throws WorkflowApiException {
        ApiSession session = apiSessions.get( sId );
        if ( session == null ) {
            throw new WorkflowApiException( "Unknown api session with id: " + sId, HttpCode.NOT_FOUND );
        }
        return session;
    }


    public UserSession getUserSessionOrThrow( UUID sId ) {
        UserSession session = userSessions.get( sId );
        if ( session == null ) {
            throw new IllegalArgumentException( "Unknown user session with id: " + sId );
        }
        return session;
    }


    public JobSession getJobSessionOrThrow( UUID sId ) {
        JobSession session = jobSessions.get( sId );
        if ( session == null ) {
            throw new IllegalArgumentException( "Unknown job session with id: " + sId );
        }
        return session;
    }


    public boolean isWorkflowOpened( UUID workflowId ) {
        return userSessions.values().stream().anyMatch( s -> s.getWId().equals( workflowId ) )
                || nestedSessions.values().stream().anyMatch( s -> s.getWId().equals( workflowId ) )
                || jobSessions.values().stream().anyMatch( s -> s.getTrigger().getWorkfowId().equals( workflowId ) );
    }


    private void removeSession( UUID sId ) {
        userSessions.remove( sId );
        apiSessions.remove( sId );
        jobSessions.remove( sId );
    }


    public void removeNestedSession( UUID sId ) {
        nestedSessions.remove( sId );
    }


    private UUID registerUserSession( Workflow wf, UUID wId, int version ) throws WorkflowRepoException {
        UUID sId = UUID.randomUUID();
        UserSession session = new UserSession( sId, wf, wId, version, repo.getWorkflowDef( wId ) );
        userSessions.put( sId, session );
        return sId;
    }


    private UUID registerApiSession( Workflow wf ) {
        UUID sId = UUID.randomUUID();
        ApiSession session = new ApiSession( sId, wf );
        apiSessions.put( sId, session );
        return sId;
    }


    public UUID registerJobSession( Workflow wf, JobTrigger trigger ) throws WorkflowRepoException {
        UUID sId = UUID.randomUUID();
        JobSession session = new JobSession( sId, wf, trigger, repo.getWorkflowDef( trigger.getWorkfowId() ) );
        jobSessions.put( sId, session );
        return sId;
    }


    /**
     * Unlike other session types, nested sessions are not managed by the sessionManager itself.
     * It only keeps a reference to it.
     */
    public void putNestedSession( NestedSession session ) {
        this.nestedSessions.put( session.sessionId, session );
    }


    private static Map<UUID, SessionModel> toSessionModelMap( Map<UUID, ? extends AbstractSession> sessions ) {
        return sessions.entrySet().stream()
                .collect( Collectors.toMap(
                        Entry::getKey,
                        entry -> entry.getValue().toModel()
                ) );
    }

}
