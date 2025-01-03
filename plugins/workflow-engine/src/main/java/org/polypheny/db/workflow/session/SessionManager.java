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

import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.WorkflowImpl;
import org.polypheny.db.workflow.models.SessionModel;
import org.polypheny.db.workflow.models.WorkflowModel;
import org.polypheny.db.workflow.repo.WorkflowRepo;
import org.polypheny.db.workflow.repo.WorkflowRepo.WorkflowRepoException;
import org.polypheny.db.workflow.repo.WorkflowRepoImpl;

public class SessionManager {

    private static SessionManager INSTANCE = null;

    private final Map<UUID, UserSession> userSessions = new ConcurrentHashMap<>();
    private final Map<UUID, ApiSession> apiSessions = new ConcurrentHashMap<>();
    private final WorkflowRepo repo = WorkflowRepoImpl.getInstance();


    private SessionManager() {
    }


    public static SessionManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new SessionManager();
        }
        return INSTANCE;
    }


    public UUID createUserSession( String newWorkflowName ) throws WorkflowRepoException {
        UUID wId = repo.createWorkflow( newWorkflowName );
        UUID sessionId = registerUserSession( new WorkflowImpl(), wId, 0 );
        saveUserSession( sessionId, "Initial Save" ); // TODO: remove initial save, delete workflow if its session is stopped without a saved version
        return registerUserSession( new WorkflowImpl(), wId, 0 );
    }


    public UUID createUserSession( UUID wId, int version ) throws WorkflowRepoException {
        WorkflowModel model = repo.readVersion( wId, version );
        return registerUserSession( WorkflowImpl.fromModel( model ), wId, version );
    }


    public Map<UUID, SessionModel> getUserSessionModels() {
        return toSessionModelMap( userSessions );
    }


    public Map<UUID, SessionModel> getApiSessionModels() {
        return toSessionModelMap( apiSessions );
    }


    public Map<UUID, SessionModel> getSessionModels() {
        Map<UUID, SessionModel> userSessionModels = getUserSessionModels();
        Map<UUID, SessionModel> apiSessionModels = getApiSessionModels();
        userSessionModels.putAll( apiSessionModels );
        return userSessionModels;
    }


    public SessionModel getSessionModel( UUID sId ) {
        return getSessionOrThrow( sId ).toModel();
    }


    public void terminateSession( UUID sId ) {
        getSessionOrThrow( sId ).terminate();
        removeSession( sId );
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


    public UserSession getUserSessionOrThrow( UUID sId ) {
        UserSession session = userSessions.get( sId );
        if ( session == null ) {
            throw new IllegalArgumentException( "Unknown user session with id: " + sId );
        }
        return session;
    }


    private boolean removeSession( UUID sId ) {
        return userSessions.remove( sId ) == null || apiSessions.remove( sId ) == null;
    }


    private UUID registerUserSession( Workflow wf, UUID wId, int version ) throws WorkflowRepoException {
        UUID sId = UUID.randomUUID();
        UserSession session = new UserSession( sId, wf, wId, version, repo.getWorkflowDef( wId ) );
        userSessions.put( sId, session );
        return sId;
    }


    private static Map<UUID, SessionModel> toSessionModelMap( Map<UUID, ? extends AbstractSession> sessions ) {
        return sessions.entrySet().stream()
                .collect( Collectors.toMap(
                        Entry::getKey,
                        entry -> entry.getValue().toModel()
                ) );
    }

}
