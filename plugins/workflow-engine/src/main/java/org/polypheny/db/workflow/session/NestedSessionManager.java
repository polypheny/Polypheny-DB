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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.WorkflowImpl;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.models.WorkflowModel;
import org.polypheny.db.workflow.repo.WorkflowRepo;
import org.polypheny.db.workflow.repo.WorkflowRepoImpl;

@Slf4j
public class NestedSessionManager {

    private final WorkflowRepo repo = WorkflowRepoImpl.getInstance();
    private final SessionManager sessionManager = SessionManager.getInstance();

    private final Map<UUID, NestedSession> sessions = new ConcurrentHashMap<>(); // map activityIds to NestedSession
    private final Set<Pair<UUID, Integer>> parentIds; // workflowId, workflowVersion

    @Getter
    private final boolean isNested;
    // fields important if itself a nested session:
    private final Map<UUID, CheckpointReader> inputs = new HashMap<>();
    @Getter
    @Setter
    private Map<String, JsonNode> inDynamicVars;
    @Getter
    @Setter
    private Map<String, JsonNode> inEnvVars;
    @Getter
    @Setter
    private Map<String, JsonNode> outDynamicVars;


    public NestedSessionManager( Set<Pair<UUID, Integer>> parentWorkflowIds, boolean isNested ) {
        this.parentIds = Set.copyOf( parentWorkflowIds );
        this.isNested = isNested;
    }


    public NestedSession getSessionForActivity( UUID activityId ) {
        return sessions.get( activityId );
    }


    public NestedSession createSession( UUID activityId, UUID workflowId, int version ) throws Exception {
        if ( isCyclic( workflowId, version ) ) {
            throw new GenericRuntimeException( "Detected cycle in nested workflow graph: " + workflowId + "_v" + version + " cannot be its own successor." );
        }
        if ( sessions.containsKey( activityId ) ) {
            NestedSession session = sessions.get( activityId );
            throw new GenericRuntimeException( "Activity " + activityId + "already has a nested session running for workflow: " + session.getWId() + "_v" + session.getOpenedVersion() );
        }

        WorkflowModel model = repo.readVersion( workflowId, version );
        try {
            UUID sId = UUID.randomUUID();
            NestedSession session = new NestedSession( sId, WorkflowImpl.fromModel( model ), workflowId, version, repo.getWorkflowDef( workflowId ), parentIds );
            sessions.put( activityId, session );
            sessionManager.putNestedSession( session );
            return session;
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Unable to instantiate nested workflow: " + e.getMessage(), e );
        }
    }


    public void terminateSession( UUID activityId ) throws Exception {
        NestedSession session = sessions.get( activityId );
        if ( session != null ) {
            try {
                session.terminate();
            } catch ( Exception e ) {
                throw new GenericRuntimeException( "Unable to terminate nested session: " + e.getMessage(), e );
            }
            sessions.remove( activityId );
            sessionManager.removeNestedSession( session.sessionId );
        }
    }


    public void terminateAll() {
        for ( UUID activityId : sessions.keySet() ) {
            try {
                terminateSession( activityId );
            } catch ( Exception e ) {
                log.warn( "Unable to terminate nested session", e );
            }
        }
        sessions.clear();
    }


    public boolean isCyclic( UUID workflowId, int version ) {
        return parentIds.contains( Pair.of( workflowId, version ) );
    }


    public void setInput( UUID activityId, CheckpointReader reader ) {
        inputs.put( activityId, reader );
    }


    public CheckpointReader removeInput( UUID activityId ) {
        return inputs.remove( activityId );
    }


    public void clearInputs() {
        inputs.clear();
        inDynamicVars = null;
        inEnvVars = null;
        outDynamicVars = null;
    }

}
