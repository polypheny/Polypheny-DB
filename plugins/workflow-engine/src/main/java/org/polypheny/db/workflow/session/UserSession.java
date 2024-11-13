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

import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.models.SessionModel;
import org.polypheny.db.workflow.models.SessionModel.SessionModelType;
import org.polypheny.db.workflow.models.websocket.CreateActivityRequest;
import org.polypheny.db.workflow.models.websocket.DeleteActivityRequest;

public class UserSession extends AbstractSession {

    @Getter
    private final UUID wId;
    @Getter
    @Setter
    private int openedVersion;


    public UserSession( UUID sessionId, Workflow wf, UUID workflowId, int openedVersion ) {
        super( wf, sessionId );
        this.wId = workflowId;
        this.openedVersion = openedVersion;
    }


    @Override
    public void terminate() {

    }


    @Override
    public synchronized void handleRequest( CreateActivityRequest request ) {
        throwIfNotEditable();
        throw new NotImplementedException();
        //Activity activity = Activity.fromType(request.activityType);
        //wf.addActivity(activity);

        // send updates to subscriber

    }


    @Override
    public synchronized void handleRequest( DeleteActivityRequest request ) {
        // TODO: propagate state changes and reset of nodes in workflow
        throwIfNotEditable();
        wf.deleteActivity( request.targetId );
        throw new NotImplementedException();
    }


    @Override
    public SessionModel toModel() {
        return new SessionModel( SessionModelType.USER_SESSION, sId, wId, openedVersion );
    }


    private boolean isEditable() {
        // While we could perform this check within the workflow, we follow the approach where the workflow is not
        // aware of the semantic meaning of its state.
        return wf.getState() == WorkflowState.IDLE;
    }


    private void throwIfNotEditable() {
        if ( !isEditable() ) {
            throw new GenericRuntimeException( "Workflow is currently not editable." );
        }
    }

}
