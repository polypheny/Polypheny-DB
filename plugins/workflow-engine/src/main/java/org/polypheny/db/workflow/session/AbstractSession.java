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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import lombok.Getter;
import org.eclipse.jetty.websocket.api.Session;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.models.SessionModel;
import org.polypheny.db.workflow.models.websocket.CreateActivityRequest;
import org.polypheny.db.workflow.models.websocket.DeleteActivityRequest;
import org.polypheny.db.workflow.models.websocket.WsRequest;


public abstract class AbstractSession {

    @Getter
    final Workflow wf;
    final UUID sId;
    private final Set<Session> subscribers = new HashSet<>();


    protected AbstractSession( Workflow wf, UUID sId ) {
        this.wf = wf;
        this.sId = sId;
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


    public UUID getId() {
        return sId;
    }


    public abstract SessionModel toModel();


    public void handleRequest( CreateActivityRequest request ) {
        throwUnsupported( request );
    }


    public void handleRequest( DeleteActivityRequest request ) {
        throwUnsupported( request );
    }


    private void throwUnsupported( WsRequest request ) {
        throw new UnsupportedOperationException( "This session type does not support " + request.type + " requests." );
    }

}
