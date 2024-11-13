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

import io.javalin.websocket.WsCloseContext;
import io.javalin.websocket.WsConfig;
import io.javalin.websocket.WsConnectContext;
import io.javalin.websocket.WsMessageContext;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.polypheny.db.workflow.models.websocket.CreateActivityRequest;
import org.polypheny.db.workflow.models.websocket.DeleteActivityRequest;
import org.polypheny.db.workflow.models.websocket.WsRequest;

@WebSocket
public class WorkflowWebSocket implements Consumer<WsConfig> {

    // maps websocket session to workflow session
    private final SessionManager sm = SessionManager.getInstance();
    private static final Map<Session, AbstractSession> sessions = new ConcurrentHashMap<>();


    public void connected( final WsConnectContext ctx ) {
        UUID sessionId = UUID.fromString( ctx.pathParam( "sessionId" ) );
        AbstractSession session = sm.getSession( sessionId );
        if ( session == null ) {
            ctx.closeSession( 1000, "Unknown session" );
            return;
        }
        session.subscribe( ctx.session );
        sessions.put( ctx.session, session );
    }


    @OnWebSocketMessage
    public void onMessage( final WsMessageContext ctx ) {
        AbstractSession session = sessions.get( ctx.session );
        WsRequest baseRequest = ctx.messageAsClass( WsRequest.class );
        System.out.println( "Received message with id: " + baseRequest.msgId );

        switch ( baseRequest.type ) {
            case CREATE_ACTIVITY -> session.handleRequest( ctx.messageAsClass( CreateActivityRequest.class ) );
            case DELETE_ACTIVITY -> session.handleRequest( ctx.messageAsClass( DeleteActivityRequest.class ) );
            default -> throw new IllegalArgumentException( "Received request with unknown type!" );
        }
    }


    public void closed( WsCloseContext ctx ) {
        sessions.get( ctx.session ).unsubscribe( ctx.session );
        sessions.remove( ctx.session );
    }


    @Override
    public void accept( WsConfig wsConfig ) {
        wsConfig.onConnect( this::connected );
        wsConfig.onMessage( this::onMessage );
        wsConfig.onClose( this::closed );
    }

}
