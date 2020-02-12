/*
 * Copyright 2019-2020 The Polypheny Project
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

package ch.unibas.dmi.dbis.polyphenydb.webui;


import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;


@org.eclipse.jetty.websocket.api.annotations.WebSocket
@Slf4j
public class WebSocket {

    private static final Queue<Session> sessions = new ConcurrentLinkedQueue<>();


    @OnWebSocketConnect
    public void connected( final Session session ) {
        log.debug( "UI connected to websocket" );
        sessions.add( session );
    }


    @OnWebSocketClose
    public void closed( final Session session, final int statusCode, final String reason ) {
        log.debug( "UI disconnected from websocket" );
        sessions.remove( session );
    }


    /**
     * Send updated pageList.
     */
    public static synchronized void sendPageList( final String pageList ) throws IOException {
        for ( Session s : sessions ) {
            s.getRemote().sendString( pageList );
        }
    }


    /**
     * Send changed Information Object as Json via the WebSocket to the GUI.
     */
    public static synchronized void broadcast( final String msg ) throws IOException {
        for ( Session s : sessions ) {
            s.getRemote().sendString( msg );
        }
    }


}
