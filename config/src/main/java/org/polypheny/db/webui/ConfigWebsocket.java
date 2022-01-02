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

package org.polypheny.db.webui;


import io.javalin.websocket.WsConfig;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;


@WebSocket
@Slf4j
public class ConfigWebsocket implements Consumer<WsConfig> {

    private static final Queue<Session> sessions = new ConcurrentLinkedQueue<>();


    public static void broadcast( final String msg ) throws IOException {
        log.trace( "broadcasting:\n{}", msg );
        for ( Session s : sessions ) {
            s.getRemote().sendString( msg );
        }
    }


    @Override
    public void accept( WsConfig wsConfig ) {
        wsConfig.onConnect( wsConnectContext -> sessions.add( wsConnectContext.session ) );
        wsConfig.onMessage( wsMessageContext -> wsMessageContext.session.getRemote().sendString( wsMessageContext.message() ) );
        wsConfig.onClose( wsCloseContext -> sessions.remove( wsCloseContext.session ) );
    }

}
