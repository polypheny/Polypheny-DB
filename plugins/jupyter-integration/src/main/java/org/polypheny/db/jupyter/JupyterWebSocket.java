/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.jupyter;

import io.javalin.websocket.WsCloseContext;
import io.javalin.websocket.WsConfig;
import io.javalin.websocket.WsConnectContext;
import io.javalin.websocket.WsMessageContext;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.polypheny.db.jupyter.model.JupyterExecutionRequest;
import org.polypheny.db.jupyter.model.JupyterKernel;
import org.polypheny.db.jupyter.model.JupyterSessionManager;

@WebSocket
@Slf4j
public class JupyterWebSocket implements Consumer<WsConfig> {

    private final JupyterSessionManager jsm = JupyterSessionManager.getInstance();

    private static final Queue<Session> sessions = new ConcurrentLinkedQueue<>();
    private static final Map<Session, JupyterKernel> kernels = new ConcurrentHashMap<>();


    public void connected( final WsConnectContext ctx ) {
        log.error( "Opening websocket." );
        String kernelId = ctx.pathParam( "kernelId" );
        JupyterKernel kernel = jsm.getKernel( kernelId );
        if ( kernel == null ) {
            ctx.closeSession( 1000, "Unknown kernel" );
            return;
        }
        kernel.subscribe( ctx.session );
        kernels.put( ctx.session, kernel );
    }


    @OnWebSocketMessage
    public void onMessage( final WsMessageContext ctx ) {
        JupyterExecutionRequest request = ctx.messageAsClass( JupyterExecutionRequest.class );
        switch ( request.type ) {
            case "code":
                JupyterKernel kernel = kernels.get( ctx.session );
                if ( kernel != null ) {
                    kernel.execute( request.content, request.uuid );
                }
                break;
            case "poly":
                log.error( "TODO: Handle Polypheny Cells" );
                break;
        }
    }


    public void closed( WsCloseContext ctx ) {
        log.error( "Closing websocket." );
        kernels.get( ctx.session ).unsubscribe( ctx.session );
        kernels.remove( ctx.session );
    }


    @Override
    public void accept( WsConfig wsConfig ) {
        wsConfig.onConnect( this::connected );
        wsConfig.onMessage( this::onMessage );
        wsConfig.onClose( this::closed );
    }

}
