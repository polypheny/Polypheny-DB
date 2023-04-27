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

package org.polypheny.db.jupyter.model;

import com.google.gson.JsonObject;
import java.net.URI;
import java.net.http.WebSocket;
import java.net.http.WebSocket.Listener;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.jupyter.JupyterClient;

@Slf4j
public class JupyterKernel {

    @Getter
    private final String name, kernelId, clientId;
    private final WebSocket webSocket;
    private final Set<JupyterKernelSubscriber> subscribers = new HashSet<>();


    public JupyterKernel( String kernelId, String name, WebSocket.Builder builder ) {
        this.kernelId = kernelId;
        this.name = name;
        this.clientId = UUID.randomUUID().toString();

        String url = JupyterClient.WS_URL + "kernels/" + this.kernelId + "/channels?session_id=" + clientId;

        this.webSocket = builder.buildAsync( URI.create( url ), new WebSocketClient() ).join();

    }


    public void subscribe( JupyterKernelSubscriber session ) {
        subscribers.add( session );
    }


    public void unsubscribe( JupyterKernelSubscriber session ) {
        subscribers.remove( session );
    }


    private void handleText( CharSequence data ) {
        subscribers.forEach( s -> s.onText( data ) );
        log.warn( "Received Text: {}", data );

    }


    public void execute( String code ) {
        String request = generateExecutionRequest( code );
        log.warn( "Sending code: {}", request );
        webSocket.sendText( request, true );
    }


    private String generateExecutionRequest( String code ) {

        JsonObject content = new JsonObject();
        content.addProperty( "silent", false );
        content.addProperty( "code", code );

        JsonObject header = new JsonObject();
        header.addProperty( "msg_id", UUID.randomUUID().toString() );
        header.addProperty( "msg_type", "execute_request" );

        JsonObject body = new JsonObject();
        body.addProperty( "channel", "shell" );
        body.add( "content", content );
        body.add( "header", header );
        body.add( "metadata", new JsonObject() );
        body.add( "parent_header", new JsonObject() );
        return body.toString();
    }


    private class WebSocketClient implements WebSocket.Listener {

        @Override
        public void onOpen( WebSocket webSocket ) {
            log.warn( "Opened Websocket" );
            WebSocket.Listener.super.onOpen( webSocket );
        }


        @Override
        public CompletionStage<?> onText( WebSocket webSocket, CharSequence data, boolean last ) {
            handleText( data );
            return WebSocket.Listener.super.onText( webSocket, data, last );
        }


        @Override
        public CompletionStage<?> onClose( WebSocket webSocket, int statusCode, String reason ) {
            subscribers.forEach( JupyterKernelSubscriber::onClose );
            JupyterSessionManager.getInstance().removeKernel( kernelId );
            return Listener.super.onClose( webSocket, statusCode, reason );
        }


        @Override
        public void onError( WebSocket webSocket, Throwable error ) {
            Listener.super.onError( webSocket, error );
        }

    }

}
