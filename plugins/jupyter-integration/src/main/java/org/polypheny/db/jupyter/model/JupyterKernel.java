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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JupyterKernel {

    @Getter
    private final String name, kernelId, clientId;
    private final WebSocket webSocket;
    private final Set<JupyterKernelSubscriber> subscribers = new HashSet<>();


    public JupyterKernel( String kernelId, String name, WebSocket.Builder builder, String host) {
        this.kernelId = kernelId;
        this.name = name;
        this.clientId = UUID.randomUUID().toString();

        String url = "ws://" + host + "/api/kernels/" + this.kernelId + "/channels?session_id=" + clientId;

        this.webSocket = builder.buildAsync( URI.create( url ), new WebSocketClient() ).join();

    }


    public void subscribe( JupyterKernelSubscriber session ) {
        subscribers.add( session );
    }


    public void unsubscribe( JupyterKernelSubscriber session ) {
        subscribers.remove( session );
    }


    private void handleText( CharSequence data ) {
        log.warn( "Received Text: {}", data );
        subscribers.forEach( s -> s.onText( data ) );
    }


    public void execute( String code ) {
        log.warn( "Sending code: {}", code );
        ByteBuffer request = buildExecutionRequest( code, false, false, true );
        webSocket.sendBinary( request, true );
    }


    private ByteBuffer buildExecutionRequest( String code, boolean silent, boolean allowStdin, boolean stopOnError ) {
        JsonObject header = new JsonObject();
        header.addProperty( "msg_id", UUID.randomUUID().toString() );
        header.addProperty( "msg_type", "execute_request" );
        header.addProperty( "version", "5.4" );

        JsonObject content = new JsonObject();
        content.addProperty( "silent", silent );
        content.addProperty( "allow_stdin", allowStdin );
        content.addProperty( "stop_on_error", stopOnError );
        content.addProperty( "code", code );

        return buildMessage( "shell", header, new JsonObject(), new JsonObject(), content );
    }


    private ByteBuffer buildMessage( String channel, JsonObject header, JsonObject parentHeader, JsonObject metadata, JsonObject content ) {
        JsonObject msg = new JsonObject();
        msg.addProperty( "channel", channel );
        msg.add( "header", header );
        msg.add( "parent_header", parentHeader );
        msg.add( "metadata", metadata );
        msg.add( "content", content );
        byte[] byteMsg = msg.toString().getBytes( StandardCharsets.UTF_8 );

        ByteBuffer bb = ByteBuffer.allocate( (byteMsg.length + 8) );
        bb.putInt( 1 ); // nbufs: no. of msg parts (= no. of offsets)
        bb.putInt( 8 ); // offset_0: position of msg in bytes
        bb.put( byteMsg );
        bb.rewind();
        return bb;
    }

    public void close() {
        webSocket.abort();
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
