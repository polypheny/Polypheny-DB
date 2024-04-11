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

package org.polypheny.db.notebooks.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.net.URI;
import java.net.http.WebSocket;
import java.net.http.WebSocket.Listener;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.notebooks.model.language.JupyterKernelLanguage;
import org.polypheny.db.notebooks.model.language.JupyterKernelLanguage.JupyterQueryPart;
import org.polypheny.db.notebooks.model.language.JupyterLanguageFactory;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.requests.QueryRequest;
import org.polypheny.db.webui.models.results.Result;

/**
 * Instances correspond to running Kernels in the Jupyter Server docker container.
 * Handles messaging from and to the Kernel, adhering to the
 * <a href="https://jupyter-client.readthedocs.io/en/stable/messaging.html">Jupyter message specification</a> (v5.4).
 */
@Slf4j
public class JupyterKernel {

    @Getter
    private final String name, kernelId, clientId;
    private final JupyterKernelLanguage kernelLanguage;
    @Getter
    private final boolean supportsPolyCells;
    private final WebSocket webSocket;
    private final Set<Session> subscribers = new HashSet<>();
    private final Gson gson = new Gson();
    private final Gson resultSetGson;
    private final JsonObject statusMsg;
    private boolean isRestarting = false;
    private final Map<String, ActivePolyCell> activePolyCells = new ConcurrentHashMap<>();
    private final JupyterSessionManager jsm = JupyterSessionManager.getInstance();


    /**
     * Create a new JupyterKernel and open a websocket connection to the corresponding kernel in the jupyter server.
     *
     * @param kernelId the ID of the kernel
     * @param name the unique name of the Kernel, defining the programming language
     * @param builder the Websocket.Builder used to open the websocket
     * @param host the address of the Docker container running the jupyter server
     */
    public JupyterKernel( String kernelId, String name, WebSocket.Builder builder, String host ) {
        this.kernelId = kernelId;
        this.name = name;
        this.clientId = UUID.randomUUID().toString();
        this.kernelLanguage = JupyterLanguageFactory.getKernelLanguage( name );

        this.supportsPolyCells = this.kernelLanguage != null;

        String url = "ws://" + host + "/api/kernels/" + this.kernelId + "/channels?session_id=" + clientId;

        this.webSocket = builder.buildAsync( URI.create( url ), new WebSocketClient() ).join();

        this.statusMsg = new JsonObject();
        this.statusMsg.addProperty( "msg_type", "status" );
        JsonObject content = new JsonObject();
        content.addProperty( "execution_state", "starting" );
        this.statusMsg.add( "content", content );
        sendInitCode();

        GsonBuilder gsonBuilder = new GsonBuilder();
        resultSetGson = gsonBuilder.create();
    }


    /**
     * Subscribe to any incoming messages from the kernel.
     *
     * @param session the UI websocket session to be added to the list of subscribers
     */
    public void subscribe( Session session ) {
        subscribers.add( session );
    }


    public void unsubscribe( Session session ) {
        subscribers.remove( session );
    }


    /**
     * Handle an incoming message from the kernel, according to its message type.
     *
     * @param data the data that makes up the message
     */
    private void handleText( CharSequence data ) {
        String dataStr = data.toString();
        if ( dataStr.length() < 10000 ) { // reduce number of large json strings that need to be parsed
            try {
                JsonObject json = gson.fromJson( dataStr, JsonObject.class );
                String msgType = json.get( "msg_type" ).getAsString();
                if ( msgType.equals( "status" ) ) {
                    statusMsg.add( "content", json.getAsJsonObject( "content" ) );
                } else if ( msgType.equals( "shutdown_reply" ) ) {
                    handleShutdownReply( json );
                } else if ( !activePolyCells.isEmpty() && msgType.equals( "input_request" ) ) {
                    handleInputRequest( json );
                    return; // do not forward the query requests to the UI
                }
            } catch ( JsonSyntaxException ignored ) {

            }
        }

        subscribers.forEach( s -> {
            try {
                s.getRemote().sendString( dataStr );
            } catch ( IOException e ) {
                s.close();
            }
        } );
    }


    private void handleShutdownReply( JsonObject json ) {
        JsonObject content = json.getAsJsonObject( "content" );
        String status = content.get( "status" ).getAsString();
        boolean restart = content.get( "restart" ).getAsBoolean();
        activePolyCells.clear();
        if ( status.equals( "ok" ) && restart ) {
            isRestarting = true;
        }
    }


    /**
     * We use input_request messages to execute queries. If an ActivePolyCell exists with the corresponding parent msg_id,
     * the query specified in the prompt field is executed, else the request is ignored.
     * The serialized result is sent back as an input_reply message.
     *
     * @param json serialized JSON object as specified by the jupyter kernel messaging protocol
     */
    private void handleInputRequest( JsonObject json ) {
        String query = json.getAsJsonObject( "content" ).get( "prompt" ).getAsString();
        JsonObject parentHeader = json.getAsJsonObject( "parent_header" );
        ActivePolyCell apc = activePolyCells.remove( parentHeader.get( "msg_id" ).getAsString() );
        if ( apc == null ) {
            return;
        }
        Optional<LogicalNamespace> namespace = Catalog.snapshot().getNamespace( apc.namespace );
        String result = anyQuery( query, apc.language, namespace.orElseThrow().name );
        ByteBuffer request = buildInputReply( result, parentHeader );
        webSocket.sendBinary( request, true );
    }


    public String getStatusMessage() {
        return statusMsg.toString();
    }


    /**
     * Send the specified code for execution to the jupyter server.
     * Code that tries to request input from the user via stdin is not supported.
     *
     * @param code the programming code to be executed
     * @param uuid a unique ID that allows assigning this request to the responses
     */
    public void execute( String code, String uuid ) {
        ByteBuffer request = buildExecutionRequest( code, uuid, false, false, true );
        webSocket.sendBinary( request, true );
    }


    /**
     * Initiate the execution of a query cell by first sending the query to the kernel
     * (necessary for possible variable expansion).
     * If this is the first query execution since the kernel was started, the initialization code is first executed.
     *
     * @param query the query to be executed
     * @param uuid a unique ID that allows assigning this request to the responses
     * @param language the query language to be used
     * @param namespace the target namespace
     * @param variable the name of the variable the result will be stored in
     * @param expandParams whether to expand variables specified in the form ${varname}
     */
    public void executePolyCell(
            String query, String uuid, String language, String namespace,
            String variable, boolean expandParams ) {
        if ( query.strip().isEmpty() ) {
            execute( "", uuid ); // properly terminates the request
            return;
        }
        if ( isRestarting ) {
            sendInitCode();
            isRestarting = false;
        }
        try {
            List<JupyterQueryPart> queries = kernelLanguage.transformToQuery( query, language, namespace, variable, expandParams );
            activePolyCells.put( uuid, new ActivePolyCell( language, namespace ) );

            for ( JupyterQueryPart part : queries ) {
                ByteBuffer request = buildExecutionRequest( part.code, uuid, part.silent, part.allowStdin, true );
                webSocket.sendBinary( request, true );
            }
        } catch ( Exception e ) {
            log.error( "Exception while executing a Polypheny Notebook cell.", e );
        }

    }


    /**
     * Execute the specified query and return the serialized results as JSON.
     *
     * @param query the query to be executed
     * @param language the query language to be used
     * @param namespace the target namespace
     * @return the serialized results in JSON format
     */
    private String anyQuery( String query, String language, String namespace ) {
        QueryRequest queryRequest = new QueryRequest( query, false, true, language, namespace );
        List<? extends Result<?, ?>> results = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( QueryLanguage.from( language ) )
                        .origin( "Notebooks" )
                        .transactionManager( jsm.getTransactionManager() )
                        .build(), queryRequest );
        return resultSetGson.toJson( results );
    }


    /**
     * Send initialization code that might be required by the corresponding kernel to support the execution of query cells.
     */
    private void sendInitCode() {
        if ( supportsPolyCells ) {
            List<String> initCode = kernelLanguage.getInitCode();
            if ( initCode == null ) {
                return;
            }
            for ( String code : initCode ) {
                ByteBuffer request = buildExecutionRequest( code );
                webSocket.sendBinary( request, true );
            }
        }
    }


    /**
     * Build an execution request for the given code that can be sent to the kernel
     *
     * @param code the code to be executed
     * @param uuid the unique ID of the request
     * @param silent if true, signals the kernel to execute this code as quietly as possible
     * @param allowStdin if true, code running in the kernel can prompt the user for input
     * @param stopOnError if true, aborts the execution queue if an exception is encountered
     * @return a ByteBuffer representing the request
     */
    private ByteBuffer buildExecutionRequest( String code, String uuid, boolean silent, boolean allowStdin, boolean stopOnError ) {
        JsonObject header = new JsonObject();
        header.addProperty( "msg_id", uuid );
        header.addProperty( "msg_type", "execute_request" );
        header.addProperty( "version", "5.4" );

        JsonObject content = new JsonObject();
        content.addProperty( "silent", silent );
        content.addProperty( "allow_stdin", allowStdin );
        content.addProperty( "stop_on_error", stopOnError );
        content.addProperty( "code", code );

        return buildMessage( "shell", header, new JsonObject(), new JsonObject(), content );
    }


    private ByteBuffer buildExecutionRequest( String code ) {
        return buildExecutionRequest( code, UUID.randomUUID().toString(), true, false, true );
    }


    /**
     * Build a reply to an input request
     *
     * @param reply the reply message
     * @param uuid the unique ID of the message
     * @param parent_header the headerof the input request that initiated this reply
     * @return a ByteBuffer representing the reply
     */
    private ByteBuffer buildInputReply( String reply, String uuid, JsonObject parent_header ) {
        JsonObject header = new JsonObject();
        header.addProperty( "msg_id", uuid );
        header.addProperty( "msg_type", "input_reply" );
        header.addProperty( "version", "5.4" );

        JsonObject content = new JsonObject();
        content.addProperty( "value", reply );

        return buildMessage( "stdin", header, parent_header, new JsonObject(), content );
    }


    private ByteBuffer buildInputReply( String reply, JsonObject parent_header ) {
        return buildInputReply( reply, UUID.randomUUID().toString(), parent_header );
    }


    /**
     * Build a kernel message according to the
     * <a href="https://jupyter-server.readthedocs.io/en/latest/developers/websocket-protocols.html">
     * Default WebSocket protocol
     * </a>.
     *
     * @param channel the socket of the kernel to be used
     * @param header the message header
     * @param parentHeader the parent header
     * @param metadata the message metadata
     * @param content the message content
     * @return a ByteBuffer representing the message
     */
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
        subscribers.forEach( c -> c.close( 1000, "Websocket to Kernel was closed" ) );
        webSocket.abort();
    }


    public String getStatus() {
        return "Input: " + !webSocket.isInputClosed() +
                ", Output: " + !webSocket.isOutputClosed() +
                ", Subscribers: " + subscribers.size();
    }


    public int getSubscriberCount() {
        return subscribers.size();
    }


    private class WebSocketClient implements WebSocket.Listener {

        private final StringBuilder textBuilder = new StringBuilder();


        @Override
        public void onOpen( WebSocket webSocket ) {
            WebSocket.Listener.super.onOpen( webSocket );
        }


        @Override
        public CompletionStage<?> onText( WebSocket webSocket, CharSequence data, boolean last ) {
            textBuilder.append( data );
            if ( last ) {
                handleText( textBuilder.toString() );
                textBuilder.setLength( 0 );
            }
            return WebSocket.Listener.super.onText( webSocket, data, last );
        }


        @Override
        public CompletionStage<?> onClose( WebSocket webSocket, int statusCode, String reason ) {
            jsm.removeKernel( kernelId );
            return Listener.super.onClose( webSocket, statusCode, reason );
        }


        @Override
        public void onError( WebSocket webSocket, Throwable error ) {
            log.error( "Exception in Jupyter WebSocket.", error );
            jsm.removeKernel( kernelId );
            Listener.super.onError( webSocket, error );
        }


    }


    /**
     * Represents a query cell that is currently being executed.
     */
    private static class ActivePolyCell {

        @Getter
        private final String language, namespace;


        public ActivePolyCell( String language, String namespace ) {
            this.language = language;
            this.namespace = namespace;
        }

    }

}
