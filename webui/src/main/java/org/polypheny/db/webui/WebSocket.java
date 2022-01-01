/*
 * Copyright 2019-2021 The Polypheny Project
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


import com.google.gson.Gson;
import io.javalin.websocket.WsCloseContext;
import io.javalin.websocket.WsConfig;
import io.javalin.websocket.WsConnectContext;
import io.javalin.websocket.WsMessageContext;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.Result;
import org.polypheny.db.webui.models.requests.QueryRequest;
import org.polypheny.db.webui.models.requests.RelAlgRequest;
import org.polypheny.db.webui.models.requests.UIRequest;


@org.eclipse.jetty.websocket.api.annotations.WebSocket
@Slf4j
public class WebSocket implements Consumer<WsConfig> {

    private static final Queue<Session> sessions = new ConcurrentLinkedQueue<>();
    private final Crud crud;
    private final HashMap<Session, Set<String>> queryAnalyzers = new HashMap<>();
    private final Gson gson;


    WebSocket( Crud crud, Gson gson ) {
        this.crud = crud;
        this.gson = gson;
    }


    public void connected( final WsConnectContext ctx ) {
        log.debug( "UI connected to WebSocket" );
        sessions.add( ctx.session );
    }


    public void closed( WsCloseContext ctx ) {
        log.debug( "UI disconnected from WebSocket" );
        sessions.remove( ctx.session );
        cleanup( ctx.session );
    }


    /**
     * Send changed Information Object as Json via the WebSocket to the GUI.
     */
    public static synchronized void broadcast( final String msg ) throws IOException {
        for ( Session s : sessions ) {
            s.getRemote().sendString( msg );
        }
    }


    public static void sendMessage( Session session, String message ) {
        try {
            session.getRemote().sendString( message );
        } catch ( IOException e ) {
            log.error( "Could not send WebSocket message to UI", e );
        }
    }


    @OnWebSocketMessage
    public void onMessage( final WsMessageContext ctx ) {
        if ( ctx.message().equals( "\"keepalive\"" ) ) {
            return;
        }
        //close analyzers of a previous query that was sent over the same socket.
        cleanup( ctx.session );

        UIRequest request = ctx.messageAsClass( UIRequest.class );
        Set<String> xIds = new HashSet<>();
        switch ( request.requestType ) {
            case "QueryRequest":
                QueryRequest queryRequest = ctx.messageAsClass( QueryRequest.class );
                QueryLanguage language = QueryLanguage.from( queryRequest.language );

                List<Result> results = LanguageCrud.anyQuery(
                        language,
                        ctx.session,
                        queryRequest,
                        crud.getTransactionManager(),
                        crud.getUserName(),
                        crud.getDatabaseName(),
                        crud );

                for ( Result result : results ) {
                    if ( result.getXid() != null ) {
                        xIds.add( result.getXid() );
                    }
                }
                ctx.send( results );
                break;

            case "RelAlgRequest":
            case "TableRequest":
                Result result;
                if ( request.requestType.equals( "RelAlgRequest" ) ) {
                    RelAlgRequest relAlgRequest = ctx.messageAsClass( RelAlgRequest.class );
                    try {
                        result = crud.executeRelAlg( relAlgRequest, ctx.session );
                    } catch ( Throwable t ) {
                        ctx.send( new Result( t ) );
                        return;
                    }
                } else {//TableRequest, is equal to UIRequest
                    UIRequest uiRequest = ctx.messageAsClass( UIRequest.class );
                    try {
                        result = crud.getTable( uiRequest );
                    } catch ( Throwable t ) {
                        ctx.send( new Result( t ) );
                        return;
                    }
                }
                if ( result.getXid() != null ) {
                    xIds.add( result.getXid() );
                }
                ctx.send( result );
                break;
            default:
                throw new RuntimeException( "Unexpected WebSocket request: " + request.requestType );
        }
        queryAnalyzers.put( ctx.session, xIds );
    }


    /**
     * Closes queryAnalyzers and deletes temporary files.
     */
    private void cleanup( final Session session ) {
        Set<String> xIds = queryAnalyzers.remove( session );
        if ( xIds == null || xIds.size() == 0 ) {
            return;
        }
        for ( String xId : xIds ) {
            InformationManager.close( xId );
            TemporalFileManager.deleteFilesOfTransaction( xId );
        }
    }


    @Override
    public void accept( WsConfig wsConfig ) {
        wsConfig.onConnect( this::connected );
        wsConfig.onMessage( this::onMessage );

        wsConfig.onClose( this::closed );
    }

}
