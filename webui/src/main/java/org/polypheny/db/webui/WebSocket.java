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


import com.google.gson.Gson;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.webui.models.Result;
import org.polypheny.db.webui.models.requests.QueryRequest;
import org.polypheny.db.webui.models.requests.RelAlgRequest;
import org.polypheny.db.webui.models.requests.UIRequest;


@org.eclipse.jetty.websocket.api.annotations.WebSocket
@Slf4j
public class WebSocket {

    private static final Queue<Session> sessions = new ConcurrentLinkedQueue<>();
    private final Crud crud;
    private final HashMap<Session, Set<String>> queryAnalyzers = new HashMap<>();
    private static final Gson gson = new Gson();


    WebSocket( Crud crud ) {
        this.crud = crud;
    }


    @OnWebSocketConnect
    public void connected( final Session session ) {
        log.debug( "UI connected to websocket" );
        sessions.add( session );
    }


    @OnWebSocketClose
    public void closed( final Session session, final int statusCode, final String reason ) {
        log.debug( "UI disconnected from websocket" );
        sessions.remove( session );
        cleanup( session );
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
            log.error( "Could not send websocket message to UI", e );
        }
    }


    public static void sendMessage( final Session session, final Object message ) {
        sendMessage( session, gson.toJson( message ) );
    }


    @OnWebSocketMessage
    public void onMessage( Session session, String message ) {
        if ( message.equals( "\"keepalive\"" ) ) {
            return;
        }
        //close analyzers of a previous query that was sent over the same socket.
        cleanup( session );

        Gson gson = new Gson();
        UIRequest request = gson.fromJson( message, UIRequest.class );
        Set<String> xIds = new HashSet<>();
        switch ( request.requestType ) {
            case "QueryRequest":
                QueryRequest queryRequest = gson.fromJson( message, QueryRequest.class );
                List<Result> results;
                if ( queryRequest.language.equals( "mql" ) ) {
                    try {
                        results = crud.documentCrud.anyQuery( session, queryRequest, crud );
                    } catch ( Throwable t ) {
                        sendMessage( session, new Result[]{ new Result( t ) } );
                        return;
                    }
                } else {

                    try {
                        results = crud.anyQuery( queryRequest, session );
                    } catch ( Throwable t ) {
                        sendMessage( session, new Result[]{ new Result( t ) } );
                        return;
                    }
                }
                for ( Result result : results ) {
                    if ( result.getXid() != null ) {
                        xIds.add( result.getXid() );
                    }
                }
                sendMessage( session, results );
                break;

            case "RelAlgRequest":
            case "TableRequest":
                Result result;
                if ( request.requestType.equals( "RelAlgRequest" ) ) {
                    RelAlgRequest relAlgRequest = gson.fromJson( message, RelAlgRequest.class );
                    try {
                        result = crud.executeRelAlg( relAlgRequest, session );
                    } catch ( Throwable t ) {
                        sendMessage( session, new Result( t ) );
                        return;
                    }
                } else {//TableRequest, is equal to UIRequest
                    UIRequest uiRequest = gson.fromJson( message, UIRequest.class );
                    try {
                        result = crud.getTable( uiRequest );
                    } catch ( Throwable t ) {
                        sendMessage( session, new Result( t ) );
                        return;
                    }
                }
                if ( result.getXid() != null ) {
                    xIds.add( result.getXid() );
                }
                sendMessage( session, result );
                break;
            default:
                throw new RuntimeException( "Unexpected websocket request: " + request.requestType );
        }
        queryAnalyzers.put( session, xIds );
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

}
