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

package org.polypheny.db.webui;


import io.javalin.websocket.WsCloseContext;
import io.javalin.websocket.WsConfig;
import io.javalin.websocket.WsConnectContext;
import io.javalin.websocket.WsMessageContext;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.requests.AlgRequest;
import org.polypheny.db.webui.models.requests.GraphRequest;
import org.polypheny.db.webui.models.requests.QueryRequest;
import org.polypheny.db.webui.models.requests.RegisterRequest;
import org.polypheny.db.webui.models.requests.RequestModel;
import org.polypheny.db.webui.models.requests.UIRequest;
import org.polypheny.db.webui.models.results.RelationalResult;
import org.polypheny.db.webui.models.results.Result;


@org.eclipse.jetty.websocket.api.annotations.WebSocket
@Slf4j
public class WebSocket implements Consumer<WsConfig> {

    private static final Queue<Session> sessions = new ConcurrentLinkedQueue<>();
    public static final String POLYPHENY_UI = "Polypheny-UI";
    private final Crud crud;
    private final ConcurrentHashMap<String, Set<String>> queryAnalyzers = new ConcurrentHashMap<>();


    WebSocket( Crud crud ) {
        this.crud = crud;
    }


    public void connected( final WsConnectContext ctx ) {
        log.debug( "UI connected to WebSocket" );
        sessions.add( ctx.session );
    }


    public void closed( WsCloseContext ctx ) {
        log.debug( "UI disconnected from WebSocket" );
        sessions.remove( ctx.session );
        Crud.cleanupOldSession( queryAnalyzers, ctx.getSessionId() );
    }


    /**
     * Send changed Information Object as Json via the WebSocket to the GUI.
     */
    public static synchronized void broadcast( final String msg ) throws IOException {
        for ( Session s : sessions ) {
            s.getRemote().sendString( msg );
        }
    }


    public static synchronized void sendMessage( Session session, String message ) {
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
        Crud.cleanupOldSession( queryAnalyzers, ctx.getSessionId() );

        RequestModel request = ctx.messageAsClass( RequestModel.class );
        Set<String> xIds = new HashSet<>();
        switch ( request.type ) {
            case "GraphRequest":
                GraphRequest graphRequest = ctx.messageAsClass( GraphRequest.class );
                PolyGraph graph = LanguageCrud.getGraph( Catalog.snapshot().getNamespace( graphRequest.namespace ).orElseThrow().name, crud.getTransactionManager(), ctx.session );

                ctx.send( graph.toJson() );

                break;
            case "QueryRequest":
                QueryRequest queryRequest = ctx.messageAsClass( QueryRequest.class );
                QueryLanguage language = QueryLanguage.from( queryRequest.language );

                List<? extends Result<?, ?>> results = LanguageCrud.anyQueryResult(
                        QueryContext.builder()
                                .query( queryRequest.query )
                                .language( language )
                                .isAnalysed( queryRequest.analyze )
                                .usesCache( queryRequest.cache )
                                .namespaceId( LanguageCrud.getNamespaceIdOrDefault( queryRequest.namespace ) )
                                .origin( POLYPHENY_UI )
                                .batch( queryRequest.noLimit ? -1 : crud.getPageSize() )
                                .transactionManager( crud.getTransactionManager() )
                                .informationTarget( i -> i.setSession( ctx.session ) ).build(), queryRequest );

                for ( Result<?, ?> result : results ) {
                    if ( !(result instanceof RelationalResult) ) {
                        continue;
                    }
                    if ( result.xid != null ) {
                        xIds.add( result.xid );
                    }
                }
                ctx.send( results );
                break;
            case "RegisterRequest":
                RegisterRequest registerRequest = ctx.messageAsClass( RegisterRequest.class );
                crud.authCrud.register( registerRequest, ctx );
                break;
            case "RelAlgRequest":
            case "EntityRequest":
                Result<?, ?> result = null;
                if ( request.type.equals( "RelAlgRequest" ) ) {
                    AlgRequest algRequest = ctx.messageAsClass( AlgRequest.class );
                    try {
                        result = crud.executeAlg( algRequest, ctx.session );
                    } catch ( Throwable t ) {
                        ctx.send( RelationalResult.builder().error( t.getMessage() ).build() );
                        return;
                    }
                } else {//TableRequest, is equal to UIRequest
                    UIRequest uiRequest = ctx.messageAsClass( UIRequest.class );
                    try {
                        LogicalNamespace namespace = Catalog.getInstance().getSnapshot().getNamespace( uiRequest.namespace ).orElse( null );
                        result = switch ( namespace == null ? DataModel.RELATIONAL : namespace.dataModel ) {
                            case RELATIONAL -> crud.getTable( uiRequest );
                            case DOCUMENT -> {
                                String entity = Catalog.snapshot().doc().getCollection( uiRequest.entityId ).map( c -> c.name ).orElse( "" );
                                yield LanguageCrud.anyQueryResult(
                                        QueryContext.builder()
                                                .query( String.format( "db.%s.find({})", entity ) )
                                                .language( QueryLanguage.from( "mongo" ) )
                                                .origin( POLYPHENY_UI )
                                                .batch( uiRequest.noLimit ? -1 : crud.getPageSize() )
                                                .transactionManager( crud.getTransactionManager() )
                                                .informationTarget( i -> i.setSession( ctx.session ) )
                                                .namespaceId( namespace == null ? Catalog.defaultNamespaceId : namespace.id )
                                                .build(), uiRequest ).get( 0 );
                            }
                            case GRAPH -> LanguageCrud.anyQueryResult(
                                    QueryContext.builder()
                                            .query( "MATCH (n) RETURN n" )
                                            .language( QueryLanguage.from( "cypher" ) )
                                            .origin( POLYPHENY_UI )
                                            .batch( uiRequest.noLimit ? -1 : crud.getPageSize() )
                                            .namespaceId( namespace == null ? Catalog.defaultNamespaceId : namespace.id )
                                            .transactionManager( crud.getTransactionManager() )
                                            .informationTarget( i -> i.setSession( ctx.session ) )
                                            .build(), uiRequest ).get( 0 );
                        };
                        if ( result == null ) {
                            throw new GenericRuntimeException( "Could not load data." );
                        }

                    } catch ( Throwable t ) {
                        ctx.send( RelationalResult.builder().error( t.getMessage() ).build() );
                        return;
                    }
                }
                if ( result.xid != null ) {
                    xIds.add( result.xid );
                }
                ctx.send( result );
                break;
            default:
                throw new GenericRuntimeException( "Unexpected WebSocket request: " + request.type );
        }
        queryAnalyzers.put( ctx.getSessionId(), xIds );
    }


    @Override
    public void accept( WsConfig wsConfig ) {
        wsConfig.onConnect( this::connected );
        wsConfig.onMessage( this::onMessage );

        wsConfig.onClose( this::closed );
    }

}
