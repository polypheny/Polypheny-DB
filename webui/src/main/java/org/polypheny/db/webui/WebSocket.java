/*
 * Copyright 2019-2025 The Polypheny Project
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
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.information.InformationPolyAlg.PlanType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.PhysicalQueryContext;
import org.polypheny.db.processing.QueryContext.TranslatedQueryContext;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.requests.GraphRequest;
import org.polypheny.db.webui.models.requests.PolyAlgRequest;
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
        log.error( "UI message received: " + ctx.message() );
        //close analyzers of a previous query that was sent over the same socket.
        Crud.cleanupOldSession( queryAnalyzers, ctx.getSessionId() );

        RequestModel request = ctx.messageAsClass( RequestModel.class );
        Set<String> xIds = new HashSet<>();
        switch ( request.type ) {
            case "GraphRequest":
                GraphRequest graphRequest = ctx.messageAsClass( GraphRequest.class );
                Pair<PolyXid, PolyGraph> xidGraph = LanguageCrud.getGraph( Catalog.snapshot().getNamespace( graphRequest.namespace ).orElseThrow().name, crud.getTransactionManager(), ctx.session );

                xIds.add( xidGraph.left.toString() );

                ctx.send( xidGraph.right.toJson() );

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
                    if ( result.xid != null ) {
                        xIds.add( result.xid );
                    }
                }

                ctx.send( results );
                break;
            case "PolyAlgRequest":
                PolyAlgRequest polyAlgRequest = ctx.messageAsClass( PolyAlgRequest.class );
                Transaction transaction = crud.getTransactionManager().startTransaction( Catalog.defaultUserId, Catalog.defaultNamespaceId, true, POLYPHENY_UI );
                AlgRoot root;
                Statement statement;
                try {
                    statement = transaction.createStatement();
                    root = PolyPlanBuilder.buildFromPolyAlg( polyAlgRequest.polyAlg, polyAlgRequest.planType, statement );
                } catch ( Exception e ) {
                    log.error( "Caught exception while building the plan builder tree", e );
                    ctx.send( RelationalResult.builder().error( e.getMessage() ).build() );
                    break;
                }

                QueryLanguage ql = switch ( polyAlgRequest.model ) {
                    case RELATIONAL -> QueryLanguage.from( "sql" );
                    case DOCUMENT -> QueryLanguage.from( "mongo" );
                    case GRAPH -> QueryLanguage.from( "cypher" );
                };
                QueryContext qc = QueryContext.builder()
                        .query( polyAlgRequest.polyAlg )
                        .language( ql )
                        .isAnalysed( true )
                        .usesCache( true )
                        .origin( POLYPHENY_UI )
                        .batch( polyAlgRequest.noLimit ? -1 : crud.getPageSize() )
                        .transactionManager( crud.getTransactionManager() )
                        .transactions( List.of( transaction ) )
                        .statement( statement )
                        .informationTarget( i -> i.setSession( ctx.session ) ).build();

                TranslatedQueryContext translated;
                if ( polyAlgRequest.planType == PlanType.PHYSICAL ) {
                    Pair<List<PolyValue>, List<AlgDataType>> dynamicParams;
                    try {
                        dynamicParams = PolyPlanBuilder.translateDynamicParams( polyAlgRequest.dynamicValues, polyAlgRequest.dynamicTypes );
                    } catch ( Exception e ) {
                        log.error( "Caught exception while translating dynamic parameters:", e );
                        ctx.send( RelationalResult.builder().error( e.getMessage() ).build() );
                        break;
                    }
                    translated = PhysicalQueryContext.fromQuery( polyAlgRequest.polyAlg, root, dynamicParams.left, dynamicParams.right, qc );
                } else {
                    translated = TranslatedQueryContext.fromQuery( polyAlgRequest.polyAlg, root, polyAlgRequest.planType == PlanType.ALLOCATION, qc );
                }
                List<? extends Result<?, ?>> polyAlgResults = LanguageCrud.anyQueryResult( translated, polyAlgRequest );
                ctx.send( polyAlgResults.get( 0 ) );
                break;

            case "RegisterRequest":
                RegisterRequest registerRequest = ctx.messageAsClass( RegisterRequest.class );
                crud.authCrud.register( registerRequest, ctx );
                break;
            case "EntityRequest":
                Result<?, ?> result;
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
                                            .namespaceId( namespace.id )
                                            .build(), uiRequest ).get( 0 );
                        }
                        case GRAPH -> LanguageCrud.anyQueryResult(
                                QueryContext.builder()
                                        .query( "MATCH (n) RETURN n" )
                                        .language( QueryLanguage.from( "cypher" ) )
                                        .origin( POLYPHENY_UI )
                                        .batch( uiRequest.noLimit ? -1 : crud.getPageSize() )
                                        .namespaceId( namespace.id )
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
                if ( result.xid != null ) {
                    xIds.add( result.xid );
                }
                System.out.println( result );
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
