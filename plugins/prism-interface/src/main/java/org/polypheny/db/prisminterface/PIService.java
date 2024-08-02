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

package org.polypheny.db.prisminterface;

import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.prisminterface.metaRetrieval.DbMetaRetriever;
import org.polypheny.db.prisminterface.metaRetrieval.PIClientInfoProperties;
import org.polypheny.db.prisminterface.statementProcessing.StatementProcessor;
import org.polypheny.db.prisminterface.statements.PIPreparedIndexedStatement;
import org.polypheny.db.prisminterface.statements.PIPreparedNamedStatement;
import org.polypheny.db.prisminterface.statements.PIStatement;
import org.polypheny.db.prisminterface.statements.PIUnparameterizedStatement;
import org.polypheny.db.prisminterface.statements.PIUnparameterizedStatementBatch;
import org.polypheny.db.prisminterface.transport.Transport;
import org.polypheny.db.prisminterface.utils.PrismUtils;
import org.polypheny.db.prisminterface.utils.PrismValueDeserializer;
import org.polypheny.db.prisminterface.utils.PropertyUtils;
import org.polypheny.db.prisminterface.utils.VersionUtils;
import org.polypheny.db.sql.language.SqlJdbcFunctionCall;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Util;
import org.polypheny.prism.ClientInfoProperties;
import org.polypheny.prism.ClientInfoPropertiesRequest;
import org.polypheny.prism.ClientInfoPropertiesResponse;
import org.polypheny.prism.CloseResultRequest;
import org.polypheny.prism.CloseResultResponse;
import org.polypheny.prism.CloseStatementRequest;
import org.polypheny.prism.CloseStatementResponse;
import org.polypheny.prism.CommitRequest;
import org.polypheny.prism.CommitResponse;
import org.polypheny.prism.ConnectionCheckRequest;
import org.polypheny.prism.ConnectionCheckResponse;
import org.polypheny.prism.ConnectionProperties;
import org.polypheny.prism.ConnectionPropertiesUpdateRequest;
import org.polypheny.prism.ConnectionPropertiesUpdateResponse;
import org.polypheny.prism.ConnectionRequest;
import org.polypheny.prism.ConnectionResponse;
import org.polypheny.prism.ConnectionResponse.Builder;
import org.polypheny.prism.DbmsVersionRequest;
import org.polypheny.prism.DbmsVersionResponse;
import org.polypheny.prism.DefaultNamespaceRequest;
import org.polypheny.prism.DefaultNamespaceResponse;
import org.polypheny.prism.DisconnectRequest;
import org.polypheny.prism.DisconnectResponse;
import org.polypheny.prism.EntitiesRequest;
import org.polypheny.prism.EntitiesResponse;
import org.polypheny.prism.ErrorResponse;
import org.polypheny.prism.ExecuteIndexedStatementBatchRequest;
import org.polypheny.prism.ExecuteIndexedStatementRequest;
import org.polypheny.prism.ExecuteNamedStatementRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.FetchRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.FunctionsRequest;
import org.polypheny.prism.FunctionsResponse;
import org.polypheny.prism.MetaStringResponse;
import org.polypheny.prism.NamespacesRequest;
import org.polypheny.prism.NamespacesResponse;
import org.polypheny.prism.PrepareStatementRequest;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.ProceduresRequest;
import org.polypheny.prism.ProceduresResponse;
import org.polypheny.prism.Request;
import org.polypheny.prism.Request.TypeCase;
import org.polypheny.prism.Response;
import org.polypheny.prism.RollbackRequest;
import org.polypheny.prism.RollbackResponse;
import org.polypheny.prism.SqlKeywordsRequest;
import org.polypheny.prism.SqlNumericFunctionsRequest;
import org.polypheny.prism.SqlStringFunctionsRequest;
import org.polypheny.prism.SqlSystemFunctionsRequest;
import org.polypheny.prism.SqlTimeDateFunctionsRequest;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;
import org.polypheny.prism.StreamAcknowledgement;
import org.polypheny.prism.StreamFetchRequest;
import org.polypheny.prism.StreamFrame;
import org.polypheny.prism.StreamSendRequest;
import org.polypheny.prism.TableTypesRequest;
import org.polypheny.prism.TableTypesResponse;
import org.polypheny.prism.TypesRequest;
import org.polypheny.prism.TypesResponse;

@Slf4j
class PIService {

    private final long connectionId;
    private final ClientManager clientManager;
    private final Transport con;
    private String uuid = null;


    private PIService( Transport con, long connectionId, ClientManager clientManager ) {
        this.con = con;
        this.connectionId = connectionId;
        this.clientManager = clientManager;
    }


    public static void acceptConnection( Transport con, long connectionId, ClientManager clientManager ) {
        PIService service = new PIService( con, connectionId, clientManager );
        service.acceptLoop();
    }


    private Response createErrorResponse( long id, String message ) {
        return Response.newBuilder()
                .setId( id )
                .setLast( true )
                .setErrorResponse( ErrorResponse.newBuilder().setMessage( message ) )
                .build();
    }


    private boolean handleFirstMessage() throws IOException {
        boolean success = false;
        Request firstReq = readOneMessage();

        if ( firstReq.getTypeCase() != TypeCase.CONNECTION_REQUEST ) {
            sendOneMessage( createErrorResponse( firstReq.getId(), "First message must be a connection request" ) );
            return false;
        }

        Response r;
        try {
            r = connect( firstReq.getConnectionRequest(), new ResponseMaker<>( firstReq, "connection_response" ) );
            success = true;
        } catch ( TransactionException | AuthenticationException e ) {
            r = createErrorResponse( firstReq.getId(), e.getMessage() );
        }
        sendOneMessage( r );
        return success;
    }


    private CompletableFuture<Request> waitForRequest() {
        return CompletableFuture.supplyAsync( () -> {
            try {
                return readOneMessage();
            } catch ( IOException e ) {
                throw new PIServiceException( e );
            }
        } );
    }


    private void handleRequest( Request req, CompletableFuture<Response> f ) {
        Response r;
        try {
            r = handleMessage( req );
        } catch ( Throwable t ) {
            r = createErrorResponse( req.getId(), t.getMessage() );
        }
        try {
            sendOneMessage( r );
            f.complete( r );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    private void handleMessages() throws IOException, ExecutionException, InterruptedException {
        if ( !handleFirstMessage() ) {
            return;
        }

        Queue<Request> waiting = new LinkedList<>();
        CompletableFuture<Request> request = waitForRequest();
        CompletableFuture<Response> response = null;
        Thread handle = null;
        try {
            while ( true ) {
                if ( response == null ) {
                    Request req = waiting.poll();
                    if ( req != null ) {
                        response = new CompletableFuture<>();
                        CompletableFuture<Response> finalResponse = response;
                        handle = new Thread( () -> handleRequest( req, finalResponse ), String.format( "PrismConnection%dRequest%dHandler", connectionId, req.getId() ) );
                        handle.setUncaughtExceptionHandler( ( t, e ) -> finalResponse.completeExceptionally( e ) );
                        handle.start();
                    }
                }

                // Wait for next event
                if ( response == null ) {
                    request.get();
                } else {
                    CompletableFuture.anyOf( request, response ).get();
                }

                if ( request.isDone() ) {
                    waiting.add( request.get() );
                    request = waitForRequest();
                } else if ( response != null && response.isDone() ) {
                    handle.join();
                    handle = null;
                    Response r = response.get();
                    if ( r.getTypeCase() == Response.TypeCase.DISCONNECT_RESPONSE ) {
                        break;
                    }
                    response = null;
                }
            }
        } catch ( ExecutionException e ) {
            if ( e.getCause() instanceof PIServiceException p && p.getCause() instanceof EOFException eof ) {
                throw eof;
            }
            throw e;
        } finally {
            if ( handle != null ) {
                handle.interrupt();
            }
        }
    }


    private void acceptLoop() {
        try {
            handleMessages();
        } catch ( Throwable e ) {
            if ( !(e instanceof EOFException) ) {
                throw new GenericRuntimeException( e );
            }
        } finally {
            if ( uuid != null ) {
                clientManager.unregisterConnection( clientManager.getClient( uuid ) );
            }
            Util.closeNoThrow( con );
        }
    }


    private void sendOneMessage( Response r ) throws IOException {
        con.sendMessage( r.toByteArray() );
    }


    private Request readOneMessage() throws IOException {
        return Request.parseFrom( con.receiveMessage() );
    }


    private Response handleMessage( Request req ) throws IOException {
        if ( uuid == null || clientManager.getClient( uuid ) == null ) {
            throw new IllegalStateException( "Clients must be authenticated before sending any messages" );
        }
        return switch ( req.getTypeCase() ) {
            case DBMS_VERSION_REQUEST -> getDbmsVersion( req.getDbmsVersionRequest(), new ResponseMaker<>( req, "dbms_version_response" ) );
            case DEFAULT_NAMESPACE_REQUEST -> getDefaultNamespace( req.getDefaultNamespaceRequest(), new ResponseMaker<>( req, "default_namespace_response" ) );
            case TABLE_TYPES_REQUEST -> getTableTypes( req.getTableTypesRequest(), new ResponseMaker<>( req, "table_types_response" ) );
            case TYPES_REQUEST -> getTypes( req.getTypesRequest(), new ResponseMaker<>( req, "types_response" ) );
            case PROCEDURES_REQUEST -> searchProcedures( req.getProceduresRequest(), new ResponseMaker<>( req, "procedures_response" ) );
            case FUNCTIONS_REQUEST -> searchFunctions( req.getFunctionsRequest(), new ResponseMaker<>( req, "functions_response" ) );
            case NAMESPACES_REQUEST -> searchNamespaces( req.getNamespacesRequest(), new ResponseMaker<>( req, "namespaces_response" ) );
            case ENTITIES_REQUEST -> searchEntities( req.getEntitiesRequest(), new ResponseMaker<>( req, "entities_response" ) );
            case SQL_STRING_FUNCTIONS_REQUEST -> getSqlStringFunctions( req.getSqlStringFunctionsRequest(), new ResponseMaker<>( req, "sql_string_functions_response" ) );
            case SQL_SYSTEM_FUNCTIONS_REQUEST -> getSqlSystemFunctions( req.getSqlSystemFunctionsRequest(), new ResponseMaker<>( req, "sql_system_functions_response" ) );
            case SQL_TIME_DATE_FUNCTIONS_REQUEST -> getSqlTimeDateFunctions( req.getSqlTimeDateFunctionsRequest(), new ResponseMaker<>( req, "sql_time_date_functions_response" ) );
            case SQL_NUMERIC_FUNCTIONS_REQUEST -> getSqlNumericFunctions( req.getSqlNumericFunctionsRequest(), new ResponseMaker<>( req, "sql_numeric_functions_response" ) );
            case SQL_KEYWORDS_REQUEST -> getSqlKeywords( req.getSqlKeywordsRequest(), new ResponseMaker<>( req, "sql_keywords_response" ) );
            case CONNECTION_REQUEST -> throw new GenericRuntimeException( "ConnectionRequest only allowed as first message" );
            case CONNECTION_CHECK_REQUEST -> checkConnection( req.getConnectionCheckRequest(), new ResponseMaker<>( req, "connection_check_response" ) );
            case DISCONNECT_REQUEST -> disconnect( req.getDisconnectRequest(), new ResponseMaker<>( req, "disconnect_response" ) );
            case CLIENT_INFO_PROPERTIES_REQUEST -> getClientInfoProperties( req.getClientInfoPropertiesRequest(), new ResponseMaker<>( req, "client_info_properties_response" ) );
            case SET_CLIENT_INFO_PROPERTIES_REQUEST -> setClientInfoProperties( req.getSetClientInfoPropertiesRequest(), new ResponseMaker<>( req, "set_client_info_properties_response" ) );
            case EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST -> executeUnparameterizedStatement( req.getExecuteUnparameterizedStatementRequest(), new ResponseMaker<>( req, "statement_response" ) );
            case EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST -> executeUnparameterizedStatementBatch( req.getExecuteUnparameterizedStatementBatchRequest(), new ResponseMaker<>( req, "statement_batch_response" ) );
            case PREPARE_INDEXED_STATEMENT_REQUEST -> prepareIndexedStatement( req.getPrepareIndexedStatementRequest(), new ResponseMaker<>( req, "prepared_statement_signature" ) );
            case EXECUTE_INDEXED_STATEMENT_REQUEST -> executeIndexedStatement( req.getExecuteIndexedStatementRequest(), new ResponseMaker<>( req, "statement_result" ) );
            case EXECUTE_INDEXED_STATEMENT_BATCH_REQUEST -> executeIndexedStatementBatch( req.getExecuteIndexedStatementBatchRequest(), new ResponseMaker<>( req, "statement_batch_response" ) );
            case PREPARE_NAMED_STATEMENT_REQUEST -> prepareNamedStatement( req.getPrepareNamedStatementRequest(), new ResponseMaker<>( req, "prepared_statement_signature" ) );
            case EXECUTE_NAMED_STATEMENT_REQUEST -> executeNamedStatement( req.getExecuteNamedStatementRequest(), new ResponseMaker<>( req, "statement_result" ) );
            case STREAM_FETCH_REQUEST -> fetchStream( req.getStreamFetchRequest(), new ResponseMaker<>( req, "stream_frame" ) );
            case STREAM_SEND_REQUEST -> receiveStream( req.getStreamSendRequest(), new ResponseMaker<>( req, "stream_acknowledgement" ) );
            case FETCH_REQUEST -> fetchResult( req.getFetchRequest(), new ResponseMaker<>( req, "frame" ) );
            case CLOSE_STATEMENT_REQUEST -> closeStatement( req.getCloseStatementRequest(), new ResponseMaker<>( req, "close_statement_response" ) );
            case COMMIT_REQUEST -> commitTransaction( req.getCommitRequest(), new ResponseMaker<>( req, "commit_response" ) );
            case ROLLBACK_REQUEST -> rollbackTransaction( req.getRollbackRequest(), new ResponseMaker<>( req, "rollback_response" ) );
            case CONNECTION_PROPERTIES_UPDATE_REQUEST -> updateConnectionProperties( req.getConnectionPropertiesUpdateRequest(), new ResponseMaker<>( req, "connection_properties_update_response" ) );
            case CLOSE_RESULT_REQUEST -> closeResult( req.getCloseResultRequest(), new ResponseMaker<>( req, "close_result_response" ) );
            case TYPE_NOT_SET -> throw new NotImplementedException( "Unsupported call " + req.getTypeCase() );
        };
    }


    private Response connect( ConnectionRequest request, ResponseMaker<ConnectionResponse> responseObserver ) throws TransactionException, AuthenticationException {

        if ( uuid != null ) {
            throw new PIServiceException( "Can only connect once per session" );
        }
        Builder responseBuilder = ConnectionResponse.newBuilder()
                .setMajorApiVersion( VersionUtils.getMAJOR_API_VERSION() )
                .setMinorApiVersion( VersionUtils.getMINOR_API_VERSION() );
        boolean isCompatible = checkApiVersion( request );
        responseBuilder.setIsCompatible( isCompatible );
        ConnectionResponse ConnectionResponse = responseBuilder.build();
        // reject incompatible client
        if ( !isCompatible ) {
            log.info( "Incompatible client and server version" );
            return responseObserver.makeResponse( ConnectionResponse );
        }

        uuid = clientManager.registerConnection( request, con );
        return responseObserver.makeResponse( ConnectionResponse );
    }


    private Response disconnect( DisconnectRequest request, ResponseMaker<DisconnectResponse> responseObserver ) {
        PIClient client = getClient();
        clientManager.unregisterConnection( client );
        uuid = null;
        return responseObserver.makeResponse( DisconnectResponse.newBuilder().build() );
    }


    private Response checkConnection( ConnectionCheckRequest request, ResponseMaker<ConnectionCheckResponse> responseObserver ) {
        return responseObserver.makeResponse( ConnectionCheckResponse.newBuilder().build() );
    }


    private Response getDbmsVersion( DbmsVersionRequest request, ResponseMaker<DbmsVersionResponse> responseObserver ) {
        return responseObserver.makeResponse( DbMetaRetriever.getDbmsVersion() );
    }


    private MetaStringResponse buildMetaStringResponse( String string ) {
        return MetaStringResponse.newBuilder()
                .setString( string )
                .build();
    }


    private Response getDefaultNamespace( DefaultNamespaceRequest request, ResponseMaker<DefaultNamespaceResponse> responseObserver ) {
        return responseObserver.makeResponse( DbMetaRetriever.getDefaultNamespace() );
    }


    private Response getTableTypes( TableTypesRequest request, ResponseMaker<TableTypesResponse> responseObserver ) {
        return responseObserver.makeResponse( DbMetaRetriever.getTableTypes() );
    }


    private Response getTypes( TypesRequest request, ResponseMaker<TypesResponse> responseStreamObserver ) {
        return responseStreamObserver.makeResponse( DbMetaRetriever.getTypes() );
    }


    private Response searchNamespaces( NamespacesRequest request, ResponseMaker<NamespacesResponse> responseObserver ) {
        String namespacePattern = request.hasNamespacePattern() ? request.getNamespacePattern() : null;
        String namespaceType = request.hasNamespaceType() ? request.getNamespaceType() : null;
        return responseObserver.makeResponse( DbMetaRetriever.searchNamespaces( namespacePattern, namespaceType ) );
    }


    private Response searchEntities( EntitiesRequest request, ResponseMaker<EntitiesResponse> responseObserver ) {
        String entityPattern = request.hasEntityPattern() ? request.getEntityPattern() : null;
        return responseObserver.makeResponse( DbMetaRetriever.searchEntities( request.getNamespaceName(), entityPattern ) );
    }


    private Response getSqlStringFunctions( SqlStringFunctionsRequest request, ResponseMaker<MetaStringResponse> responseObserver ) {
        return responseObserver.makeResponse( buildMetaStringResponse( SqlJdbcFunctionCall.getStringFunctions() ) );
    }


    private Response getSqlSystemFunctions( SqlSystemFunctionsRequest request, ResponseMaker<MetaStringResponse> responseObserver ) {
        return responseObserver.makeResponse( buildMetaStringResponse( SqlJdbcFunctionCall.getSystemFunctions() ) );
    }


    private Response getSqlTimeDateFunctions( SqlTimeDateFunctionsRequest request, ResponseMaker<MetaStringResponse> responseObserver ) {
        return responseObserver.makeResponse( buildMetaStringResponse( SqlJdbcFunctionCall.getTimeDateFunctions() ) );
    }


    private Response getSqlNumericFunctions( SqlNumericFunctionsRequest request, ResponseMaker<MetaStringResponse> responseObserver ) {
        return responseObserver.makeResponse( buildMetaStringResponse( SqlJdbcFunctionCall.getNumericFunctions() ) );
    }


    private Response getSqlKeywords( SqlKeywordsRequest request, ResponseMaker<MetaStringResponse> responseObserver ) {
        // TODO actually return keywords
        return responseObserver.makeResponse( buildMetaStringResponse( "" ) );
    }


    private Response searchProcedures( ProceduresRequest request, ResponseMaker<ProceduresResponse> responseObserver ) {
        String procedurePattern = request.hasProcedureNamePattern() ? request.getProcedureNamePattern() : null;
        return responseObserver.makeResponse( DbMetaRetriever.getProcedures( request.getLanguage(), procedurePattern ) );
    }


    private Response searchFunctions( FunctionsRequest request, ResponseMaker<FunctionsResponse> responseObserver ) {
        QueryLanguage queryLanguage = QueryLanguage.from( request.getQueryLanguage() );
        FunctionCategory functionCategory = FunctionCategory.valueOf( request.getFunctionCategory() );
        return responseObserver.makeResponse( DbMetaRetriever.getFunctions( queryLanguage, functionCategory ) );
    }


    private Response executeUnparameterizedStatement( ExecuteUnparameterizedStatementRequest request, ResponseMaker<StatementResponse> responseObserver ) throws IOException {
        PIClient client = getClient();
        PIUnparameterizedStatement statement = client.getStatementManager().createUnparameterizedStatement( request );
        Response mid = responseObserver.makeResponse( PrismUtils.createResult( statement ), false );
        sendOneMessage( mid );
        StatementResult result = statement.execute(
                request.hasFetchSize()
                        ? request.getFetchSize()
                        : PropertyUtils.DEFAULT_FETCH_SIZE
        );
        return responseObserver.makeResponse( PrismUtils.createResult( statement, result ) );
    }


    private Response executeUnparameterizedStatementBatch( ExecuteUnparameterizedStatementBatchRequest request, ResponseMaker<StatementBatchResponse> responseObserver ) throws IOException {
        PIClient client = getClient();
        PIUnparameterizedStatementBatch batch = client.getStatementManager().createUnparameterizedStatementBatch( request.getStatementsList() );
        Response mid = responseObserver.makeResponse( PrismUtils.createStatementBatchStatus( batch.getBatchId() ), false );
        sendOneMessage( mid );
        List<Long> updateCounts = batch.executeBatch();
        return responseObserver.makeResponse( PrismUtils.createStatementBatchStatus( batch.getBatchId(), updateCounts ) );
    }


    private Response prepareIndexedStatement( PrepareStatementRequest request, ResponseMaker<PreparedStatementSignature> responseObserver ) {
        PIClient client = getClient();
        PIPreparedIndexedStatement statement = client.getStatementManager().createIndexedPreparedInterfaceStatement( request );
        return responseObserver.makeResponse( PrismUtils.createPreparedStatementSignature( statement ) );
    }


    private Response executeIndexedStatement( ExecuteIndexedStatementRequest request, ResponseMaker<StatementResult> responseObserver ) {
        PIClient client = getClient();
        PIPreparedIndexedStatement statement = client.getStatementManager().getIndexedPreparedStatement( request.getStatementId() );
        int fetchSize = request.hasFetchSize()
                ? request.getFetchSize()
                : PropertyUtils.DEFAULT_FETCH_SIZE;
        return responseObserver.makeResponse( statement.execute( PrismValueDeserializer.deserializeParameterList( request.getParameters().getParametersList() ), statement.getParameterMetas(), fetchSize ) );
    }


    private Response executeIndexedStatementBatch( ExecuteIndexedStatementBatchRequest request, ResponseMaker<StatementBatchResponse> resultObserver ) {
        PIClient client = getClient();
        PIPreparedIndexedStatement statement = client.getStatementManager().getIndexedPreparedStatement( request.getStatementId() );
        List<List<PolyValue>> valuesList = PrismValueDeserializer.deserializeParameterLists( request.getParametersList() );
        List<Long> updateCounts = statement.executeBatch( valuesList );
        return resultObserver.makeResponse( PrismUtils.createStatementBatchStatus( statement.getId(), updateCounts ) );
    }


    private Response prepareNamedStatement( PrepareStatementRequest request, ResponseMaker<PreparedStatementSignature> responseObserver ) {
        PIClient client = getClient();
        PIPreparedNamedStatement statement = client.getStatementManager().createNamedPreparedInterfaceStatement( request );
        return responseObserver.makeResponse( PrismUtils.createPreparedStatementSignature( statement ) );
    }


    private Response executeNamedStatement( ExecuteNamedStatementRequest request, ResponseMaker<StatementResult> responseObserver ) {
        PIClient client = getClient();
        PIPreparedNamedStatement statement = client.getStatementManager().getNamedPreparedStatement( request.getStatementId() );
        int fetchSize = request.hasFetchSize()
                ? request.getFetchSize()
                : PropertyUtils.DEFAULT_FETCH_SIZE;
        try {
            return responseObserver.makeResponse( statement.execute( PrismValueDeserializer.deserilaizeParameterMap( request.getParameters().getParametersMap() ), fetchSize ) );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
    }


    private Response fetchStream( StreamFetchRequest request, ResponseMaker<StreamFrame> responseObserver ) {
        PIClient client = getClient();
        PIStatement statement = client.getStatementManager().getStatement( request.getStatementId() );
        try {
            return responseObserver.makeResponse( statement.getStreamingFramework().getIndex().get( request.getStreamId() ).get( request.getPosition(), request.getLength() ) );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( e );
        }
    }

    private Response receiveStream( StreamSendRequest request, ResponseMaker<StreamAcknowledgement> responseObserver ) {
        PIClient client = getClient();
        return responseObserver.makeResponse(client.getStreamReceiver().appendOrCreateNew(request));
    }


    private Response fetchResult( FetchRequest request, ResponseMaker<Frame> responseObserver ) {
        PIClient client = getClient();
        PIStatement statement = client.getStatementManager().getStatement( request.getStatementId() );
        int fetchSize = request.hasFetchSize()
                ? request.getFetchSize()
                : PropertyUtils.DEFAULT_FETCH_SIZE;
        Frame frame = StatementProcessor.fetch( statement, fetchSize );
        return responseObserver.makeResponse( frame );
    }


    private Response commitTransaction( CommitRequest request, ResponseMaker<CommitResponse> responseStreamObserver ) {
        PIClient client = getClient();
        client.commitCurrentTransaction();
        return responseStreamObserver.makeResponse( CommitResponse.newBuilder().build() );
    }


    private Response rollbackTransaction( RollbackRequest request, ResponseMaker<RollbackResponse> responseStreamObserver ) {
        PIClient client = getClient();
        client.rollbackCurrentTransaction();
        return responseStreamObserver.makeResponse( RollbackResponse.newBuilder().build() );
    }


    private Response closeStatement( CloseStatementRequest request, ResponseMaker<CloseStatementResponse> responseObserver ) {
        PIClient client = getClient();
        client.getStatementManager().closeStatementOrBatch( request.getStatementId() );
        return responseObserver.makeResponse( CloseStatementResponse.newBuilder().build() );
    }


    private Response closeResult( CloseResultRequest request, ResponseMaker<CloseResultResponse> responseObserver ) {
        PIClient client = getClient();
        PIStatement statement = client.getStatementManager().getStatement( request.getStatementId() );
        statement.closeResults();
        return responseObserver.makeResponse( CloseResultResponse.newBuilder().build() );
    }


    private Response updateConnectionProperties( ConnectionPropertiesUpdateRequest request, ResponseMaker<ConnectionPropertiesUpdateResponse> responseObserver ) {
        PIClient client = getClient();
        ConnectionProperties properties = request.getConnectionProperties();
        if ( properties.hasIsAutoCommit() ) {
            client.setAutoCommit( properties.getIsAutoCommit() );
        }
        if ( properties.hasNamespaceName() ) {
            String namespaceName = properties.getNamespaceName();
            Optional<LogicalNamespace> optionalNamespace = Catalog.getInstance().getSnapshot().getNamespace( namespaceName );
            if ( optionalNamespace.isEmpty() ) {
                throw new PIServiceException( "Getting namespace " + namespaceName + " failed." );
            }
            client.setNamespace( optionalNamespace.get() );
        }
        return responseObserver.makeResponse( ConnectionPropertiesUpdateResponse.newBuilder().build() );
    }


    private Response getClientInfoProperties( ClientInfoPropertiesRequest request, ResponseMaker<ClientInfoProperties> responseObserver ) {
        PIClient client = getClient();
        ClientInfoProperties.Builder responseBuilder = ClientInfoProperties.newBuilder();
        PIClientInfoProperties PIClientInfoProperties = client.getPIClientInfoProperties();
        PIClientInfoProperties.stringPropertyNames().forEach( s -> responseBuilder.putProperties( s, PIClientInfoProperties.getProperty( s ) ) );
        return responseObserver.makeResponse( responseBuilder.build() );
    }


    private Response setClientInfoProperties( ClientInfoProperties properties, ResponseMaker<ClientInfoPropertiesResponse> reponseObserver ) {
        PIClient client = getClient();
        client.getPIClientInfoProperties().putAll( properties.getPropertiesMap() );
        return reponseObserver.makeResponse( ClientInfoPropertiesResponse.newBuilder().build() );
    }


    private PIClient getClient() throws PIServiceException {
        if ( uuid == null ) {
            throw new PIServiceException( "Must authenticate first" );
        }
        return clientManager.getClient( uuid );
    }


    private static boolean checkApiVersion( ConnectionRequest connectionRequest ) {
        return connectionRequest.getMajorApiVersion() == VersionUtils.getMAJOR_API_VERSION();
    }

}
