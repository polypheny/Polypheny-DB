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

package org.polypheny.db.protointerface;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.proto.ClientInfoProperties;
import org.polypheny.db.protointerface.proto.ClientInfoPropertiesRequest;
import org.polypheny.db.protointerface.proto.ClientInfoPropertiesResponse;
import org.polypheny.db.protointerface.proto.CloseStatementRequest;
import org.polypheny.db.protointerface.proto.CloseStatementResponse;
import org.polypheny.db.protointerface.proto.CommitRequest;
import org.polypheny.db.protointerface.proto.CommitResponse;
import org.polypheny.db.protointerface.proto.ConnectionCheckRequest;
import org.polypheny.db.protointerface.proto.ConnectionCheckResponse;
import org.polypheny.db.protointerface.proto.ConnectionProperties;
import org.polypheny.db.protointerface.proto.ConnectionPropertiesUpdateRequest;
import org.polypheny.db.protointerface.proto.ConnectionPropertiesUpdateResponse;
import org.polypheny.db.protointerface.proto.ConnectionRequest;
import org.polypheny.db.protointerface.proto.ConnectionResponse;
import org.polypheny.db.protointerface.proto.ConnectionResponse.Builder;
import org.polypheny.db.protointerface.proto.DatabasesRequest;
import org.polypheny.db.protointerface.proto.DatabasesResponse;
import org.polypheny.db.protointerface.proto.DbmsVersionRequest;
import org.polypheny.db.protointerface.proto.DbmsVersionResponse;
import org.polypheny.db.protointerface.proto.DisconnectRequest;
import org.polypheny.db.protointerface.proto.DisconnectResponse;
import org.polypheny.db.protointerface.proto.EntitiesRequest;
import org.polypheny.db.protointerface.proto.EntitiesResponse;
import org.polypheny.db.protointerface.proto.ErrorResponse;
import org.polypheny.db.protointerface.proto.ExecuteIndexedStatementBatchRequest;
import org.polypheny.db.protointerface.proto.ExecuteIndexedStatementRequest;
import org.polypheny.db.protointerface.proto.ExecuteNamedStatementRequest;
import org.polypheny.db.protointerface.proto.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.db.protointerface.proto.ExecuteUnparameterizedStatementRequest;
import org.polypheny.db.protointerface.proto.FetchRequest;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.FunctionsRequest;
import org.polypheny.db.protointerface.proto.FunctionsResponse;
import org.polypheny.db.protointerface.proto.LanguageRequest;
import org.polypheny.db.protointerface.proto.LanguageResponse;
import org.polypheny.db.protointerface.proto.MetaStringResponse;
import org.polypheny.db.protointerface.proto.Namespace;
import org.polypheny.db.protointerface.proto.NamespaceRequest;
import org.polypheny.db.protointerface.proto.NamespacesRequest;
import org.polypheny.db.protointerface.proto.NamespacesResponse;
import org.polypheny.db.protointerface.proto.PrepareStatementRequest;
import org.polypheny.db.protointerface.proto.PreparedStatementSignature;
import org.polypheny.db.protointerface.proto.ProceduresRequest;
import org.polypheny.db.protointerface.proto.ProceduresResponse;
import org.polypheny.db.protointerface.proto.Request;
import org.polypheny.db.protointerface.proto.Request.TypeCase;
import org.polypheny.db.protointerface.proto.Response;
import org.polypheny.db.protointerface.proto.RollbackRequest;
import org.polypheny.db.protointerface.proto.RollbackResponse;
import org.polypheny.db.protointerface.proto.SqlKeywordsRequest;
import org.polypheny.db.protointerface.proto.SqlNumericFunctionsRequest;
import org.polypheny.db.protointerface.proto.SqlStringFunctionsRequest;
import org.polypheny.db.protointerface.proto.SqlSystemFunctionsRequest;
import org.polypheny.db.protointerface.proto.SqlTimeDateFunctionsRequest;
import org.polypheny.db.protointerface.proto.StatementBatchResponse;
import org.polypheny.db.protointerface.proto.StatementResponse;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.proto.TableTypesRequest;
import org.polypheny.db.protointerface.proto.TableTypesResponse;
import org.polypheny.db.protointerface.proto.TypesRequest;
import org.polypheny.db.protointerface.proto.TypesResponse;
import org.polypheny.db.protointerface.statementProcessing.StatementProcessor;
import org.polypheny.db.protointerface.statements.PIPreparedIndexedStatement;
import org.polypheny.db.protointerface.statements.PIPreparedNamedStatement;
import org.polypheny.db.protointerface.statements.PIStatement;
import org.polypheny.db.protointerface.statements.PIUnparameterizedStatement;
import org.polypheny.db.protointerface.statements.PIUnparameterizedStatementBatch;
import org.polypheny.db.protointerface.utils.PropertyUtils;
import org.polypheny.db.protointerface.utils.ProtoUtils;
import org.polypheny.db.protointerface.utils.ProtoValueDeserializer;
import org.polypheny.db.sql.language.SqlJdbcFunctionCall;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Util;

@Slf4j
public class PIService {

    private static final int majorApiVersion = 2;
    private static final int minorApiVersion = 0;
    private final ClientManager clientManager;
    private final Socket con;
    private String uuid = null;


    public PIService( Socket con, ClientManager clientManager ) {
        this.con = con;
        this.clientManager = clientManager;
        Thread t = new Thread( this::acceptLoop );
        t.start();
    }


    private boolean handleFirstMessage( InputStream in, OutputStream out ) throws IOException {
        boolean success = false;
        Request firstReq = readOneMessage( in );

        if ( firstReq.getTypeCase() != TypeCase.CONNECTION_REQUEST ) {
            Response r = Response.newBuilder()
                    .setId( firstReq.getId() )
                    .setLast( true )
                    .setErrorResponse( ErrorResponse.newBuilder().setMessage( "First message must be a connection request" ) )
                    .build();
            sendOneMessage( r, out );
            return false;
        }

        Response r;
        try {
            r = connect( firstReq.getConnectionRequest(), new ResponseMaker<>( firstReq, "connection_response" ) );
            success = true;
        } catch ( TransactionException | AuthenticationException e ) {
            r = Response.newBuilder()
                    .setId( firstReq.getId() )
                    .setLast( true )
                    .setErrorResponse( ErrorResponse.newBuilder().setMessage( e.getMessage() ) )
                    .build();
        }
        sendOneMessage( r, out );
        return success;
    }


    private void handleMessages( InputStream in, OutputStream out ) throws IOException {
        if ( !handleFirstMessage( in, out ) ) {
            return;
        }
        while ( true ) {
            Request req = readOneMessage( in );
            CompletableFuture<Response> resp = CompletableFuture.supplyAsync( () -> {
                try {
                    return handleMessage( req, out );
                } catch ( TransactionException | AuthenticationException | IOException e ) {
                    throw new PIServiceException( e );
                }
            } );
            Response r;
            try {
                r = resp.get();
            } catch ( Throwable t ) {
                log.error( t.getMessage() );
                r = Response.newBuilder()
                        .setId( req.getId() )
                        .setLast( true )
                        .setErrorResponse( ErrorResponse.newBuilder().setMessage( t.getMessage() ) )
                        .build();
            }
            sendOneMessage( r, out );
            if ( r.getTypeCase() == Response.TypeCase.DISCONNECT_RESPONSE || r.getTypeCase() == Response.TypeCase.ERROR_RESPONSE ) {
                return;
            }
        }
    }


    private void acceptLoop() {
        try {
            InputStream in = con.getInputStream();
            OutputStream out = con.getOutputStream();
            handleMessages( in, out );
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


    private static void sendOneMessage( Response r, OutputStream out ) throws IOException {
        byte[] b = r.toByteArray();
        ByteBuffer bb = ByteBuffer.allocate( 8 );
        bb.order( ByteOrder.LITTLE_ENDIAN );
        bb.putLong( b.length );
        out.write( bb.array() );
        out.write( b );
    }


    private static Request readOneMessage( InputStream in ) throws IOException {
        byte[] b = in.readNBytes( 8 );
        if ( b.length != 8 ) {
            if ( b.length == 0 ) { // EOF
                throw new EOFException();
            }
            throw new IOException( "short read" );
        }
        ByteBuffer bb = ByteBuffer.wrap( b );
        bb.order( ByteOrder.LITTLE_ENDIAN ); // TODO Big endian like other network protocols?
        long length = bb.getLong();
        byte[] msg = in.readNBytes( (int) length );
        return Request.parseFrom( msg );
    }


    private Response handleMessage( Request req, OutputStream out ) throws TransactionException, AuthenticationException, IOException {
        return switch ( req.getTypeCase() ) {
            case DBMS_VERSION_REQUEST -> getDbmsVersion( req.getDbmsVersionRequest(), new ResponseMaker<>( req, "dbms_version_response" ) );
            case LANGUAGE_REQUEST -> throw new NotImplementedException( "Currently not used" );
            case DATABASES_REQUEST -> getDatabases( req.getDatabasesRequest(), new ResponseMaker<>( req, "databases_response" ) );
            case TABLE_TYPES_REQUEST -> getTableTypes( req.getTableTypesRequest(), new ResponseMaker<>( req, "table_types_response" ) );
            case TYPES_REQUEST -> getTypes( req.getTypesRequest(), new ResponseMaker<>( req, "types_response" ) );
            case USER_DEFINED_TYPES_REQUEST -> throw new NotImplementedException();
            case CLIENT_INFO_PROPERTY_META_REQUEST -> throw new NotImplementedException();
            case PROCEDURES_REQUEST -> searchProcedures( req.getProceduresRequest(), new ResponseMaker<>( req, "procedures_response" ) );
            case FUNCTIONS_REQUEST -> searchFunctions( req.getFunctionsRequest(), new ResponseMaker<>( req, "functions_response" ) );
            case NAMESPACES_REQUEST -> searchNamespaces( req.getNamespacesRequest(), new ResponseMaker<>( req, "namespaces_response" ) );
            case NAMESPACE_REQUEST -> throw new NotImplementedException( "Currently not used" );
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
            case EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST -> executeUnparameterizedStatement( req.getExecuteUnparameterizedStatementRequest(), out, new ResponseMaker<>( req, "statement_response" ) );
            case EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST -> executeUnparameterizedStatementBatch( req.getExecuteUnparameterizedStatementBatchRequest(), out, new ResponseMaker<>( req, "statement_batch_response" ) );
            case PREPARE_INDEXED_STATEMENT_REQUEST -> prepareIndexedStatement( req.getPrepareIndexedStatementRequest(), new ResponseMaker<>( req, "prepared_statement_signature" ) );
            case EXECUTE_INDEXED_STATEMENT_REQUEST -> executeIndexedStatement( req.getExecuteIndexedStatementRequest(), new ResponseMaker<>( req, "statement_result" ) );
            case EXECUTE_INDEXED_STATEMENT_BATCH_REQUEST -> throw new NotImplementedException( "Not yet implemented" );
            case PREPARE_NAMED_STATEMENT_REQUEST -> prepareNamedStatement( req.getPrepareNamedStatementRequest(), new ResponseMaker<>( req, "prepared_statement_signature" ) );
            case EXECUTE_NAMED_STATEMENT_REQUEST -> executeNamedStatement( req.getExecuteNamedStatementRequest(), new ResponseMaker<>( req, "statement_result" ) );
            case FETCH_REQUEST -> fetchResult( req.getFetchRequest(), new ResponseMaker<>( req, "frame" ) );
            case CLOSE_STATEMENT_REQUEST -> closeStatement( req.getCloseStatementRequest(), new ResponseMaker<>( req, "close_statement_response" ) );
            case COMMIT_REQUEST -> commitTransaction( req.getCommitRequest(), new ResponseMaker<>( req, "commit_response" ) );
            case ROLLBACK_REQUEST -> rollbackTransaction( req.getRollbackRequest(), new ResponseMaker<>( req, "rollback_response" ) );
            case CONNECTION_PROPERTIES_UPDATE_REQUEST -> updateConnectionProperties( req.getConnectionPropertiesUpdateRequest(), new ResponseMaker<>( req, "connection_properties_update_response" ) );
            case TYPE_NOT_SET -> throw new NotImplementedException( "Unsupported call " + req.getTypeCase() );
        };
    }


    public Response connect( ConnectionRequest request, ResponseMaker<ConnectionResponse> responseObserver ) throws TransactionException, AuthenticationException {
        if ( uuid != null ) {
            throw new PIServiceException( "Can only connect once per session" );
        }
        Builder responseBuilder = ConnectionResponse.newBuilder()
                .setMajorApiVersion( majorApiVersion )
                .setMinorApiVersion( minorApiVersion );
        boolean isCompatible = checkApiVersion( request );
        responseBuilder.setIsCompatible( isCompatible );
        ConnectionResponse ConnectionResponse = responseBuilder.build();
        // reject incompatible client
        if ( !isCompatible ) {
            log.info( "Incompatible client and server version" );
            return responseObserver.makeResponse( ConnectionResponse );
        }

        uuid = clientManager.registerConnection( request );
        return responseObserver.makeResponse( ConnectionResponse );
    }


    public Response disconnect( DisconnectRequest request, ResponseMaker<DisconnectResponse> responseObserver ) {
        PIClient client = getClient();
        clientManager.unregisterConnection( client );
        uuid = null;
        return responseObserver.makeResponse( DisconnectResponse.newBuilder().build() );
    }


    public Response checkConnection( ConnectionCheckRequest request, ResponseMaker<ConnectionCheckResponse> responseObserver ) {
        getClient().setIsActive();
        return responseObserver.makeResponse( ConnectionCheckResponse.newBuilder().build() );
    }


    public Response getDbmsVersion( DbmsVersionRequest request, ResponseMaker<DbmsVersionResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        return responseObserver.makeResponse( DbMetaRetriever.getDbmsVersion() );
    }


    public void getSupportedLanguages( LanguageRequest request, StreamObserver<LanguageResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        LanguageResponse supportedLanguages = LanguageResponse.newBuilder()
                .addAllLanguageNames( new LinkedList<>() )
                .build();
        responseObserver.onNext( supportedLanguages );
        responseObserver.onCompleted();
    }


    private MetaStringResponse buildMetaStringResponse( String string ) {
        return MetaStringResponse.newBuilder()
                .setString( string )
                .build();
    }


    public Response getDatabases( DatabasesRequest request, ResponseMaker<DatabasesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        return responseObserver.makeResponse( DbMetaRetriever.getDatabases() );
    }


    public Response getTableTypes( TableTypesRequest request, ResponseMaker<TableTypesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        return responseObserver.makeResponse( DbMetaRetriever.getTableTypes() );
    }


    public Response getTypes( TypesRequest request, ResponseMaker<TypesResponse> responseStreamObserver ) {
        /* called as client auth check */
        getClient();
        return responseStreamObserver.makeResponse( DbMetaRetriever.getTypes() );
    }


    public Response searchNamespaces( NamespacesRequest request, ResponseMaker<NamespacesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        String namespacePattern = request.hasNamespacePattern() ? request.getNamespacePattern() : null;
        String namespaceType = request.hasNamespaceType() ? request.getNamespaceType() : null;
        return responseObserver.makeResponse( DbMetaRetriever.searchNamespaces( namespacePattern, namespaceType ) );
    }


    public void getNamespace( NamespaceRequest request, StreamObserver<Namespace> responseObserver ) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext( DbMetaRetriever.getNamespace( request.getNamespaceName() ) );
        responseObserver.onCompleted();
    }


    public Response searchEntities( EntitiesRequest request, ResponseMaker<EntitiesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        String entityPattern = request.hasEntityPattern() ? request.getEntityPattern() : null;
        return responseObserver.makeResponse( DbMetaRetriever.searchEntities( request.getNamespaceName(), entityPattern ) );
    }


    public Response getSqlStringFunctions( SqlStringFunctionsRequest request, ResponseMaker<MetaStringResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        return responseObserver.makeResponse( buildMetaStringResponse( SqlJdbcFunctionCall.getStringFunctions() ) );
    }


    public Response getSqlSystemFunctions( SqlSystemFunctionsRequest request, ResponseMaker<MetaStringResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        return responseObserver.makeResponse( buildMetaStringResponse( SqlJdbcFunctionCall.getSystemFunctions() ) );
    }


    public Response getSqlTimeDateFunctions( SqlTimeDateFunctionsRequest request, ResponseMaker<MetaStringResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        return responseObserver.makeResponse( buildMetaStringResponse( SqlJdbcFunctionCall.getTimeDateFunctions() ) );
    }


    public Response getSqlNumericFunctions( SqlNumericFunctionsRequest request, ResponseMaker<MetaStringResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        return responseObserver.makeResponse( buildMetaStringResponse( SqlJdbcFunctionCall.getNumericFunctions() ) );
    }


    public Response getSqlKeywords( SqlKeywordsRequest request, ResponseMaker<MetaStringResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        // TODO actually return keywords
        return responseObserver.makeResponse( buildMetaStringResponse( "" ) );
    }


    public Response searchProcedures( ProceduresRequest request, ResponseMaker<ProceduresResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        String procedurePattern = request.hasProcedureNamePattern() ? request.getProcedureNamePattern() : null;
        return responseObserver.makeResponse( DbMetaRetriever.getProcedures( request.getLanguage(), procedurePattern ) );
    }


    public Response searchFunctions( FunctionsRequest request, ResponseMaker<FunctionsResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        QueryLanguage queryLanguage = QueryLanguage.from( request.getQueryLanguage() );
        FunctionCategory functionCategory = FunctionCategory.valueOf( request.getFunctionCategory() );
        return responseObserver.makeResponse( DbMetaRetriever.getFunctions( queryLanguage, functionCategory ) );
    }


    public Response executeUnparameterizedStatement( ExecuteUnparameterizedStatementRequest request, OutputStream out, ResponseMaker<StatementResponse> responseObserver ) throws IOException {
        PIClient client = getClient();
        PIUnparameterizedStatement statement = client.getStatementManager().createUnparameterizedStatement( request );
        Response mid = responseObserver.makeResponse( ProtoUtils.createResult( statement ), false );
        sendOneMessage( mid, out );
        StatementResult result = statement.execute(
                request.hasFetchSize()
                        ? request.getFetchSize()
                        : PropertyUtils.DEFAULT_FETCH_SIZE
        );
        return responseObserver.makeResponse( ProtoUtils.createResult( statement, result ) );
    }


    public Response executeUnparameterizedStatementBatch( ExecuteUnparameterizedStatementBatchRequest request, OutputStream out, ResponseMaker<StatementBatchResponse> responseObserver ) throws IOException {
        PIClient client = getClient();
        PIUnparameterizedStatementBatch batch = client.getStatementManager().createUnparameterizedStatementBatch( request.getStatementsList() );
        Response mid = responseObserver.makeResponse( ProtoUtils.createStatementBatchStatus( batch.getBatchId() ), false );
        sendOneMessage( mid, out );
        List<Long> updateCounts = batch.executeBatch();
        return responseObserver.makeResponse( ProtoUtils.createStatementBatchStatus( batch.getBatchId(), updateCounts ) );
    }


    public Response prepareIndexedStatement( PrepareStatementRequest request, ResponseMaker<PreparedStatementSignature> responseObserver ) {
        PIClient client = getClient();
        PIPreparedIndexedStatement statement = client.getStatementManager().createIndexedPreparedInterfaceStatement( request );
        return responseObserver.makeResponse( ProtoUtils.createPreparedStatementSignature( statement ) );
    }


    public Response executeIndexedStatement( ExecuteIndexedStatementRequest request, ResponseMaker<StatementResult> responseObserver ) {
        PIClient client = getClient();
        PIPreparedIndexedStatement statement = client.getStatementManager().getIndexedPreparedStatement( request.getStatementId() );
        int fetchSize = request.hasFetchSize()
                ? request.getFetchSize()
                : PropertyUtils.DEFAULT_FETCH_SIZE;
        return responseObserver.makeResponse( statement.execute( ProtoValueDeserializer.deserializeParameterList( request.getParameters().getParametersList() ), fetchSize ) );
    }


    public void executeIndexedStatementBatch( ExecuteIndexedStatementBatchRequest request, StreamObserver<StatementBatchResponse> resultObserver ) throws Exception {
        PIClient client = getClient();
        PIPreparedIndexedStatement statement = client.getStatementManager().getIndexedPreparedStatement( request.getStatementId() );
        List<List<PolyValue>> valuesList = ProtoValueDeserializer.deserializeParameterLists( request.getParametersList() );
        List<Long> updateCounts = statement.executeBatch( valuesList );
        resultObserver.onNext( ProtoUtils.createStatementBatchStatus( statement.getId(), updateCounts ) );
        resultObserver.onCompleted();
    }


    public Response prepareNamedStatement( PrepareStatementRequest request, ResponseMaker<PreparedStatementSignature> responseObserver ) {
        PIClient client = getClient();
        PIPreparedNamedStatement statement = client.getStatementManager().createNamedPreparedInterfaceStatement( request );
        return responseObserver.makeResponse( ProtoUtils.createPreparedStatementSignature( statement ) );
    }


    public Response executeNamedStatement( ExecuteNamedStatementRequest request, ResponseMaker<StatementResult> responseObserver ) {
        PIClient client = getClient();
        PIPreparedNamedStatement statement = client.getStatementManager().getNamedPreparedStatement( request.getStatementId() );
        int fetchSize = request.hasFetchSize()
                ? request.getFetchSize()
                : PropertyUtils.DEFAULT_FETCH_SIZE;
        try {
            return responseObserver.makeResponse( statement.execute( ProtoValueDeserializer.deserilaizeParameterMap( request.getParameters().getParametersMap() ), fetchSize ) );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
    }


    public Response fetchResult( FetchRequest request, ResponseMaker<Frame> responseObserver ) {
        PIClient client = getClient();
        PIStatement statement = client.getStatementManager().getStatement( request.getStatementId() );
        int fetchSize = request.hasFetchSize()
                ? request.getFetchSize()
                : PropertyUtils.DEFAULT_FETCH_SIZE;
        Frame frame = StatementProcessor.fetch( statement, fetchSize );
        return responseObserver.makeResponse( frame );
    }


    public Response commitTransaction( CommitRequest request, ResponseMaker<CommitResponse> responseStreamObserver ) {
        PIClient client = getClient();
        client.commitCurrentTransaction();
        return responseStreamObserver.makeResponse( CommitResponse.newBuilder().build() );
    }


    public Response rollbackTransaction( RollbackRequest request, ResponseMaker<RollbackResponse> responseStreamObserver ) {
        PIClient client = getClient();
        client.rollbackCurrentTransaction();
        return responseStreamObserver.makeResponse( RollbackResponse.newBuilder().build() );
    }


    public Response closeStatement( CloseStatementRequest request, ResponseMaker<CloseStatementResponse> responseObserver ) {
        PIClient client = getClient();
        client.getStatementManager().closeStatementOrBatch( request.getStatementId() );
        return responseObserver.makeResponse( CloseStatementResponse.newBuilder().build() );
    }


    public Response updateConnectionProperties( ConnectionPropertiesUpdateRequest request, ResponseMaker<ConnectionPropertiesUpdateResponse> responseObserver ) {
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


    public Response getClientInfoProperties( ClientInfoPropertiesRequest request, ResponseMaker<ClientInfoProperties> responseObserver ) {
        PIClient client = getClient();
        ClientInfoProperties.Builder responseBuilder = ClientInfoProperties.newBuilder();
        PIClientInfoProperties PIClientInfoProperties = client.getPIClientInfoProperties();
        PIClientInfoProperties.stringPropertyNames().forEach( s -> responseBuilder.putProperties( s, PIClientInfoProperties.getProperty( s ) ) );
        return responseObserver.makeResponse( responseBuilder.build() );
    }


    public Response setClientInfoProperties( ClientInfoProperties properties, ResponseMaker<ClientInfoPropertiesResponse> reponseObserver ) {
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
        return connectionRequest.getMajorApiVersion() == majorApiVersion;
    }

}
