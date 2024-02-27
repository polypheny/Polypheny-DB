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
import java.util.concurrent.atomic.AtomicBoolean;
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
    private ClientManager clientManager;
    private final Socket con;
    private String uuid = null;


    public PIService( Socket con, ClientManager clientManager ) {
        this.con = con;
        this.clientManager = clientManager;
        Thread t = new Thread( this::acceptLoop );
        t.start();
    }


    private void acceptLoop() {
        try {
            InputStream in = con.getInputStream();
            OutputStream out = con.getOutputStream();
            while ( true ) {
                readAndHandleOneMessage( in, out );
            }
        } catch ( Throwable e ) {
            if ( uuid != null ) {
                clientManager.unregisterConnection( clientManager.getClient( uuid ) );
            }

            Util.closeNoThrow( con );
            if ( !(e instanceof EOFException) ) {
                throw new GenericRuntimeException( e );
            }
        }
    }


    private static Request readRequest( InputStream in ) throws IOException {
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


    private void handleMessage( Request req, OutputStream os, AtomicBoolean done ) throws TransactionException, AuthenticationException {
        switch ( req.getTypeCase() ) {
            case DBMS_VERSION_REQUEST, LANGUAGE_REQUEST, DATABASES_REQUEST, TABLE_TYPES_REQUEST, TYPES_REQUEST, USER_DEFINED_TYPES_REQUEST, CLIENT_INFO_PROPERTY_META_REQUEST, PROCEDURES_REQUEST, FUNCTIONS_REQUEST, NAMESPACES_REQUEST, NAMESPACE_REQUEST, ENTITIES_REQUEST, SQL_STRING_FUNCTIONS_REQUEST, SQL_SYSTEM_FUNCTIONS_REQUEST, SQL_TIME_DATE_FUNCTIONS_REQUEST, SQL_NUMERIC_FUNCTIONS_REQUEST, SQL_KEYWORDS_REQUEST -> throw new NotImplementedException( "Unsupported call " + req.getTypeCase() );
            case CONNECTION_REQUEST -> connect( req.getConnectionRequest(), new StreamObserver<>( req, os, "connection_response", done ) );
            case CONNECTION_CHECK_REQUEST -> throw new GenericRuntimeException( "ee" );
            case DISCONNECT_REQUEST -> disconnect( req.getDisconnectRequest(), new StreamObserver<>( req, os, "disconnect_response", done ) );
            case CLIENT_INFO_PROPERTIES_REQUEST, CLIENT_INFO_PROPERTIES -> throw new NotImplementedException( "Unsupported call " + req.getTypeCase() );
            case EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST -> executeUnparameterizedStatement( req.getExecuteUnparameterizedStatementRequest(), new StreamObserver<>( req, os, "statement_response", done ) );
            case EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST -> throw new GenericRuntimeException( "eee" );
            case PREPARE_INDEXED_STATEMENT_REQUEST -> prepareIndexedStatement( req.getPrepareIndexedStatementRequest(), new StreamObserver<>( req, os, "prepared_statement_signature", done ) );
            case EXECUTE_INDEXED_STATEMENT_REQUEST -> executeIndexedStatement( req.getExecuteIndexedStatementRequest(), new StreamObserver<>( req, os, "statement_result", done ) );
            case EXECUTE_INDEXED_STATEMENT_BATCH_REQUEST -> throw new GenericRuntimeException( "eee" );
            case PREPARE_NAMED_STATEMENT_REQUEST -> prepareNamedStatement( req.getPrepareNamedStatementRequest(), new StreamObserver<>( req, os, "prepared_statement_signature", done ) );
            case EXECUTE_NAMED_STATEMENT_REQUEST -> executeNamedStatement( req.getExecuteNamedStatementRequest(), new StreamObserver<>( req, os, "statement_result", done ) );
            case FETCH_REQUEST -> fetchResult( req.getFetchRequest(), new StreamObserver<>( req, os, "frame", done ) );
            case CLOSE_STATEMENT_REQUEST -> closeStatement( req.getCloseStatementRequest(), new StreamObserver<>( req, os, "close_statement_response", done ) );
            case COMMIT_REQUEST -> commitTransaction( req.getCommitRequest(), new StreamObserver<>( req, os, "commit_response", done ) );
            case ROLLBACK_REQUEST -> rollbackTransaction( req.getRollbackRequest(), new StreamObserver<>( req, os, "rollback_response", done ) );
            case CONNECTION_PROPERTIES_UPDATE_REQUEST, TYPE_NOT_SET -> throw new NotImplementedException( "Unsupported call " + req.getTypeCase() );
        }
    }


    private void readAndHandleOneMessage( InputStream in, OutputStream out ) throws IOException {
        Request req = readRequest( in );
        AtomicBoolean done = new AtomicBoolean( false );
        if ( req.getId() == 0 ) {
            throw new GenericRuntimeException( "client send message with invalid id" );
        }
        try {
            handleMessage( req, out, done );
        } catch ( Exception | TransactionException e ) {
            if ( !done.get() ) {
                StreamObserver<ErrorResponse> s = new StreamObserver<>( req, out, "error_response", new AtomicBoolean() );
                ErrorResponse resp = ErrorResponse.newBuilder().setMessage( e.getMessage() ).build();
                s.onNext( resp );
                s.onCompleted();
            }
            if ( e instanceof AuthenticationException ) {
                // TODO: log?
                throw new EOFException(); // TODO: Better way to communicate close connection w/o error
            }
            throw new GenericRuntimeException( e );
        }
    }


    public void connect( ConnectionRequest request, StreamObserver<ConnectionResponse> responseObserver ) throws TransactionException, AuthenticationException {
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
            responseObserver.onNext( ConnectionResponse );
            responseObserver.onCompleted();
            throw new GenericRuntimeException( "Incompatible" );
        }

        uuid = clientManager.registerConnection( request );
        responseObserver.onNext( ConnectionResponse );
        responseObserver.onCompleted();
    }


    public void disconnect( DisconnectRequest request, StreamObserver<DisconnectResponse> responseObserver ) {
        PIClient client = getClient();
        clientManager.unregisterConnection( client );
        responseObserver.onNext( DisconnectResponse.newBuilder().build() );
        responseObserver.onCompleted();
    }


    public void checkConnection( ConnectionCheckRequest request, StreamObserver<ConnectionCheckResponse> responseObserver ) {
        getClient().setIsActive();
        responseObserver.onNext( ConnectionCheckResponse.newBuilder().build() );
        responseObserver.onCompleted();
    }


    public void getDbmsVersion( DbmsVersionRequest request, StreamObserver<DbmsVersionResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext( DbMetaRetriever.getDbmsVersion() );
        responseObserver.onCompleted();
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


    public void getDatabases( DatabasesRequest request, StreamObserver<DatabasesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext( DbMetaRetriever.getDatabases() );
        responseObserver.onCompleted();
    }


    public void getTableTypes( TableTypesRequest request, StreamObserver<TableTypesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext( DbMetaRetriever.getTableTypes() );
        responseObserver.onCompleted();
    }


    public void getTypes( TypesRequest request, StreamObserver<TypesResponse> responseStreamObserver ) {
        /* called as client auth check */
        getClient();
        responseStreamObserver.onNext( DbMetaRetriever.getTypes() );
        responseStreamObserver.onCompleted();
    }


    public void searchNamespaces( NamespacesRequest request, StreamObserver<NamespacesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        String namespacePattern = request.hasNamespacePattern() ? request.getNamespacePattern() : null;
        String namespaceType = request.hasNamespaceType() ? request.getNamespaceType() : null;
        responseObserver.onNext( DbMetaRetriever.searchNamespaces( namespacePattern, namespaceType ) );
        responseObserver.onCompleted();
    }


    public void getNamespace( NamespaceRequest request, StreamObserver<Namespace> responseObserver ) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext( DbMetaRetriever.getNamespace( request.getNamespaceName() ) );
        responseObserver.onCompleted();
    }


    public void searchEntities( EntitiesRequest request, StreamObserver<EntitiesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        String entityPattern = request.hasEntityPattern() ? request.getEntityPattern() : null;
        responseObserver.onNext( DbMetaRetriever.searchEntities( request.getNamespaceName(), entityPattern ) );
        responseObserver.onCompleted();
    }


    public void getSqlStringFunctions( SqlStringFunctionsRequest request, StreamObserver<MetaStringResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext( buildMetaStringResponse( SqlJdbcFunctionCall.getStringFunctions() ) );
        responseObserver.onCompleted();
    }


    public void getSqlSystemFunctions( SqlSystemFunctionsRequest request, StreamObserver<MetaStringResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext( buildMetaStringResponse( SqlJdbcFunctionCall.getSystemFunctions() ) );
        responseObserver.onCompleted();
    }


    public void getSqlTimeDateFunctions( SqlTimeDateFunctionsRequest request, StreamObserver<MetaStringResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext( buildMetaStringResponse( SqlJdbcFunctionCall.getTimeDateFunctions() ) );
        responseObserver.onCompleted();
    }


    public void getSqlNumericFunctions( SqlNumericFunctionsRequest request, StreamObserver<MetaStringResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext( buildMetaStringResponse( SqlJdbcFunctionCall.getNumericFunctions() ) );
        responseObserver.onCompleted();
    }


    public void getSqlKeywords( SqlKeywordsRequest request, StreamObserver<MetaStringResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        // TODO actually return keywords
        responseObserver.onNext( buildMetaStringResponse( "" ) );
        responseObserver.onCompleted();
    }


    public void searchProcedures( ProceduresRequest request, StreamObserver<ProceduresResponse> responeObserver ) {
        /* called as client auth check */
        getClient();
        String procedurePattern = request.hasProcedureNamePattern() ? request.getProcedureNamePattern() : null;
        responeObserver.onNext( DbMetaRetriever.getProcedures( request.getLanguage(), procedurePattern ) );
        responeObserver.onCompleted();
    }


    public void searchFunctions( FunctionsRequest request, StreamObserver<FunctionsResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        QueryLanguage queryLanguage = QueryLanguage.from( request.getQueryLanguage() );
        FunctionCategory functionCategory = FunctionCategory.valueOf( request.getFunctionCategory() );
        responseObserver.onNext( DbMetaRetriever.getFunctions( queryLanguage, functionCategory ) );
        responseObserver.onCompleted();
    }


    public void executeUnparameterizedStatement( ExecuteUnparameterizedStatementRequest request, StreamObserver<StatementResponse> responseObserver ) {
        PIClient client = getClient();
        PIUnparameterizedStatement statement = client.getStatementManager().createUnparameterizedStatement( request );
        responseObserver.onNext( ProtoUtils.createResult( statement ), false );
        StatementResult result = statement.execute(
                request.hasFetchSize()
                        ? request.getFetchSize()
                        : PropertyUtils.DEFAULT_FETCH_SIZE
        );
        responseObserver.onNext( ProtoUtils.createResult( statement, result ) );
        responseObserver.onCompleted();
    }


    public void executeUnparameterizedStatementBatch( ExecuteUnparameterizedStatementBatchRequest request, StreamObserver<StatementBatchResponse> responseObserver ) throws Exception {
        PIClient client = getClient();
        PIUnparameterizedStatementBatch batch = client.getStatementManager().createUnparameterizedStatementBatch( request.getStatementsList() );
        responseObserver.onNext( ProtoUtils.createStatementBatchStatus( batch.getBatchId() ) );
        List<Long> updateCounts = batch.executeBatch();
        responseObserver.onNext( ProtoUtils.createStatementBatchStatus( batch.getBatchId(), updateCounts ) );
        responseObserver.onCompleted();
    }


    public void prepareIndexedStatement( PrepareStatementRequest request, StreamObserver<PreparedStatementSignature> responseObserver ) {
        PIClient client = getClient();
        PIPreparedIndexedStatement statement = client.getStatementManager().createIndexedPreparedInterfaceStatement( request );
        responseObserver.onNext( ProtoUtils.createPreparedStatementSignature( statement ) );
        responseObserver.onCompleted();
    }


    public void executeIndexedStatement( ExecuteIndexedStatementRequest request, StreamObserver<StatementResult> responseObserver ) {
        PIClient client = getClient();
        PIPreparedIndexedStatement statement = client.getStatementManager().getIndexedPreparedStatement( request.getStatementId() );
        int fetchSize = request.hasFetchSize()
                ? request.getFetchSize()
                : PropertyUtils.DEFAULT_FETCH_SIZE;
        try {
            responseObserver.onNext( statement.execute( ProtoValueDeserializer.deserializeParameterList( request.getParameters().getParametersList() ), fetchSize ) );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
        responseObserver.onCompleted();
    }


    public void executeIndexedStatementBatch( ExecuteIndexedStatementBatchRequest request, StreamObserver<StatementBatchResponse> resultObserver ) throws Exception {
        PIClient client = getClient();
        PIPreparedIndexedStatement statement = client.getStatementManager().getIndexedPreparedStatement( request.getStatementId() );
        List<List<PolyValue>> valuesList = ProtoValueDeserializer.deserializeParameterLists( request.getParametersList() );
        List<Long> updateCounts = statement.executeBatch( valuesList );
        resultObserver.onNext( ProtoUtils.createStatementBatchStatus( statement.getId(), updateCounts ) );
        resultObserver.onCompleted();
    }


    public void prepareNamedStatement( PrepareStatementRequest request, StreamObserver<PreparedStatementSignature> responseObserver ) {
        PIClient client = getClient();
        PIPreparedNamedStatement statement = client.getStatementManager().createNamedPreparedInterfaceStatement( request );
        responseObserver.onNext( ProtoUtils.createPreparedStatementSignature( statement ) );
        responseObserver.onCompleted();
    }


    public void executeNamedStatement( ExecuteNamedStatementRequest request, StreamObserver<StatementResult> responseObserver ) {
        PIClient client = getClient();
        PIPreparedNamedStatement statement = client.getStatementManager().getNamedPreparedStatement( request.getStatementId() );
        int fetchSize = request.hasFetchSize()
                ? request.getFetchSize()
                : PropertyUtils.DEFAULT_FETCH_SIZE;
        try {
            responseObserver.onNext( statement.execute( ProtoValueDeserializer.deserilaizeParameterMap( request.getParameters().getParametersMap() ), fetchSize ) );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
        responseObserver.onCompleted();
    }


    public void fetchResult( FetchRequest request, StreamObserver<Frame> responseObserver ) {
        PIClient client = getClient();
        PIStatement statement = client.getStatementManager().getStatement( request.getStatementId() );
        int fetchSize = request.hasFetchSize()
                ? request.getFetchSize()
                : PropertyUtils.DEFAULT_FETCH_SIZE;
        Frame frame = StatementProcessor.fetch( statement, fetchSize );
        responseObserver.onNext( frame );
        responseObserver.onCompleted();
    }


    public void commitTransaction( CommitRequest request, StreamObserver<CommitResponse> responseStreamObserver ) {
        PIClient client = getClient();
        client.commitCurrentTransaction();
        responseStreamObserver.onNext( CommitResponse.newBuilder().build() );
        responseStreamObserver.onCompleted();
    }


    public void rollbackTransaction( RollbackRequest request, StreamObserver<RollbackResponse> responseStreamObserver ) {
        PIClient client = getClient();
        client.rollbackCurrentTransaction();
        responseStreamObserver.onNext( RollbackResponse.newBuilder().build() );
        responseStreamObserver.onCompleted();
    }


    public void closeStatement( CloseStatementRequest request, StreamObserver<CloseStatementResponse> responseObserver ) {
        PIClient client = getClient();
        client.getStatementManager().closeStatementOrBatch( request.getStatementId() );
        responseObserver.onNext( CloseStatementResponse.newBuilder().build() );
        responseObserver.onCompleted();
    }


    public void updateConnectionProperties( ConnectionPropertiesUpdateRequest request, StreamObserver<ConnectionPropertiesUpdateResponse> responseObserver ) {
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
        responseObserver.onNext( ConnectionPropertiesUpdateResponse.newBuilder().build() );
        responseObserver.onCompleted();
    }


    public void getClientInfoProperties( ClientInfoPropertiesRequest request, StreamObserver<ClientInfoProperties> responseObserver ) {
        PIClient client = getClient();
        ClientInfoProperties.Builder responseBuilder = ClientInfoProperties.newBuilder();
        PIClientInfoProperties PIClientInfoProperties = client.getPIClientInfoProperties();
        PIClientInfoProperties.stringPropertyNames().forEach( s -> responseBuilder.putProperties( s, PIClientInfoProperties.getProperty( s ) ) );
        responseObserver.onNext( responseBuilder.build() );
        responseObserver.onCompleted();
    }


    public void setClientInfoProperties( ClientInfoProperties properties, StreamObserver<ClientInfoPropertiesResponse> reponseObserver ) {
        PIClient client = getClient();
        client.getPIClientInfoProperties().putAll( properties.getPropertiesMap() );
        reponseObserver.onNext( ClientInfoPropertiesResponse.newBuilder().build() );
        reponseObserver.onCompleted();
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
