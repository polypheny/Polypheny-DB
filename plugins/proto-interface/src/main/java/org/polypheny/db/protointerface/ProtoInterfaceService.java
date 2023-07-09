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

import io.grpc.stub.StreamObserver;
import java.util.LinkedList;
import java.util.List;
import lombok.SneakyThrows;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.protointerface.proto.CloseStatementRequest;
import org.polypheny.db.protointerface.proto.CloseStatementResponse;
import org.polypheny.db.protointerface.proto.ColumnsRequest;
import org.polypheny.db.protointerface.proto.ColumnsResponse;
import org.polypheny.db.protointerface.proto.CommitRequest;
import org.polypheny.db.protointerface.proto.CommitResponse;
import org.polypheny.db.protointerface.proto.ConnectionCheckRequest;
import org.polypheny.db.protointerface.proto.ConnectionCheckResponse;
import org.polypheny.db.protointerface.proto.ConnectionReply;
import org.polypheny.db.protointerface.proto.ConnectionRequest;
import org.polypheny.db.protointerface.proto.DatabasesRequest;
import org.polypheny.db.protointerface.proto.DatabasesResponse;
import org.polypheny.db.protointerface.proto.DbmsVersionRequest;
import org.polypheny.db.protointerface.proto.DbmsVersionResponse;
import org.polypheny.db.protointerface.proto.ExportedKeysRequest;
import org.polypheny.db.protointerface.proto.ExportedKeysResponse;
import org.polypheny.db.protointerface.proto.FetchRequest;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.ImportedKeysRequest;
import org.polypheny.db.protointerface.proto.ImportedKeysResponse;
import org.polypheny.db.protointerface.proto.IndexesRequest;
import org.polypheny.db.protointerface.proto.IndexesResponse;
import org.polypheny.db.protointerface.proto.LanguageRequest;
import org.polypheny.db.protointerface.proto.LanguageResponse;
import org.polypheny.db.protointerface.proto.NamespacesRequest;
import org.polypheny.db.protointerface.proto.NamespacesResponse;
import org.polypheny.db.protointerface.proto.ParameterList;
import org.polypheny.db.protointerface.proto.ParameterSet;
import org.polypheny.db.protointerface.proto.PreparedStatement;
import org.polypheny.db.protointerface.proto.PreparedStatementSignature;
import org.polypheny.db.protointerface.proto.PrimaryKeysRequest;
import org.polypheny.db.protointerface.proto.PrimaryKeysResponse;
import org.polypheny.db.protointerface.proto.ProtoInterfaceGrpc;
import org.polypheny.db.protointerface.proto.RollbackRequest;
import org.polypheny.db.protointerface.proto.RollbackResponse;
import org.polypheny.db.protointerface.proto.StatementBatchStatus;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.proto.StatementStatus;
import org.polypheny.db.protointerface.proto.TableTypesRequest;
import org.polypheny.db.protointerface.proto.TableTypesResponse;
import org.polypheny.db.protointerface.proto.TablesRequest;
import org.polypheny.db.protointerface.proto.TablesResponse;
import org.polypheny.db.protointerface.proto.TypesRequest;
import org.polypheny.db.protointerface.proto.TypesResponse;
import org.polypheny.db.protointerface.proto.UnparameterizedStatement;
import org.polypheny.db.protointerface.proto.UnparameterizedStatementBatch;
import org.polypheny.db.protointerface.statements.IndexedPreparedInterfaceStatement;
import org.polypheny.db.protointerface.statements.NamedPreparedInterfaceStatement;
import org.polypheny.db.protointerface.statements.ProtoInterfaceStatement;
import org.polypheny.db.protointerface.statements.ProtoInterfaceStatementBatch;
import org.polypheny.db.protointerface.statements.UnparameterizedInterfaceStatement;
import org.polypheny.db.protointerface.utils.ProtoUtils;
import org.polypheny.db.protointerface.utils.ProtoValueDeserializer;

public class ProtoInterfaceService extends ProtoInterfaceGrpc.ProtoInterfaceImplBase {

    private static final int majorApiVersion = 2;
    private static final int minorApiVersion = 0;
    private ClientManager clientManager;
    private StatementManager statementManager;


    public ProtoInterfaceService( ClientManager clientManager ) {
        this.clientManager = clientManager;
        this.statementManager = new StatementManager();
    }


    @SneakyThrows
    @Override
    public void connect( ConnectionRequest connectionRequest, StreamObserver<ConnectionReply> responseObserver ) {
        ConnectionReply.Builder responseBuilder = ConnectionReply.newBuilder()
                .setMajorApiVersion( majorApiVersion )
                .setMinorApiVersion( minorApiVersion );
        boolean isCompatible = checkApiVersion( connectionRequest );
        responseBuilder.setIsCompatible( isCompatible );
        ConnectionReply connectionReply = responseBuilder.build();
        // reject incompatible client
        if ( !isCompatible ) {
            responseObserver.onNext( connectionReply );
            responseObserver.onCompleted();
            return;
        }
        clientManager.registerConnection( connectionRequest );
        responseObserver.onNext( connectionReply );
        responseObserver.onCompleted();
    }


    @Override
    public void checkConnection( ConnectionCheckRequest connectionCheckRequest, StreamObserver<ConnectionCheckResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext( ConnectionCheckResponse.newBuilder().build() );
        responseObserver.onCompleted();
    }


    @Override
    public void getDbmsVersion( DbmsVersionRequest dbmsVersionRequest, StreamObserver<DbmsVersionResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        try {
            String versionName = PolyphenyDb.class.getPackage().getImplementationVersion();
            int nextSeparatorIndex = versionName.indexOf( '.' );
            if ( nextSeparatorIndex <= 0 ) {
                throw new ProtoInterfaceServiceException( "Could not parse database version info" );
            }
            int majorVersion = Integer.parseInt( versionName.substring( 0, nextSeparatorIndex ) );

            versionName = versionName.substring( nextSeparatorIndex + 1 );
            nextSeparatorIndex = versionName.indexOf( '.' );
            if ( nextSeparatorIndex <= 0 ) {
                throw new ProtoInterfaceServiceException( "Could not parse database version info" );
            }
            int minorVersion = Integer.parseInt( versionName.substring( 0, nextSeparatorIndex ) );

            DbmsVersionResponse dbmsVersionResponse = DbmsVersionResponse.newBuilder()
                    .setDbmsName( "Polypheny-DB" )
                    .setVersionName( PolyphenyDb.class.getPackage().getImplementationVersion() )
                    .setMajorVersion( majorVersion )
                    .setMinorVersion( minorVersion )
                    .build();
            responseObserver.onNext( dbmsVersionResponse );
            responseObserver.onCompleted();
        } catch ( Exception e ) {
            throw new ProtoInterfaceServiceException( "Could not parse database version info" );
        }
    }


    @Override
    public void getTables( TablesRequest tablesRequest, StreamObserver<TablesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        String namespacePattern = tablesRequest.hasNamespacePattern() ? tablesRequest.getNamespacePattern() : null;
        String tablePattern = tablesRequest.hasTablePattern() ? tablesRequest.getTablePattern() : null;
        List<String> tableTypes = tablesRequest.getTableTypesCount() == 0 ? null : tablesRequest.getTableTypesList();
        responseObserver.onNext( DbMetaRetriever.getTables( namespacePattern, tablePattern, tableTypes ) );
        responseObserver.onCompleted();
    }


    @Override
    public void getTableTypes( TableTypesRequest tableTypesRequest, StreamObserver<TableTypesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext( DbMetaRetriever.getTableTypes() );
        responseObserver.onCompleted();
    }


    @Override
    public void getNamespaces( NamespacesRequest namespacesRequest, StreamObserver<NamespacesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        String namespacePattern = namespacesRequest.hasNamespacePattern() ? namespacesRequest.getNamespacePattern() : null;
        responseObserver.onNext( DbMetaRetriever.getNamespaces( namespacePattern ) );
        responseObserver.onCompleted();
    }


    @Override
    public void getColumns( ColumnsRequest columnsRequest, StreamObserver<ColumnsResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        String namespacePattern = columnsRequest.hasNamespacePattern() ? columnsRequest.getNamespacePattern() : null;
        String tablePattern = columnsRequest.getTablePattern();
        String columnPattern = columnsRequest.getColumnPattern();
        responseObserver.onNext( DbMetaRetriever.getColumns( namespacePattern, tablePattern, columnPattern ) );
        responseObserver.onCompleted();
    }


    @Override
    public void getPrimaryKeys( PrimaryKeysRequest primaryKeysRequest, StreamObserver<PrimaryKeysResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        String namespacePattern = primaryKeysRequest.hasNamespacePattern() ? primaryKeysRequest.getNamespacePattern() : null;
        String tablePattern = primaryKeysRequest.getTablePattern();
        responseObserver.onNext( DbMetaRetriever.getPrimaryKeys( namespacePattern, tablePattern ) );
        responseObserver.onCompleted();
    }


    @Override
    public void getDatabases( DatabasesRequest databasesRequest, StreamObserver<DatabasesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext( DbMetaRetriever.getDatabases() );
        responseObserver.onCompleted();
    }


    @Override
    public void getImportedKeys( ImportedKeysRequest importedKeysRequest, StreamObserver<ImportedKeysResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        String namespacePattern = importedKeysRequest.hasNamespacePattern() ? importedKeysRequest.getNamespacePattern() : null;
        String tablePattern = importedKeysRequest.getTablePattern();
        responseObserver.onNext( DbMetaRetriever.getImportedKeys( namespacePattern, tablePattern ) );
        responseObserver.onCompleted();
    }


    @Override
    public void getExportedKeys( ExportedKeysRequest exportedKeysRequest, StreamObserver<ExportedKeysResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        String namespacePattern = exportedKeysRequest.hasNamespacePattern() ? exportedKeysRequest.getNamespacePattern() : null;
        String tablePattern = exportedKeysRequest.getTablePattern();
        responseObserver.onNext( DbMetaRetriever.getExportedKeys( namespacePattern, tablePattern ) );
        responseObserver.onCompleted();
    }


    @Override
    public void getTypes( TypesRequest typesRequest, StreamObserver<TypesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext( DbMetaRetriever.getTypes() );
        responseObserver.onCompleted();
    }


    @Override
    public void getIndexes( IndexesRequest indexesRequest, StreamObserver<IndexesResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        String namespacePattern = indexesRequest.hasNamespacePattern() ? indexesRequest.getNamespacePattern() : null;
        String tablePattern = indexesRequest.getTablePattern();
        responseObserver.onNext( DbMetaRetriever.getIndexes( namespacePattern, tablePattern, indexesRequest.getUnique() ) );
        responseObserver.onCompleted();
    }


    @Override
    public void getSupportedLanguages( LanguageRequest languageRequest, StreamObserver<LanguageResponse> responseObserver ) {
        /* called as client auth check */
        getClient();
        LanguageResponse supportedLanguages = LanguageResponse.newBuilder()
                .addAllLanguageNames( new LinkedList<>() )
                .build();
        responseObserver.onNext( supportedLanguages );
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void executeUnparameterizedStatement( UnparameterizedStatement unparameterizedStatement, StreamObserver<StatementStatus> responseObserver ) {
        ProtoInterfaceClient client = getClient();
        UnparameterizedInterfaceStatement statement = statementManager.createUnparameterizedStatement( client, unparameterizedStatement );
        responseObserver.onNext( ProtoUtils.createStatus( statement ) );
        StatementResult result = statement.execute();
        responseObserver.onNext( ProtoUtils.createStatus( statement, result ) );
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void executeUnparameterizedStatementBatch( UnparameterizedStatementBatch unparameterizedStatementBatch, StreamObserver<StatementBatchStatus> responseObserver ) {
        ProtoInterfaceClient client = getClient();
        ProtoInterfaceStatementBatch batch = statementManager.createUnparameterizedStatementBatch( client, unparameterizedStatementBatch.getStatementsList() );
        responseObserver.onNext( ProtoUtils.createStatementBatchStatus( batch ) );
        List<Long> updateCounts = batch.execute();
        responseObserver.onNext( ProtoUtils.createStatementBatchStatus( batch, updateCounts ) );
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void prepareIndexedStatement( PreparedStatement preparedStatement, StreamObserver<PreparedStatementSignature> responseObserver ) {
        ProtoInterfaceClient client = getClient();
        IndexedPreparedInterfaceStatement statement = statementManager.createIndexedPreparedInterfaceStatement( client, preparedStatement );
        responseObserver.onNext( ProtoUtils.createPreparedStatementSignature( statement ) );
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void executeIndexedStatement( ParameterList parameterList, StreamObserver<StatementResult> responseObserver ) {
        ProtoInterfaceClient client = getClient();
        IndexedPreparedInterfaceStatement statement = statementManager.getIndexedPreparedStatement( client, parameterList.getStatementId() );
        responseObserver.onNext( statement.execute( ProtoValueDeserializer.deserializeValueList( parameterList.getValuesList() ) ) );
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void prepareNamedStatement( PreparedStatement preparedStatement, StreamObserver<PreparedStatementSignature> responseObserver ) {
        ProtoInterfaceClient client = getClient();
        NamedPreparedInterfaceStatement statement = statementManager.createNamedPreparedInterfaceStatement( client, preparedStatement );
        responseObserver.onNext( ProtoUtils.createPreparedStatementSignature( statement ) );
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void executeNamedStatement( ParameterSet parameterSet, StreamObserver<StatementResult> responseObserver ) {
        ProtoInterfaceClient client = getClient();
        NamedPreparedInterfaceStatement statement = statementManager.getNamedPreparedStatement( client, parameterSet.getStatementId() );
        responseObserver.onNext( statement.execute( ProtoValueDeserializer.deserilaizeValueMap( parameterSet.getValuesMap() ) ) );
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void fetchResult( FetchRequest fetchRequest, StreamObserver<Frame> responseObserver ) {
        ProtoInterfaceClient client = getClient();
        ProtoInterfaceStatement statement = statementManager.getStatement( client, fetchRequest.getStatementId() );
        Frame frame;
        if ( fetchRequest.hasFetchSize() ) {
            frame = statement.fetch( fetchRequest.getOffset(), fetchRequest.getFetchSize() );
        }
        frame = statement.fetch( fetchRequest.getOffset() );
        responseObserver.onNext( frame );
        responseObserver.onCompleted();
    }


    @Override
    public void commitTransaction( CommitRequest commitRequest, StreamObserver<CommitResponse> responseStreamObserver ) {
        ProtoInterfaceClient client = getClient();
        client.commitCurrentTransaction();
        responseStreamObserver.onNext( CommitResponse.newBuilder().build() );
        responseStreamObserver.onCompleted();
    }


    @Override
    public void rollbackTransaction( RollbackRequest rollbackRequest, StreamObserver<RollbackResponse> responseStreamObserver ) {
        ProtoInterfaceClient client = getClient();
        client.rollbackCurrentTransaction();
        responseStreamObserver.onNext( RollbackResponse.newBuilder().build() );
        responseStreamObserver.onCompleted();
    }


    @Override
    public void closeStatement( CloseStatementRequest closeStatementRequest, StreamObserver<CloseStatementResponse> responseObserver ) {
        ProtoInterfaceClient client = getClient();
        statementManager.closeStatementOrBatch( client, closeStatementRequest.getStatementId() );
        responseObserver.onNext( CloseStatementResponse.newBuilder().build() );
    }


    private ProtoInterfaceClient getClient() throws ProtoInterfaceServiceException {
        return clientManager.getClient( ClientMetaInterceptor.CLIENT.get() );
    }


    private boolean checkApiVersion( ConnectionRequest connectionRequest ) {
        return connectionRequest.getMajorApiVersion() == majorApiVersion;
    }

}
