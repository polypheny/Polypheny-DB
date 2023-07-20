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
import lombok.SneakyThrows;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.proto.*;
import org.polypheny.db.protointerface.statements.*;
import org.polypheny.db.protointerface.utils.ProtoUtils;
import org.polypheny.db.protointerface.utils.ProtoValueDeserializer;
import org.polypheny.db.sql.language.SqlJdbcFunctionCall;
import org.polypheny.db.type.entity.PolyValue;

import java.util.LinkedList;
import java.util.List;

public class PIService extends ProtoInterfaceGrpc.ProtoInterfaceImplBase {

    private static final int majorApiVersion = 2;
    private static final int minorApiVersion = 0;
    private ClientManager clientManager;

    public PIService(ClientManager clientManager) {
        this.clientManager = clientManager;
    }


    @SneakyThrows
    @Override
    public void connect(ConnectionRequest connectionRequest, StreamObserver<ConnectionReply> responseObserver) {
        ConnectionReply.Builder responseBuilder = ConnectionReply.newBuilder()
                .setMajorApiVersion(majorApiVersion)
                .setMinorApiVersion(minorApiVersion);
        boolean isCompatible = checkApiVersion(connectionRequest);
        responseBuilder.setIsCompatible(isCompatible);
        ConnectionReply connectionReply = responseBuilder.build();
        // reject incompatible client
        if (!isCompatible) {
            responseObserver.onNext(connectionReply);
            responseObserver.onCompleted();
            return;
        }
        clientManager.registerConnection(connectionRequest);
        responseObserver.onNext(connectionReply);
        responseObserver.onCompleted();
    }

    @SneakyThrows
    @Override
    public void disconnect(DisconnectionRequest disconnectionRequest, StreamObserver<DisconnectionResponse> responseObserver) {
        PIClient client = getClient();
        clientManager.unregisterConnection(client);
        responseObserver.onNext(DisconnectionResponse.newBuilder().build());
        responseObserver.onCompleted();
    }


    @Override
    public void checkConnection(ConnectionCheckRequest connectionCheckRequest, StreamObserver<ConnectionCheckResponse> responseObserver) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext(ConnectionCheckResponse.newBuilder().build());
        responseObserver.onCompleted();
    }


    @Override
    public void getDbmsVersion(DbmsVersionRequest dbmsVersionRequest, StreamObserver<DbmsVersionResponse> responseObserver) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext(DbMetaRetriever.getDbmsVersion());
        responseObserver.onCompleted();
    }


    @Override
    public void getSupportedLanguages(LanguageRequest languageRequest, StreamObserver<LanguageResponse> responseObserver) {
        /* called as client auth check */
        getClient();
        LanguageResponse supportedLanguages = LanguageResponse.newBuilder()
                .addAllLanguageNames(new LinkedList<>())
                .build();
        responseObserver.onNext(supportedLanguages);
        responseObserver.onCompleted();
    }

    private MetaStringResponse buildMetaStringResponse(String string) {
        return MetaStringResponse.newBuilder()
                .setString(string)
                .build();
    }

    @Override
    public void getDatabases(DatabasesRequest request, StreamObserver<DatabasesResponse> responseObserver) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext(DbMetaRetriever.getDatabases());
        responseObserver.onCompleted();
    }

    @Override
    public void getTableTypes(TableTypesRequest request, StreamObserver<TableTypesResponse> responseObserver) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext(DbMetaRetriever.getTableTypes());
        responseObserver.onCompleted();
    }

    @Override
    public void getTypes(TypesRequest request, StreamObserver<TypesResponse> responseStreamObserver) {
        /* called as client auth check */
        getClient();
        responseStreamObserver.onNext(DbMetaRetriever.getTypes());
        responseStreamObserver.onCompleted();
    }

    @Override
    public void searchNamespaces(NamespacesRequest request, StreamObserver<NamespacesResponse> responseObserver) {
        /* called as client auth check */
        getClient();
        String namespacePattern = request.hasNamespacePattern() ? request.getNamespacePattern() : null;
        String namespaceType = request.hasNamespaceType() ? request.getNamespaceType() : null;
        responseObserver.onNext(DbMetaRetriever.searchNamespaces(namespacePattern, namespaceType));
        responseObserver.onCompleted();
    }

    @Override
    public void getNamespace(NamespaceRequest request, StreamObserver<Namespace> responseObserver) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext(DbMetaRetriever.getNamespace(request.getNamespaceName()));
        responseObserver.onCompleted();
    }

    @Override
    public void searchEntities(EntitiesRequest request, StreamObserver<EntitiesResponse> responseObserver) {
        /* called as client auth check */
        getClient();
        String entityPattern = request.hasEntityPattern() ? request.getEntityPattern() : null;
        responseObserver.onNext(DbMetaRetriever.searchEntities(request.getNamespaceName(), entityPattern));
        responseObserver.onCompleted();
    }

    @Override
    public void getSqlStringFunctions(SqlStringFunctionsRequest request, StreamObserver<MetaStringResponse> responseObserver) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext(buildMetaStringResponse(SqlJdbcFunctionCall.getStringFunctions()));
        responseObserver.onCompleted();
    }

    @Override
    public void getSqlSystemFunctions(SqlSystemFunctionsRequest request, StreamObserver<MetaStringResponse> responseObserver) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext(buildMetaStringResponse(SqlJdbcFunctionCall.getSystemFunctions()));
        responseObserver.onCompleted();
    }

    @Override
    public void getSqlTimeDateFunctions(SqlTimeDateFunctionsRequest request, StreamObserver<MetaStringResponse> responseObserver) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext(buildMetaStringResponse(SqlJdbcFunctionCall.getTimeDateFunctions()));
        responseObserver.onCompleted();
    }

    @Override
    public void getSqlNumericFunctions(SqlNumericFunctionsRequest request, StreamObserver<MetaStringResponse> responseObserver) {
        /* called as client auth check */
        getClient();
        responseObserver.onNext(buildMetaStringResponse(SqlJdbcFunctionCall.getNumericFunctions()));
        responseObserver.onCompleted();
    }

    @Override
    public void getSqlKeywords(SqlKeywordsRequest request, StreamObserver<MetaStringResponse> responseObserver) {
        /* called as client auth check */
        getClient();
        // TODO actually return keywords
        responseObserver.onNext(buildMetaStringResponse(""));
        responseObserver.onCompleted();
    }

    @Override
    public void searchProcedures(ProceduresRequest request, StreamObserver<ProceduresResponse> responeObserver) {
        /* called as client auth check */
        getClient();
        String procedurePattern = request.hasProcedureNamePattern() ? request.getProcedureNamePattern() : null;
        responeObserver.onNext(DbMetaRetriever.getProcedures(request.getLanguage(), procedurePattern));
        responeObserver.onCompleted();
    }

    @Override
    public void searchFunctions(FunctionsRequest request, StreamObserver<FunctionsResponse> responseObserver) {
        /* called as client auth check */
        getClient();
        QueryLanguage queryLanguage = QueryLanguage.from(request.getQueryLanguage());
        FunctionCategory functionCategory = FunctionCategory.valueOf(request.getFunctionCategory());
        responseObserver.onNext(DbMetaRetriever.getFunctions(queryLanguage, functionCategory));
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void executeUnparameterizedStatement(UnparameterizedStatement unparameterizedStatement, StreamObserver<StatementStatus> responseObserver) {
        PIClient client = getClient();
        PIUnparameterizedStatement statement = client.getStatementManager().createUnparameterizedStatement(unparameterizedStatement);
        responseObserver.onNext(ProtoUtils.createStatus(statement));
        StatementResult result = statement.execute();
        responseObserver.onNext(ProtoUtils.createStatus(statement, result));
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void executeUnparameterizedStatementBatch(UnparameterizedStatementBatch unparameterizedStatementBatch, StreamObserver<StatementBatchStatus> responseObserver) {
        PIClient client = getClient();
        PIUnparameterizedStatementBatch batch = client.getStatementManager().createUnparameterizedStatementBatch(unparameterizedStatementBatch.getStatementsList());
        responseObserver.onNext(ProtoUtils.createStatementBatchStatus(batch.getBatchId()));
        List<Long> updateCounts = batch.executeBatch();
        responseObserver.onNext(ProtoUtils.createStatementBatchStatus(batch.getBatchId(), updateCounts));
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void prepareIndexedStatement(PreparedStatement preparedStatement, StreamObserver<PreparedStatementSignature> responseObserver) {
        PIClient client = getClient();
        PIPreparedIndexedStatement statement = client.getStatementManager().createIndexedPreparedInterfaceStatement(preparedStatement);
        responseObserver.onNext(ProtoUtils.createPreparedStatementSignature(statement));
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void executeIndexedStatement(ParameterList parameterList, StreamObserver<StatementResult> responseObserver) {
        PIClient client = getClient();
        PIPreparedIndexedStatement statement = client.getStatementManager().getIndexedPreparedStatement(parameterList.getStatementId());
        responseObserver.onNext(statement.execute(ProtoValueDeserializer.deserializeParameterList(parameterList.getParametersList())));
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void executeIndexedStatementBatch(IndexedParameterBatch indexedParameterBatch, StreamObserver<StatementBatchStatus> resultObserver) {
        PIClient client = getClient();
        PIPreparedIndexedStatement statement = client.getStatementManager().getIndexedPreparedStatement(indexedParameterBatch.getStatementId());
        List<List<PolyValue>> valuesList = ProtoValueDeserializer.deserializeParameterLists(indexedParameterBatch.getParameterListsList());
        List<Long> updateCounts = statement.executeBatch(valuesList);
        resultObserver.onNext(ProtoUtils.createStatementBatchStatus(statement.getId(), updateCounts));
        resultObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void prepareNamedStatement(PreparedStatement preparedStatement, StreamObserver<PreparedStatementSignature> responseObserver) {
        PIClient client = getClient();
        PIPreparedNamedStatement statement = client.getStatementManager().createNamedPreparedInterfaceStatement(preparedStatement);
        responseObserver.onNext(ProtoUtils.createPreparedStatementSignature(statement));
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void executeNamedStatement(ParameterSet parameterSet, StreamObserver<StatementResult> responseObserver) {
        PIClient client = getClient();
        PIPreparedNamedStatement statement = client.getStatementManager().getNamedPreparedStatement(parameterSet.getStatementId());
        responseObserver.onNext(statement.execute(ProtoValueDeserializer.deserilaizeValueMap(parameterSet.getParametersMap())));
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void fetchResult(FetchRequest fetchRequest, StreamObserver<Frame> responseObserver) {
        PIClient client = getClient();
        PIStatement statement = client.getStatementManager().getStatement(fetchRequest.getStatementId());
        Frame frame = statement.fetch(fetchRequest.getOffset());
        responseObserver.onNext(frame);
        responseObserver.onCompleted();
    }


    @Override
    public void commitTransaction(CommitRequest commitRequest, StreamObserver<CommitResponse> responseStreamObserver) {
        PIClient client = getClient();
        client.commitCurrentTransaction();
        responseStreamObserver.onNext(CommitResponse.newBuilder().build());
        responseStreamObserver.onCompleted();
    }


    @Override
    public void rollbackTransaction(RollbackRequest rollbackRequest, StreamObserver<RollbackResponse> responseStreamObserver) {
        PIClient client = getClient();
        client.rollbackCurrentTransaction();
        responseStreamObserver.onNext(RollbackResponse.newBuilder().build());
        responseStreamObserver.onCompleted();
    }


    @Override
    public void closeStatement(CloseStatementRequest closeStatementRequest, StreamObserver<CloseStatementResponse> responseObserver) {
        PIClient client = getClient();
        client.getStatementManager().closeStatementOrBatch(closeStatementRequest.getStatementId());
        responseObserver.onNext(CloseStatementResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateConnectionProperties(ConnectionProperties connectionProperties, StreamObserver<ConnectionPropertiesUpdateResponse> responseObserver) {
        PIClient client = getClient();
        client.setClientProperties(connectionProperties);
        responseObserver.onNext(ConnectionPropertiesUpdateResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateStatementProperties(StatementProperties statementProperties, StreamObserver<StatementPropertiesUpdateResponse> responseObserver) {
        PIClient client = getClient();
        PIStatement statement = client.getStatementManager().getStatement(statementProperties.getStatementId());
        statement.updateProperties(statementProperties);
        responseObserver.onNext(StatementPropertiesUpdateResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void getClientInfoProperties(ClientInfoPropertiesRequest request, StreamObserver<ClientInfoProperties> responseObserver) {
        PIClient client = getClient();
        org.polypheny.db.protointerface.proto.ClientInfoProperties.Builder responseBuilder = ClientInfoProperties.newBuilder();
        PIClientInfoProperties PIClientInfoProperties = client.getPIClientInfoProperties();
        PIClientInfoProperties.stringPropertyNames().forEach(s -> responseBuilder.putProperties(s, PIClientInfoProperties.getProperty(s)));
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void  setClientInfoProperties(ClientInfoProperties properties, StreamObserver<ClientInfoPropertiesResponse> reponseObserver) {
        PIClient client = getClient();
        client.getPIClientInfoProperties().putAll(properties.getPropertiesMap());
        reponseObserver.onNext(ClientInfoPropertiesResponse.newBuilder().build());
        reponseObserver.onCompleted();
    }




    private PIClient getClient() throws PIServiceException {
        return clientManager.getClient(ClientMetaInterceptor.CLIENT.get());
    }


    private boolean checkApiVersion(ConnectionRequest connectionRequest) {
        return connectionRequest.getMajorApiVersion() == majorApiVersion;
    }

}
