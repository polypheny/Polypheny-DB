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
import org.polypheny.db.protointerface.proto.CloseStatementRequest;
import org.polypheny.db.protointerface.proto.CloseStatementResponse;
import org.polypheny.db.protointerface.proto.CommitRequest;
import org.polypheny.db.protointerface.proto.CommitResponse;
import org.polypheny.db.protointerface.proto.ConnectionReply;
import org.polypheny.db.protointerface.proto.ConnectionRequest;
import org.polypheny.db.protointerface.proto.FetchRequest;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.LanguageRequest;
import org.polypheny.db.protointerface.proto.LanguageResponse;
import org.polypheny.db.protointerface.proto.ProtoInterfaceGrpc;
import org.polypheny.db.protointerface.proto.RollbackRequest;
import org.polypheny.db.protointerface.proto.RollbackResponse;
import org.polypheny.db.protointerface.proto.StatementBatchStatus;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.proto.StatementStatus;
import org.polypheny.db.protointerface.proto.UnparameterizedStatementBatch;
import org.polypheny.db.protointerface.statements.ProtoInterfaceStatement;
import org.polypheny.db.protointerface.statements.ProtoInterfaceStatementBatch;
import org.polypheny.db.protointerface.statements.UnparameterizedInterfaceStatement;
import org.polypheny.db.protointerface.utils.ProtoUtils;

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
    public void getSupportedLanguages( LanguageRequest languageRequest, StreamObserver<LanguageResponse> responseObserver ) {
        LanguageResponse supportedLanguages = LanguageResponse.newBuilder()
                .addAllLanguageNames( new LinkedList<>() )
                .build();
        responseObserver.onNext( supportedLanguages );
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void executeUnparameterizedStatement( org.polypheny.db.protointerface.proto.UnparameterizedStatement unparameterizedStatement, StreamObserver<StatementStatus> responseObserver ) {
        ProtoInterfaceClient client = getClient();
        String languageName = unparameterizedStatement.getStatementLanguageName();
        if ( !statementManager.isSupportedLanguage( languageName ) ) {
            throw new ProtoInterfaceServiceException( "Language " + languageName + " not supported." );
        }
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
        responseObserver.onNext( ProtoUtils.createStatementBatchStatus( batch) );
        List<Long> updateCounts = batch.execute();
        responseObserver.onNext( ProtoUtils.createStatementBatchStatus( batch, updateCounts ) );
        responseObserver.onCompleted();
    }

    @SneakyThrows
    @Override
    public void fetchResult( FetchRequest fetchRequest, StreamObserver<Frame> responseObserver ) {
        ProtoInterfaceClient client = getClient();
        ProtoInterfaceStatement statement = statementManager.getStatement( client, fetchRequest.getStatementId() );
        Frame frame;
        if (fetchRequest.hasFetchSize() ) {
             frame = statement.fetch( fetchRequest.getOffset() , fetchRequest.getFetchSize());
        }
        frame = statement.fetch( fetchRequest.getOffset());
        responseObserver.onNext( frame );
        responseObserver.onCompleted();
    }

    @Override
    public void commitTransaction( CommitRequest commitRequest, StreamObserver<CommitResponse> responseStreamObserver) {
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

    private ProtoInterfaceClient getClient() throws ProtoInterfaceServiceException{
        return clientManager.getClient(ClientMetaInterceptor.CLIENT.get());
    }


    private boolean checkApiVersion( ConnectionRequest connectionRequest ) {
        return connectionRequest.getMajorApiVersion() == majorApiVersion;
    }

}
