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

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.reflection.v1alpha.ErrorResponse;
import io.grpc.stub.StreamObserver;
import java.util.LinkedList;
import lombok.SneakyThrows;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.proto.CloseStatementRequest;
import org.polypheny.db.protointerface.proto.CloseStatementResponse;
import org.polypheny.db.protointerface.proto.ConnectionReply;
import org.polypheny.db.protointerface.proto.ConnectionRequest;
import org.polypheny.db.protointerface.proto.LanguageRequest;
import org.polypheny.db.protointerface.proto.ProtoInterfaceGrpc;
import org.polypheny.db.protointerface.proto.QueryResult;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.proto.StatementStatus;
import org.polypheny.db.protointerface.proto.SupportedLanguages;
import org.polypheny.db.protointerface.proto.UnparameterizedStatement;
import org.polypheny.db.protointerface.statements.UnparameterizedInterfaceStatement;
import org.polypheny.db.protointerface.utils.ProtoUtils;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;

public class ProtoInterfaceService extends ProtoInterfaceGrpc.ProtoInterfaceImplBase {

    private static final int majorApiVersion = 2;
    private static final int minorApiVersion = 0;
    private ClientManager clientManager;
    private StatementManager statementManager;


    public ProtoInterfaceService( ClientManager clientManager ) {
        this.clientManager = clientManager;
        this.statementManager = new StatementManager();
        PolyValue value = new PolyString( "hi! i'm new ;)" );
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
    public void getSupportedLanguages( LanguageRequest languageRequest, StreamObserver<SupportedLanguages> responseObserver ) {
        SupportedLanguages supportedLanguages = SupportedLanguages.newBuilder()
                .addAllLanguageNames( new LinkedList<>() )
                .build();
        responseObserver.onNext( supportedLanguages );
        responseObserver.onCompleted();
    }


    @SneakyThrows
    @Override
    public void executeUnparameterizedStatement( UnparameterizedStatement unparameterizedStatement, StreamObserver<StatementStatus> responseObserver ) {
        ProtoInterfaceClient client = ClientMetaInterceptor.CLIENT.get();
        String languageName = unparameterizedStatement.getStatementLanguageName();
        if ( !statementManager.isSupportedLanguage( languageName ) ) {
            throw new ProtoInterfaceServiceException( "Language " + languageName + " not supported." );
        }
        if ( unparameterizedStatement.hasProperties() ) {
            client.setStatementProperties( ProtoUtils.unwrapStringMap( unparameterizedStatement.getProperties() ) );
        }
        UnparameterizedInterfaceStatement statement = statementManager.createUnparameterizedStatement( client, QueryLanguage.from( languageName ), unparameterizedStatement.getStatement() );
        Thread statusThread = new Thread(new StatementStatusProvider( unparameterizedStatement.getStatusUpdateInterval(), statement, responseObserver));
        statusThread.start();
        StatementResult result = statement.execute();
        statusThread.interrupt();
        responseObserver.onNext( ProtoUtils.createStatus( statement, result ) );
        responseObserver.onCompleted();
    }

    @Override
    public void closeStatement( CloseStatementRequest closeStatementRequest, StreamObserver<CloseStatementResponse> responseObserver ) {
        ProtoInterfaceClient client = ClientMetaInterceptor.CLIENT.get();
        statementManager.closeStatement( client, closeStatementRequest.getStatementId() );
        responseObserver.onNext( CloseStatementResponse.newBuilder().build() );
    }


    private boolean checkApiVersion( ConnectionRequest connectionRequest ) {
        return connectionRequest.getMajorApiVersion() == majorApiVersion;
    }

}
