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
import java.util.stream.Collectors;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.proto.*;
import org.polypheny.db.protointerface.statements.UnparameterizedInterfaceStatement;
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
        PolyValue value = new PolyString("hi! i'm new ;)");
    }


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
        try {
            clientManager.registerConnection( connectionRequest );

        } catch ( TransactionException | AuthenticationException e ) {
            throw new RuntimeException( e );
        }
        responseObserver.onNext( connectionReply );
        responseObserver.onCompleted();
    }


    @Override
    public void getSupportedLanguages( LanguageRequest languageRequest, StreamObserver<SupportedLanguages> responseObserver ) {
        SupportedLanguages supportedLanguages = SupportedLanguages.newBuilder()
                .addAllLanguageNames( new LinkedList<>())
                .build();
        responseObserver.onNext( supportedLanguages );
        responseObserver.onCompleted();
    }


    @Override
    public void executeUnparameterizedStatement( UnparameterizedStatement unparameterizedStatement, StreamObserver<QueryResult> responseObserver ) {
        ProtoInterfaceClient client = ClientMetaInterceptor.CLIENT.get();
        String languageName = unparameterizedStatement.getStatementLanguageName();
        if (!statementManager.getSupportedLanguages().contains( languageName )) {
            throw new ProtoInterfaceServiceException( "Language " + languageName + " not supported." );
        }
        UnparameterizedInterfaceStatement statement = statementManager.createUnparameterizedStatement( client, QueryLanguage.from( languageName ), unparameterizedStatement.getStatement() );
        try {
            QueryResult result = statement.execute();
            responseObserver.onNext( result );
            responseObserver.onCompleted();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException( e.getMessage() );
        }
    }


    private boolean checkApiVersion( ConnectionRequest connectionRequest ) {
        return connectionRequest.getMajorApiVersion() == majorApiVersion;
    }

}
