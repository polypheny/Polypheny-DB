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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.proto.ParameterizedStatement;
import org.polypheny.db.protointerface.proto.PreparedStatement;
import org.polypheny.db.protointerface.proto.UnparameterizedStatement;
import org.polypheny.db.protointerface.statements.ParameterizedInterfaceStatement;
import org.polypheny.db.protointerface.statements.ProtoInterfaceStatement;
import org.polypheny.db.protointerface.statements.ProtoInterfaceStatementBatch;
import org.polypheny.db.protointerface.statements.UnparameterizedInterfaceStatement;
import org.polypheny.db.protointerface.statements.UnparameterizedInterfaceStatementBatch;

@Slf4j
public class StatementManager {

    private final AtomicInteger statementIdGenerator;
    private Set<String> supportedLanguages;
    private ConcurrentHashMap<String, ProtoInterfaceStatement> openStatments;
    private ConcurrentHashMap<String, ProtoInterfaceStatementBatch> openBatches;


    public StatementManager() {
        statementIdGenerator = new AtomicInteger();
        openStatments = new ConcurrentHashMap<>();
        supportedLanguages = new HashSet<>();
        updateSupportedLanguages();
    }


    public void updateSupportedLanguages() {
        supportedLanguages = LanguageManager.getLanguages()
                .stream()
                .map( QueryLanguage::getSerializedName )
                .collect( Collectors.toSet() );
    }


    public Set<String> getSupportedLanguages() {
        return supportedLanguages;
    }


    public UnparameterizedInterfaceStatement createUnparameterizedStatement( ProtoInterfaceClient protoInterfaceClient, UnparameterizedStatement statement ) {
        String languageName = statement.getStatementLanguageName();
        if ( !isSupportedLanguage( languageName ) ) {
            throw new ProtoInterfaceServiceException( "Language " + languageName + " not supported." );
        }
        return createUnparameterizedStatement( protoInterfaceClient, QueryLanguage.from( languageName ), statement.getStatement() );
    }


    private synchronized UnparameterizedInterfaceStatement createUnparameterizedStatement( ProtoInterfaceClient protoInterfaceClient, QueryLanguage queryLanguage, String query ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "createStatement( Connection {} )", protoInterfaceClient );
        }
        final int statementId = statementIdGenerator.getAndIncrement();
        final String statementKey = getId( protoInterfaceClient.getClientUUID(), statementId );
        final UnparameterizedInterfaceStatement statement = new UnparameterizedInterfaceStatement( statementId, protoInterfaceClient, queryLanguage, query );
        openStatments.put( statementKey, statement );
        if ( log.isTraceEnabled() ) {
            log.trace( "created statement {}", statement );
        }
        return statement;
    }


    public synchronized UnparameterizedInterfaceStatementBatch createUnparameterizedStatementBatch( ProtoInterfaceClient protoInterfaceClient, List<UnparameterizedStatement> statements ) {
        List<UnparameterizedInterfaceStatement> unparameterizedInterfaceStatements = statements.stream()
                .map( s -> createUnparameterizedStatement( protoInterfaceClient, s ) )
                .collect( Collectors.toList() );
        final int batchId = statementIdGenerator.getAndIncrement();
        final String batchKey = getId( protoInterfaceClient.getClientUUID(), batchId );
        final UnparameterizedInterfaceStatementBatch batch = new UnparameterizedInterfaceStatementBatch( batchId, protoInterfaceClient, unparameterizedInterfaceStatements );
        openBatches.put( batchKey, batch );
        if ( log.isTraceEnabled() ) {
            log.trace( "created batch {}", batch );
        }
        return batch;
    }


    public ParameterizedInterfaceStatement createParameterizedStatement( ProtoInterfaceClient protoInterfaceClient, PreparedStatement statement ) {
        String languageName = statement.getStatementLanguageName();
        if ( !isSupportedLanguage( languageName ) ) {
            throw new ProtoInterfaceServiceException( "Language " + languageName + " not supported." );
        }
        return createParameterizedStatement( protoInterfaceClient, QueryLanguage.from( languageName ), statement.getStatement() );
    }


    public ParameterizedInterfaceStatement createParameterizedStatement( ProtoInterfaceClient protoInterfaceClient, ParameterizedStatement statement ) {
        String languageName = statement.getStatementLanguageName();
        if ( !isSupportedLanguage( languageName ) ) {
            throw new ProtoInterfaceServiceException( "Language " + languageName + " not supported." );
        }
        return createParameterizedStatement( protoInterfaceClient, QueryLanguage.from( languageName ), statement.getStatement() );
    }


    private synchronized ParameterizedInterfaceStatement createParameterizedStatement( ProtoInterfaceClient protoInterfaceClient, QueryLanguage queryLanguage, String query ) {
        final int statementId = statementIdGenerator.getAndIncrement();
        final String statementKey = getId( protoInterfaceClient.getClientUUID(), statementId );
        final ParameterizedInterfaceStatement statement = new ParameterizedInterfaceStatement( statementId, protoInterfaceClient, queryLanguage, query );
        openStatments.put( statementKey, statement );
        if ( log.isTraceEnabled() ) {
            log.trace( "created prepared statement {}", statement );
        }
        return statement;
    }


    public void closeBatch( ProtoInterfaceClient client, ProtoInterfaceStatementBatch toClose ) {
        toClose.getStatements().forEach( s -> closeStatementOrBatch( client, s.getStatementId() ) );
    }


    private void closeStatement( ProtoInterfaceClient client, int statementId ) {
        String statementKey = getId( client.getClientUUID(), statementId );
        closeStatement( statementKey );
    }


    private void closeStatement( String statementKey ) {
        ProtoInterfaceStatement statementToClose = openStatments.remove( statementKey );
        if ( statementToClose == null ) {
            return;
        }
        // TODO: implement closing of statements
    }


    public void closeStatementOrBatch( ProtoInterfaceClient client, int statementId ) {
        String statementKey = getId( client.getClientUUID(), statementId );
        ProtoInterfaceStatementBatch batchToClose = openBatches.remove( statementKey );
        if ( batchToClose != null ) {
            closeBatch( client, batchToClose );
            return;
        }
        closeStatement( statementKey );
    }


    public ProtoInterfaceStatement getStatement( ProtoInterfaceClient client, int statementId ) {
        String statementKey = getId( client.getClientUUID(), statementId );
        ProtoInterfaceStatement statement = openStatments.get( statementKey );
        if ( statement == null ) {
            throw new ProtoInterfaceServiceException( "A statement with id " + statementId + " does not exist for that client" );
        }
        return statement;
    }

    public ParameterizedInterfaceStatement getParameterizedStatement(ProtoInterfaceClient client, int statementId) {
        String statementKey = getId( client.getClientUUID(), statementId );
        ProtoInterfaceStatement statement = openStatments.get( statementKey );
        if ( statement == null ) {
            throw new ProtoInterfaceServiceException( "A statement with id " + statementId + " does not exist for that client" );
        }
        if (!(statement instanceof ParameterizedInterfaceStatement)) {
            throw new ProtoInterfaceServiceException( "A prepared statement with id " + statementId + " does not exist for that client" );
        }
        return (ParameterizedInterfaceStatement)statement;
    }


    public boolean isSupportedLanguage( String statementLanguageName ) {
        return getSupportedLanguages().contains( statementLanguageName );
    }


    private String getId( String clientUUID, int statementId ) {
        return clientUUID + "::" + statementId;
    }

}
