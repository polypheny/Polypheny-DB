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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.polypheny.db.protointerface.proto.ParameterizedStatement;
import org.polypheny.db.protointerface.proto.ParameterizedStatementBatch;
import org.polypheny.db.protointerface.statements.OldPIStatement;
import org.polypheny.db.type.entity.PolyValue;

@Slf4j
public class StatementManager {

    private final AtomicInteger statementIdGenerator;
    private Set<String> supportedLanguages;
    private ConcurrentHashMap<String, OldPIStatement> openStatments;


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
                .collect( Collectors.toSet());
    }


    public Set<String> getSupportedLanguages() {
        return supportedLanguages;
    }


    public synchronized OldPIStatement createStatement( ProtoInterfaceClient protoInterfaceClient, QueryLanguage queryLanguage, String query ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "createStatement( Connection {} )", protoInterfaceClient );
        }
        final int statementId = statementIdGenerator.getAndIncrement();
        final String statementKey = getStatementKey( protoInterfaceClient.getClientUUID(), statementId );
        final OldPIStatement protoInterfaceStatement = new OldPIStatement( statementId, protoInterfaceClient, queryLanguage, query );
        openStatments.put( statementKey, protoInterfaceStatement );
        if ( log.isTraceEnabled() ) {
            log.trace( "created statement {}", protoInterfaceStatement );
        }
        return protoInterfaceStatement;
    }


    public ProtoInterfaceStatementBatch createStatementBatch( ParameterizedStatementBatch pStatementBatch, ProtoInterfaceClient protoInterfaceClient ) {
        List<ParameterizedStatement> statements = pStatementBatch.getParameterizedStatementList();
        ProtoInterfaceStatementBatch statementBatch = new ProtoInterfaceStatementBatch( pStatementBatch.getStatementPropertiesMap() );

        QueryLanguage queryLanguage;
        OldPIStatement protoInterfaceStatement;
        List<Map<String, PolyValue>> valuesMaps;

        for ( ParameterizedStatement statement : statements ) {
            // check if valid language
            if ( !isSupportedLanguage( statement.getStatementLanguageName() ) ) {
                throw new ProtoInterfaceServiceException( "Language " + statement.getStatementLanguageName() + " not supported." );
            }
            queryLanguage = QueryLanguage.from( statement.getStatementLanguageName() );
            protoInterfaceStatement = createStatement( protoInterfaceClient, queryLanguage, statement.getStatement() );
            valuesMaps = PolyValueDeserializer.deserializeValueMapBatch( statement.getValueMapBatch() );
            protoInterfaceStatement.addValues( valuesMaps );
            statementBatch.addStatement( protoInterfaceStatement );
        }
        return statementBatch;
    }


    private boolean isSupportedLanguage( String statementLanguageName ) {
        return getSupportedLanguages().contains(statementLanguageName);
    }


    private String getStatementKey( String clientUUID, int statementId ) {
        return clientUUID + "::" + statementId;
    }

}
