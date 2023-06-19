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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.statements.ProtoInterfaceStatement;
import org.polypheny.db.protointerface.statements.UnparameterizedInterfaceStatement;
import org.polypheny.db.protointerface.utils.ProtoUtils;

@Slf4j
public class StatementManager {

    private final AtomicInteger statementIdGenerator;
    private Set<String> supportedLanguages;
    private ConcurrentHashMap<String, ProtoInterfaceStatement> openStatments;


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


    public synchronized UnparameterizedInterfaceStatement createUnparameterizedStatement( ProtoInterfaceClient protoInterfaceClient, QueryLanguage queryLanguage, String query ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "createStatement( Connection {} )", protoInterfaceClient );
        }
        final int statementId = statementIdGenerator.getAndIncrement();
        final String statementKey = getStatementKey( protoInterfaceClient.getClientUUID(), statementId );
        final UnparameterizedInterfaceStatement unparameterizedInterfaceStatement = new UnparameterizedInterfaceStatement( statementId, protoInterfaceClient, queryLanguage, query );
        openStatments.put( statementKey, unparameterizedInterfaceStatement );
        if ( log.isTraceEnabled() ) {
            log.trace( "created statement {}", unparameterizedInterfaceStatement );
        }
        return unparameterizedInterfaceStatement;
    }

    public void closeStatement(ProtoInterfaceClient client, int statementId) {
        String statementKey = getStatementKey( client.getClientUUID(), statementId );
        ProtoInterfaceStatement toClose = openStatments.remove(statementKey);
        if (toClose == null) {
            return;
        }
        // TODO: implement closing of statements
    }

    public ProtoInterfaceStatement getStatement(ProtoInterfaceClient client, int statementId) {
        String statementKey = getStatementKey( client.getClientUUID(), statementId );
        ProtoInterfaceStatement statement = openStatments.get(statementKey);
        if (statement == null) {
            throw new ProtoInterfaceServiceException( "A statement with id " + statementId + " does not exist for that client" );
        }
        return statement;
    }

    public boolean isSupportedLanguage( String statementLanguageName ) {
        return getSupportedLanguages().contains(statementLanguageName);
    }


    private String getStatementKey( String clientUUID, int statementId ) {
        return clientUUID + "::" + statementId;
    }

}
