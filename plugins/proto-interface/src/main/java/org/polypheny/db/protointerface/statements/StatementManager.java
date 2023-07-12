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

package org.polypheny.db.protointerface.statements;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.PIClient;
import org.polypheny.db.protointerface.PIStatementProperties;
import org.polypheny.db.protointerface.ProtoInterfaceServiceException;
import org.polypheny.db.protointerface.proto.PreparedStatement;
import org.polypheny.db.protointerface.proto.UnparameterizedStatement;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class StatementManager {

    private final AtomicInteger statementIdGenerator;
    private Set<String> supportedLanguages;
    private ConcurrentHashMap<String, PIStatement> openStatments;
    private ConcurrentHashMap<String, PIStatementBatch> openUnparameterizedBatches;


    public StatementManager() {
        statementIdGenerator = new AtomicInteger();
        openStatments = new ConcurrentHashMap<>();
        openUnparameterizedBatches = new ConcurrentHashMap<>();
        supportedLanguages = new HashSet<>();
        updateSupportedLanguages();
    }


    public void updateSupportedLanguages() {
        supportedLanguages = LanguageManager.getLanguages()
                .stream()
                .map(QueryLanguage::getSerializedName)
                .collect(Collectors.toSet());
    }


    public Set<String> getSupportedLanguages() {
        return supportedLanguages;
    }


    public synchronized PIUnparameterizedStatement createUnparameterizedStatement(PIClient protoInterfaceClient, UnparameterizedStatement statement) {
        if (log.isTraceEnabled()) {
            log.trace("createStatement( Connection {} )", protoInterfaceClient);
        }
        String languageName = statement.getStatementLanguageName();
        if (!isSupportedLanguage(languageName)) {
            throw new ProtoInterfaceServiceException("Language " + languageName + " not supported.");
        }
        final int statementId = statementIdGenerator.getAndIncrement();
        final String statementKey = getId(protoInterfaceClient.getClientUUID(), statementId);
        final PIUnparameterizedStatement interfaceStatement = PIUnparameterizedStatement.newBuilder()
                .setStatementId(statementId)
                .setQuery(statement.getStatement())
                .setQueryLanguage(QueryLanguage.from(languageName))
                .setProtoInterfaceClient(protoInterfaceClient)
                .setProperties(getPropertiesOrDefault(protoInterfaceClient, statement))
                .build();
        openStatments.put(statementKey, interfaceStatement);
        if (log.isTraceEnabled()) {
            log.trace("created statement {}", interfaceStatement);
        }
        return interfaceStatement;
    }

    private PIStatementProperties getPropertiesOrDefault(PIClient client, UnparameterizedStatement unparameterizedStatement) {
        if (unparameterizedStatement.hasProperties()) {
            return new PIStatementProperties(unparameterizedStatement.getProperties());
        }
        return PIStatementProperties.getDefaultInstance(client.getClientProperties());
    }

    private PIStatementProperties getPropertiesOrDefault(PIClient client, PreparedStatement preparedStatement) {
        if (preparedStatement.hasProperties()) {
            return new PIStatementProperties(preparedStatement.getProperties());
        }
        return PIStatementProperties.getDefaultInstance(client.getClientProperties());
    }


    public synchronized PIUnparameterizedStatementBatch createUnparameterizedStatementBatch(PIClient protoInterfaceClient, List<UnparameterizedStatement> statements) {
        List<PIUnparameterizedStatement> PIUnparameterizedStatements = statements.stream()
                .map(s -> createUnparameterizedStatement(protoInterfaceClient, s))
                .collect(Collectors.toList());
        final int batchId = statementIdGenerator.getAndIncrement();
        final String batchKey = getId(protoInterfaceClient.getClientUUID(), batchId);
        final PIUnparameterizedStatementBatch batch = new PIUnparameterizedStatementBatch(batchId, protoInterfaceClient, PIUnparameterizedStatements);
        openUnparameterizedBatches.put(batchKey, batch);
        if (log.isTraceEnabled()) {
            log.trace("created batch {}", batch);
        }
        return batch;
    }


    public synchronized PIPreparedIndexedStatement createIndexedPreparedInterfaceStatement(PIClient protoInterfaceClient, PreparedStatement statement) {
        String languageName = statement.getStatementLanguageName();
        if (!isSupportedLanguage(languageName)) {
            throw new ProtoInterfaceServiceException("Language " + languageName + " not supported.");
        }
        final int statementId = statementIdGenerator.getAndIncrement();
        final String statementKey = getId(protoInterfaceClient.getClientUUID(), statementId);
        final PIPreparedIndexedStatement interfaceStatement = PIPreparedIndexedStatement.newBuilder()
                .setStatementId(statementId)
                .setProtoInterfaceClient(protoInterfaceClient)
                .setQuery(statement.getStatement())
                .setQueryLanguage(QueryLanguage.from(languageName))
                .setProperties(getPropertiesOrDefault(protoInterfaceClient, statement))
                .build();
        openStatments.put(statementKey, interfaceStatement);
        if (log.isTraceEnabled()) {
            log.trace("created named prepared statement {}", interfaceStatement);
        }
        return interfaceStatement;
    }


    public synchronized PIPreparedNamedStatement createNamedPreparedInterfaceStatement(PIClient protoInterfaceClient, PreparedStatement statement) {
        String languageName = statement.getStatementLanguageName();
        if (!isSupportedLanguage(languageName)) {
            throw new ProtoInterfaceServiceException("Language " + languageName + " not supported.");
        }
        final int statementId = statementIdGenerator.getAndIncrement();
        final String statementKey = getId(protoInterfaceClient.getClientUUID(), statementId);
        final PIPreparedNamedStatement interfaceStatement = PIPreparedNamedStatement.newBuilder()
                .setStatementId(statementId)
                .setQuery(statement.getStatement())
                .setQueryLanguage(QueryLanguage.from(statement.getStatementLanguageName()))
                .setProperties(getPropertiesOrDefault(protoInterfaceClient, statement))
                .setProtoInterfaceClient(protoInterfaceClient)
                .build();
        openStatments.put(statementKey, interfaceStatement);
        if (log.isTraceEnabled()) {
            log.trace("created named prepared statement {}", interfaceStatement);
        }
        return interfaceStatement;
    }


    public void closeBatch(PIClient client, PIStatementBatch toClose) {
        toClose.getStatements().forEach(s -> closeStatementOrBatch(s.getProtoInterfaceClient(), s.getStatementId()));
    }


    private void closeStatement(PIClient client, int statementId) {
        String statementKey = getId(client.getClientUUID(), statementId);
        closeStatement(statementKey);
    }


    private void closeStatement(String statementKey) {
        PIStatement statementToClose = openStatments.remove(statementKey);
        if (statementToClose == null) {
            return;
        }
        // TODO: implement closing of statements
    }


    public void closeStatementOrBatch(PIClient client, int statementId) {
        if (client == null) {
            throw new RuntimeException("CLIENT NULL");
        }
        String statementKey = getId(client.getClientUUID(), statementId);
        PIStatementBatch batchToClose = openUnparameterizedBatches.remove(statementKey);
        if (batchToClose != null) {
            closeBatch(client, batchToClose);
            return;
        }
        closeStatement(statementKey);
    }


    public PIStatement getStatement(PIClient client, int statementId) {
        String statementKey = getId(client.getClientUUID(), statementId);
        PIStatement statement = openStatments.get(statementKey);
        if (statement == null) {
            throw new ProtoInterfaceServiceException("A statement with id " + statementId + " does not exist for that client");
        }
        return statement;
    }


    public PIPreparedNamedStatement getNamedPreparedStatement(PIClient client, int statementId) {
        String statementKey = getId(client.getClientUUID(), statementId);
        PIStatement statement = openStatments.get(statementKey);
        if (statement == null) {
            throw new ProtoInterfaceServiceException("A statement with id " + statementId + " does not exist for that client");
        }
        if (!(statement instanceof PIPreparedNamedStatement)) {
            throw new ProtoInterfaceServiceException("A prepared statement with id " + statementId + " does not exist for that client");
        }
        return (PIPreparedNamedStatement) statement;
    }


    public PIPreparedIndexedStatement getIndexedPreparedStatement(PIClient client, int statementId) {
        String statementKey = getId(client.getClientUUID(), statementId);
        PIStatement statement = openStatments.get(statementKey);
        if (statement == null) {
            throw new ProtoInterfaceServiceException("A statement with id " + statementId + " does not exist for that client");
        }
        if (!(statement instanceof PIPreparedIndexedStatement)) {
            throw new ProtoInterfaceServiceException("A prepared statement with id " + statementId + " does not exist for that client");
        }
        return (PIPreparedIndexedStatement) statement;
    }


    public boolean isSupportedLanguage(String statementLanguageName) {
        return getSupportedLanguages().contains(statementLanguageName);
    }


    private String getId(String clientUUID, int statementId) {
        return clientUUID + "::" + statementId;
    }

}
