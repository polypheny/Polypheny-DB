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

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.languages.QueryLanguage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class StatementManager {
    private final AtomicInteger statementIdGenerator;
    ConcurrentHashMap<String, ProtoInterfaceStatement> openStatments;

    public StatementManager() {
        statementIdGenerator = new AtomicInteger();
        openStatments = new ConcurrentHashMap<>();
    }

    public synchronized ProtoInterfaceStatement createStatement(ProtoInterfaceClient protoInterfaceClient, QueryLanguage queryLanguage) {
        if (log.isTraceEnabled()) {
            log.trace("createStatement( Connection {} )", protoInterfaceClient);
        }
        final int statementId = statementIdGenerator.getAndIncrement();
        final String statementKey = getStatementKey(protoInterfaceClient.getClientUUID(), statementId);
        final ProtoInterfaceStatement protoInterfaceStatement = new ProtoInterfaceStatement(statementId, protoInterfaceClient, queryLanguage);
        openStatments.put(statementKey, protoInterfaceStatement);
        if (log.isTraceEnabled()) {
            log.trace("created statement {}", protoInterfaceStatement);
        }
        return protoInterfaceStatement;
    }

    private String getStatementKey(String clientUUID, int statementId) {
        return clientUUID + "::" + statementId;
    }

}
