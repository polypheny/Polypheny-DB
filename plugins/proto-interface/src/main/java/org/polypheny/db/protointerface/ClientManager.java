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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.protointerface.proto.ConnectionRequest;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ClientManager {
    private final String DEFAULT_USERNAME_KEY = "username";
    private final String DEFAULT_PASSWORD_KEY = "password";
    private ConcurrentHashMap<String, ProtoInterfaceClient> openConnections;
    private final Authenticator authenticator;
    private final TransactionManager transactionManager;

    public ClientManager(Authenticator authenticator, TransactionManager transactionManager) {
        this.authenticator = authenticator;
        this.transactionManager = transactionManager;
    }

    public void registerConnection(ConnectionRequest connectionRequest) throws AuthenticationException, TransactionException, ProtoInterfaceServiceException {
        if ( log.isTraceEnabled() ) {
            log.trace( "User {} tries to establish connection via proto interface.", connectionRequest.getClientUUID());
        }
        // reject already connected user
        if (isConnected(connectionRequest.getClientUUID())) {
            throw new ProtoInterfaceServiceException("user with uid " + connectionRequest.getClientUUID() + "is already connected.");
        }
        Map<String, String> properties = connectionRequest.getConnectionPropertiesMap();
        if (!credentialsPresent(properties)) {
            throw new ProtoInterfaceServiceException("No username and password given.");
        }
        ProtoInterfaceClient.Builder connectionBuilder = ProtoInterfaceClient.newBuilder();
        connectionBuilder
                .setMajorApiVersion(connectionRequest.getMajorApiVersion())
                .setMinorApiVersion(connectionRequest.getMinorApiVersion())
                .setClientUUID(connectionRequest.getClientUUID())
                .setProperties(properties)
                .setTransactionManager(transactionManager);

        final CatalogUser user = authenticateUser(properties.get(DEFAULT_USERNAME_KEY), properties.get(DEFAULT_PASSWORD_KEY));
        Transaction transaction = transactionManager.startTransaction(user, null, false, "proto-interface");
        LogicalNamespace namespace;
        if (connectionRequest.containsConnectionProperties("namespace")) {
            namespace = Catalog.getInstance().getSnapshot().getNamespace(connectionRequest.getConnectionPropertiesOrThrow("namespace"));
        } else {
            namespace = Catalog.getInstance().getSnapshot().getNamespace(Catalog.defaultNamespaceName);
        }
        assert namespace != null;
        transaction.commit();
        connectionBuilder
                .setCatalogUser(user)
                .setLogicalNamespace(namespace);
        openConnections.put(connectionRequest.getClientUUID(), connectionBuilder.build());
        if ( log.isTraceEnabled() ) {
            log.trace( "proto-interface established connection to user {}.", connectionRequest.getClientUUID());
        }
    }

    public ProtoInterfaceClient getClient(String clientUUID) {
        return openConnections.get(clientUUID);
    }

    private boolean credentialsPresent(Map<String, String> properties) {
        return properties.containsKey(DEFAULT_USERNAME_KEY) && properties.containsKey(DEFAULT_PASSWORD_KEY);
    }

    CatalogUser authenticateUser(String username, String password) throws AuthenticationException {
        return authenticator.authenticate(username, password);
    }

    boolean isConnected(String clientUUID) {
        return openConnections.containsKey(clientUUID);
    }
}
