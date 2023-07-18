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

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ClientManager {

    private ConcurrentHashMap<String, PIClient> openConnections;
    private final Authenticator authenticator;
    private final TransactionManager transactionManager;


    public ClientManager(Authenticator authenticator, TransactionManager transactionManager) {
        this.openConnections = new ConcurrentHashMap<>();
        this.authenticator = authenticator;
        this.transactionManager = transactionManager;
    }

    public void unregisterConnection(PIClient client) {
        synchronized (client) {
            client.prepareForDisposal();
            openConnections.remove(client.getClientUUID());
        }
    }


    public void registerConnection(ConnectionRequest connectionRequest) throws AuthenticationException, TransactionException, PIServiceException {
        if (log.isTraceEnabled()) {
            log.trace("User {} tries to establish connection via proto interface.", connectionRequest.getClientUuid());
        }
        // reject already connected user
        if (isConnected(connectionRequest.getClientUuid())) {
            throw new PIServiceException("A user with uid " + connectionRequest.getClientUuid() + "is already connected.");
        }
        if (!connectionRequest.hasUsername() || !connectionRequest.hasPassword()) {
            throw new PIServiceException("No username or password given.");
        }
        PIClientProperties properties = getPropertiesOrDefault(connectionRequest);
        final CatalogUser user = authenticateUser(connectionRequest.getUsername(), connectionRequest.getPassword());
        Transaction transaction = transactionManager.startTransaction(user, null, false, "proto-interface");
        LogicalNamespace namespace;
        if (properties.haveNamespaceName()) {
            namespace = Catalog.getInstance().getSnapshot().getNamespace(properties.getNamespaceName());
        } else {
            namespace = Catalog.getInstance().getSnapshot().getNamespace(Catalog.defaultNamespaceName);
        }
        assert namespace != null;
        transaction.commit();
        properties.updateNamespaceName(namespace.getName());
        PIClient client = PIClient.newBuilder()
                .setMajorApiVersion(connectionRequest.getMajorApiVersion())
                .setMinorApiVersion(connectionRequest.getMinorApiVersion())
                .setClientUUID(connectionRequest.getClientUuid())
                .setTransactionManager(transactionManager)
                .setCatalogUser(user)
                .setLogicalNamespace(namespace)
                .setClientProperties(properties)
                .build();
        openConnections.put(connectionRequest.getClientUuid(), client);
        if (log.isTraceEnabled()) {
            log.trace("proto-interface established connection to user {}.", connectionRequest.getClientUuid());
        }
    }

    private PIClientProperties getPropertiesOrDefault(ConnectionRequest connectionRequest) {
        if (connectionRequest.hasConnectionProperties()) {
            return new PIClientProperties(connectionRequest.getConnectionProperties());
        }
        return PIClientProperties.getDefaultInstance();
    }


    public PIClient getClient(String clientUUID) throws PIServiceException {
        if (!openConnections.containsKey(clientUUID)) {
            throw new PIServiceException("Client not registered! Has the server been restarted in the meantime?");
        }
        return openConnections.get(clientUUID);
    }


    private CatalogUser authenticateUser(String username, String password) throws AuthenticationException {
        return authenticator.authenticate(username, password);
    }


    private boolean isConnected(String clientUUID) {
        return openConnections.containsKey(clientUUID);
    }

}
