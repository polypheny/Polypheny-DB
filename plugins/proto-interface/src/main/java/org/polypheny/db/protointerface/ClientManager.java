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

import java.util.List;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.protointerface.proto.ConnectionRequest;
import org.polypheny.db.protointerface.utils.PropertyUtils;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

@Slf4j
public class ClientManager {

    private static final long HEARTBEAT_TOLERANCE = 2000;
    @Getter
    private long heartbeatInterval;

    private ConcurrentHashMap<String, PIClient> openConnections;
    private final Authenticator authenticator;
    private final TransactionManager transactionManager;
    private Timer cleanupTimer;


    public ClientManager( PIPlugin.ProtoInterface protoInterface ) {
        this.openConnections = new ConcurrentHashMap<>();
        this.authenticator = protoInterface.getAuthenticator();
        this.transactionManager = protoInterface.getTransactionManager();
        if ( protoInterface.isRequiresHeartbeat() ) {
            this.heartbeatInterval = protoInterface.getHeartbeatInterval();
            this.cleanupTimer = new Timer();
            cleanupTimer.schedule( createNewCleanupTask(), 0, heartbeatInterval + HEARTBEAT_TOLERANCE );
        }
        this.heartbeatInterval = 0;
    }


    public void unregisterConnection( PIClient client ) {
        synchronized ( client ) {
            client.prepareForDisposal();
            openConnections.remove( client.getClientUUID() );
        }
    }


    public void registerConnection( ConnectionRequest connectionRequest ) throws AuthenticationException, TransactionException, PIServiceException {
        if ( log.isTraceEnabled() ) {
            log.trace( "User {} tries to establish connection via proto interface.", connectionRequest.getClientUuid() );
        }
        // reject already connected user
        if ( isConnected( connectionRequest.getClientUuid() ) ) {
            throw new PIServiceException( "A user with uid " + connectionRequest.getClientUuid() + "is already connected." );
        }
        String username = connectionRequest.hasUsername() ? connectionRequest.getUsername() : Catalog.USER_NAME;
        String password = connectionRequest.hasPassword() ? connectionRequest.getPassword() : null;
        LogicalUser user = authenticator.authenticate( username, password );
        Transaction transaction = transactionManager.startTransaction( user.id, false, "proto-interface" );
        transaction.commit();
        LogicalNamespace namespace = getNamespaceOrDefault( connectionRequest );
        assert namespace != null;
        boolean isAutocommit = getAutocommitOrDefault( connectionRequest );
        PIClient client = new PIClient(
                connectionRequest.getClientUuid(),
                user,
                transactionManager,
                namespace,
                isAutocommit
        );
        openConnections.put( connectionRequest.getClientUuid(), client );
        if ( log.isTraceEnabled() ) {
            log.trace( "proto-interface established connection to user {}.", connectionRequest.getClientUuid() );
        }
    }


    private LogicalNamespace getNamespaceOrDefault( ConnectionRequest connectionRequest ) {
        String namespaceName = PropertyUtils.DEFAULT_NAMESPACE_NAME;
        if ( connectionRequest.hasConnectionProperties() && connectionRequest.getConnectionProperties().hasNamespaceName() ) {
            namespaceName = connectionRequest.getConnectionProperties().getNamespaceName();
        }
        Optional<LogicalNamespace> optionalNamespace = Catalog.getInstance().getSnapshot().getNamespace( namespaceName );
        if ( optionalNamespace.isEmpty() ) {
            throw new PIServiceException( "Getting namespace " + namespaceName + " failed." );
        }

        return optionalNamespace.get();
    }


    private boolean getAutocommitOrDefault( ConnectionRequest connectionRequest ) {
        if ( connectionRequest.hasConnectionProperties() && connectionRequest.getConnectionProperties().hasIsAutoCommit() ) {
            return connectionRequest.getConnectionProperties().getIsAutoCommit();
        }
        return PropertyUtils.AUTOCOMMIT_DEFAULT;
    }


    public PIClient getClient( String clientUUID ) throws PIServiceException {
        if ( !openConnections.containsKey( clientUUID ) ) {
            throw new PIServiceException( "Client not registered! Has the server been restarted in the meantime?" );
        }
        return openConnections.get( clientUUID );
    }


    private TimerTask createNewCleanupTask() {
        Runnable runnable = this::unregisterInactiveClients;
        return new TimerTask() {
            @Override
            public void run() {
                runnable.run();
            }
        };
    }


    private void unregisterInactiveClients() {
        List<PIClient> inactiveClients = openConnections.values().stream()
                .filter( c -> !c.returnAndResetIsActive() ).collect( Collectors.toList() );
        inactiveClients.forEach( this::unregisterConnection );
    }


    private boolean isConnected( String clientUUID ) {
        return openConnections.containsKey( clientUUID );
    }

}
