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

import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.protointerface.proto.ConnectionRequest;
import org.polypheny.db.protointerface.transport.Transport;
import org.polypheny.db.protointerface.utils.PropertyUtils;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

@Slf4j
public class ClientManager {

    private static final long HEARTBEAT_TOLERANCE = 2000;
    @Getter
    private long heartbeatInterval;

    private final ConcurrentHashMap<String, PIClient> clients;
    private final Authenticator authenticator;
    private final TransactionManager transactionManager;
    private Timer cleanupTimer;
    private final MonitoringPage monitoringPage;


    public ClientManager( PIPlugin.ProtoInterface protoInterface ) {
        this.clients = new ConcurrentHashMap<>();
        this.authenticator = protoInterface.getAuthenticator();
        this.transactionManager = protoInterface.getTransactionManager();
        if ( protoInterface.isRequiresHeartbeat() ) {
            this.heartbeatInterval = protoInterface.getHeartbeatInterval();
            this.cleanupTimer = new Timer();
            cleanupTimer.schedule( createNewCleanupTask(), 0, heartbeatInterval + HEARTBEAT_TOLERANCE );
        }
        this.heartbeatInterval = 0;
        this.monitoringPage = protoInterface.getMonitoringPage();
        monitoringPage.setClientManager( this );
    }


    public void unregisterConnection( PIClient client ) {
        synchronized ( client ) {
            client.prepareForDisposal();
            clients.remove( client.getClientUUID() );
        }
    }


    public String registerConnection( ConnectionRequest connectionRequest, Transport t ) throws AuthenticationException, TransactionException, PIServiceException {
        byte[] raw = new byte[32];
        new SecureRandom().nextBytes( raw );
        String uuid = Base64.getUrlEncoder().encodeToString( raw );

        if ( log.isTraceEnabled() ) {
            log.trace( "User {} tries to establish connection via proto interface.", uuid );
        }
        final LogicalUser user;
        if ( connectionRequest.hasUsername() ) {
            String username = connectionRequest.getUsername();
            if ( !connectionRequest.hasPassword() ) {
                throw new AuthenticationException( "A password is required" );
            }
            String password = connectionRequest.getPassword();
            user = authenticator.authenticate( username, password );
        } else {
            user = t.getPeer()
                    .flatMap( u -> Catalog.getInstance().getSnapshot().getUser( u ) )
                    .orElseThrow( () -> new AuthenticationException( "Peer authentication failed: No user with that name" ) );
        }

        Transaction transaction = transactionManager.startTransaction( user.id, false, "proto-interface" );
        transaction.commit();
        LogicalNamespace namespace = getNamespaceOrDefault( connectionRequest );
        boolean isAutocommit = getAutocommitOrDefault( connectionRequest );
        PIClient client = new PIClient(
                uuid,
                user,
                transactionManager,
                namespace,
                monitoringPage,
                isAutocommit
        );
        clients.put( uuid, client );
        if ( log.isTraceEnabled() ) {
            log.trace( "proto-interface established connection to user {}.", uuid );
        }
        return uuid;
    }


    public Stream<Entry<String, PIClient>> getClients() {
        return clients.entrySet().stream();
    }

    public int getClientCount() {
        return clients.size();
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
        if ( !clients.containsKey( clientUUID ) ) {
            throw new PIServiceException( "Client not registered! Has the server been restarted in the meantime?" );
        }
        return clients.get( clientUUID );
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
        List<PIClient> inactiveClients = clients.values().stream()
                .filter( c -> !c.returnAndResetIsActive() ).toList();
        inactiveClients.forEach( this::unregisterConnection );
    }

}
