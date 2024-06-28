/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.prisminterface;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.prisminterface.PIPlugin.PrismInterface;
import org.polypheny.db.prisminterface.transport.Transport;
import org.polypheny.db.prisminterface.utils.PropertyUtils;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.prism.ConnectionRequest;

@Slf4j
class ClientManager {

    private final ConcurrentHashMap<String, PIClient> clients;
    private final Authenticator authenticator;
    private final TransactionManager transactionManager;
    private final MonitoringPage monitoringPage;


    ClientManager( PrismInterface prismInterface ) {
        this.clients = new ConcurrentHashMap<>();
        this.authenticator = prismInterface.getAuthenticator();
        this.transactionManager = prismInterface.getTransactionManager();
        this.monitoringPage = prismInterface.getMonitoringPage();
        monitoringPage.setClientManager( this );
    }


    public void unregisterConnection( PIClient client ) {
        client.prepareForDisposal();
        clients.remove( client.getClientUUID() );
    }


    private LogicalUser getUser( ConnectionRequest connectionRequest, Transport t ) throws AuthenticationException {
        if ( connectionRequest.hasUsername() ) {
            String username = connectionRequest.getUsername();
            if ( !connectionRequest.hasPassword() ) {
                throw new AuthenticationException( "A password is required" );
            }
            String password = connectionRequest.getPassword();
            return authenticator.authenticate( username, password );
        } else if ( t.getPeer().isPresent() ) {
            String username = t.getPeer().get();
            Optional<LogicalUser> catalogUser = Catalog.getInstance().getSnapshot().getUser( username );
            if ( catalogUser.isPresent() ) {
                return catalogUser.get();
            } else {
                if ( username.equals( System.getProperty( "user.name" ) ) ) {
                    return Catalog.getInstance().getSnapshot().getUser( Catalog.USER_NAME ).orElseThrow();
                } else {
                    throw new AuthenticationException( "Peer authentication failed: No user with that name" );
                }
            }
        }
        throw new AuthenticationException( "Authentication failed" );
    }


    String registerConnection( ConnectionRequest connectionRequest, Transport t ) throws AuthenticationException, TransactionException, PIServiceException {
        byte[] raw = new byte[32];
        new SecureRandom().nextBytes( raw );
        String uuid = Base64.getUrlEncoder().encodeToString( raw );

        if ( log.isTraceEnabled() ) {
            log.trace( "User {} tries to establish connection via prism interface.", uuid );
        }
        final LogicalUser user = getUser( connectionRequest, t );
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
            log.trace( "prism-interface established connection to user {}.", uuid );
        }
        return uuid;
    }


    Stream<Entry<String, PIClient>> getClients() {
        return clients.entrySet().stream();
    }


    int getClientCount() {
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


    PIClient getClient( String clientUUID ) throws PIServiceException {
        return clients.get( clientUUID );
    }

}
