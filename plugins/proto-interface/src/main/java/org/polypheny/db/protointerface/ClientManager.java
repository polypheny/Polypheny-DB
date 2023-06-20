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
import org.polypheny.db.protointerface.utils.ProtoUtils;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ClientManager {
    private ConcurrentHashMap<String, ProtoInterfaceClient> openConnections;
    private final Authenticator authenticator;
    private final TransactionManager transactionManager;


    public ClientManager( Authenticator authenticator, TransactionManager transactionManager ) {
        this.openConnections = new ConcurrentHashMap<>();
        this.authenticator = authenticator;
        this.transactionManager = transactionManager;
    }


    public void registerConnection( ConnectionRequest connectionRequest ) throws AuthenticationException, TransactionException, ProtoInterfaceServiceException {
        if ( log.isTraceEnabled() ) {
            log.trace( "User {} tries to establish connection via proto interface.", connectionRequest.getClientUuid() );
        }
        // reject already connected user
        if ( isConnected( connectionRequest.getClientUuid() ) ) {
            throw new ProtoInterfaceServiceException( "user with uid " + connectionRequest.getClientUuid() + "is already connected." );
        }
        Map<String, String> properties = connectionRequest.getConnectionPropertiesMap();
        if ( !credentialsPresent( properties ) ) {
            throw new ProtoInterfaceServiceException( "No username and password given." );
        }
        ProtoInterfaceClient.Builder clientBuilder = ProtoInterfaceClient.newBuilder();
        clientBuilder
                .setMajorApiVersion( connectionRequest.getMajorApiVersion() )
                .setMinorApiVersion( connectionRequest.getMinorApiVersion() )
                .setClientUUID( connectionRequest.getClientUuid() )
                .setConnectionProperties( properties )
                .setTransactionManager( transactionManager );
        final CatalogUser user = authenticateUser( properties.get( PropertyKeys.USERNAME ), properties.get( PropertyKeys.PASSWORD ) );
        Transaction transaction = transactionManager.startTransaction( user, null, false, "proto-interface" );
        LogicalNamespace namespace;
        if ( properties.containsKey( "namespace" )) {
            namespace = Catalog.getInstance().getSnapshot().getNamespace( properties.get( "namespace" ) );
        } else {
            namespace = Catalog.getInstance().getSnapshot().getNamespace( Catalog.defaultNamespaceName );
        }
        assert namespace != null;
        transaction.commit();
        clientBuilder
                .setCatalogUser( user )
                .setLogicalNamespace( namespace );
        openConnections.put( connectionRequest.getClientUuid(), clientBuilder.build() );
        if ( log.isTraceEnabled() ) {
            log.trace( "proto-interface established connection to user {}.", connectionRequest.getClientUuid() );
        }
    }


    public ProtoInterfaceClient getClient( String clientUUID ) throws ProtoInterfaceServiceException{
        if (!openConnections.contains( clientUUID )) {
            throw new ProtoInterfaceServiceException( "Client not registered" );
        }
        return openConnections.get( clientUUID );
    }


    private boolean credentialsPresent( Map<String, String> properties ) {
        return properties.containsKey( PropertyKeys.USERNAME ) && properties.containsKey( PropertyKeys.PASSWORD );
    }


    private CatalogUser authenticateUser( String username, String password ) throws AuthenticationException {
        return authenticator.authenticate( username, password );
    }


    private boolean isConnected( String clientUUID ) {
        return openConnections.containsKey( clientUUID );
    }

}
