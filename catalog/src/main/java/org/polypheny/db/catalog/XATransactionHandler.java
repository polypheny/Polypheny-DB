/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.catalog;


import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.CatalogConnectionException;
import org.polypheny.db.catalog.exceptions.CatalogTransactionException;


/**
 * Implementation of the TransactionHandler for distributed transactions.
 */
@Slf4j
class XATransactionHandler extends TransactionHandler {

    private static final Map<Xid, XATransactionHandler> activeInstances;
    private static final Queue<XATransactionHandler> freeInstances;

    private final XAConnection xaConnection;
    private final XAResource xaResource;
    private Xid xid;


    static {
        activeInstances = new ConcurrentHashMap<>();
        freeInstances = new ConcurrentLinkedQueue<>();
    }


    private XATransactionHandler() throws CatalogConnectionException {
        super();
        try {
            xaConnection = Database.getInstance().getXaConnection();
            xaResource = xaConnection.getXAResource();
            connection = xaConnection.getConnection();
            statement = connection.createStatement();
        } catch ( SQLException e ) {
            throw new CatalogConnectionException( "Error while connecting to catalog storage", e );
        }
    }


    private void init( final Xid xid ) throws CatalogTransactionException {
        if ( activeInstances.containsKey( xid ) ) {
            throw new CatalogTransactionException( "There is already a connection handler for this xid!" );
        }
        this.xid = xid;
        try {
            xaResource.start( xid, XAResource.TMNOFLAGS );
        } catch ( XAException e ) {
            throw new CatalogTransactionException( "Error while starting transaction", e );
        }
    }


    @Override
    boolean prepare() throws CatalogTransactionException {
        try {
            xaResource.end( xid, XAResource.TMSUCCESS );
            return xaResource.prepare( xid ) == XAResource.XA_OK;
        } catch ( XAException e ) {
            throw new CatalogTransactionException( "Error while executing prepare on catalog transaction!", e );
        }
    }


    @Override
    void commit() throws CatalogTransactionException {
        try {
            xaResource.commit( xid, false );
        } catch ( XAException e ) {
            throw new CatalogTransactionException( "Error while committing transaction in catalog storage", e );
        } finally {
            close();
        }
    }


    @Override
    void rollback() throws CatalogTransactionException {
        try {
            xaResource.end( xid, XAResource.TMFAIL );
            xaResource.rollback( xid );
        } catch ( XAException e ) {
            throw new CatalogTransactionException( "Error while rollback transaction in catalog storage", e );
        } finally {
            close();
        }
    }


    private void close() {
        log.debug( "Closing a transaction handler for the catalog. Size of freeInstances before closing: {}", freeInstances.size() );
        try {
            if ( openStatements != null ) {
                for ( Statement openStatement : openStatements ) {
                    openStatement.close();
                }
            }
        } catch ( SQLException e ) {
            log.debug( "Exception while closing connections in connection handler", e );
        } finally {
            openStatements = null;
            activeInstances.remove( xid );
            freeInstances.add( this );
            log.debug( "Size of freeInstances after closing: {}", freeInstances.size() );
        }
    }


    static XATransactionHandler getOrCreateTransactionHandler( Xid xid ) throws CatalogConnectionException, CatalogTransactionException {
        if ( !activeInstances.containsKey( xid ) ) {
            XATransactionHandler xaConnectionHandler = getFreeTransactionHandler();
            xaConnectionHandler.init( xid );
            activeInstances.put( xid, xaConnectionHandler );
            return xaConnectionHandler;
        }
        return getTransactionHandler( xid );
    }


    static boolean hasTransactionHandler( Xid xid ) {
        return XATransactionHandler.activeInstances.containsKey( xid );
    }


    static XATransactionHandler getTransactionHandler( Xid xid ) {
        return XATransactionHandler.activeInstances.get( xid );
    }


    private static XATransactionHandler getFreeTransactionHandler() throws CatalogConnectionException {
        XATransactionHandler handler = freeInstances.poll();
        if ( handler == null ) {
            log.debug( "Creating a new transaction handler for the catalog. Current freeInstances-Size: {}", freeInstances.size() );
            handler = new XATransactionHandler();
        }
        return handler;
    }


}
