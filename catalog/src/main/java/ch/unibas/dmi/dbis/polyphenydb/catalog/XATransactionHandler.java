/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.catalog;


import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogTransactionException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of the TransactionHandler for distributed transactions.
 */
class XATransactionHandler extends TransactionHandler {

    private static final Logger logger = LoggerFactory.getLogger( XATransactionHandler.class );

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


    @SuppressWarnings("Duplicates")
    private void close() {
        logger.info( "Closing a transaction handler for the catalog. Size of freeInstances before closing: " + freeInstances.size() );
        try {
            if ( openStatements != null ) {
                for ( Statement openStatement : openStatements ) {
                    openStatement.close();
                }
            }
        } catch ( SQLException e ) {
            logger.debug( "Exception while closing connections in connection handler", e );
        } finally {
            openStatements = null;
            activeInstances.remove( xid );
            freeInstances.add( this );
            logger.info( "Size of freeInstances after closing: " + freeInstances.size() );
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
            logger.debug( "Creating a new transaction handler for the catalog. Current freeInstances-Size: " + freeInstances.size() );
            handler = new XATransactionHandler();
        }
        return handler;
    }


}
