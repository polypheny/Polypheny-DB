/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
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

package ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.connection;


import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import lombok.extern.slf4j.Slf4j;


/**
 * Implementation of the ConnectionFactory for distributed transactions.
 */
@Slf4j
public class XaConnectionFactory implements ConnectionFactory {

    protected final Map<Xid, XaConnectionHandler> activeInstances;
    protected final Queue<XaConnectionHandler> freeInstances;

    private final int maxConnections;
    private final XADataSource dataSource;


    public XaConnectionFactory( XADataSource dataSource, int maxConnections ) {
        super();
        this.maxConnections = maxConnections;
        this.dataSource = dataSource;
        this.activeInstances = new ConcurrentHashMap<>();
        this.freeInstances = new ConcurrentLinkedQueue<>();
    }


    @Override
    public XaConnectionHandler getOrCreateConnectionHandler( Xid xid ) throws ConnectionHandlerException {
        if ( !activeInstances.containsKey( xid ) ) {
            XaConnectionHandler xaConnectionHandler = getFreeTransactionHandler();
            xaConnectionHandler.init( xid );
            activeInstances.put( xid, xaConnectionHandler );
            return xaConnectionHandler;
        }
        return getConnectionHandler( xid );
    }


    @Override
    public boolean hasConnectionHandler( Xid xid ) {
        return activeInstances.containsKey( xid );
    }


    @Override
    public XaConnectionHandler getConnectionHandler( Xid xid ) {
        return activeInstances.get( xid );
    }


    @Override
    public void close() throws SQLException {
        log.warn( "Not implemented!" );
    }


    private XaConnectionHandler getFreeTransactionHandler() throws ConnectionHandlerException {
        XaConnectionHandler handler = freeInstances.poll();
        if ( handler == null ) {
            if ( getNumActive() + getNumIdle() < maxConnections ) {
                log.debug( "Creating a new transaction handler. Current freeInstances-Size: {}", freeInstances.size() );
                try {
                    handler = new XaConnectionHandler( dataSource.getXAConnection() );
                } catch ( SQLException e ) {
                    throw new ConnectionHandlerException( "Caught exception while creating connection handler", e );
                }
            } else {
                log.warn( "No free connection handler and max number of handlers reach. Wait for a free instances." );
                // Wait for a free instance
                while ( handler == null ) {
                    try {
                        Thread.sleep( 10 );
                    } catch ( InterruptedException e ) {
                        // Ignore
                    }
                    handler = freeInstances.poll();
                }
            }
        }
        return handler;
    }


    @Override
    public int getMaxTotal() {
        return maxConnections;
    }


    @Override
    public int getNumActive() {
        return activeInstances.size();
    }


    @Override
    public int getNumIdle() {
        return freeInstances.size();
    }


    public class XaConnectionHandler extends ConnectionHandler {

        private final XAResource xaResource;
        private Xid xid;


        XaConnectionHandler( XAConnection xaConnection ) throws ConnectionHandlerException {
            super();
            try {
                xaResource = xaConnection.getXAResource();
                connection = xaConnection.getConnection();
                statement = connection.createStatement();
            } catch ( SQLException e ) {
                throw new ConnectionHandlerException( "Error while connecting to database!", e );
            }
        }


        void init( final Xid xid ) throws ConnectionHandlerException {
            if ( activeInstances.containsKey( xid ) ) {
                throw new ConnectionHandlerException( "There is already a connection handler for this xid!" );
            }
            this.xid = xid;
            try {
                xaResource.start( xid, XAResource.TMNOFLAGS );
            } catch ( XAException e ) {
                throw new ConnectionHandlerException( "Error while starting transaction", e );
            }
        }


        @Override
        public boolean prepare() throws ConnectionHandlerException {
            try {
                xaResource.end( xid, XAResource.TMSUCCESS );
                return xaResource.prepare( xid ) == XAResource.XA_OK;
            } catch ( XAException e ) {
                throw new ConnectionHandlerException( "Error while executing prepare on transaction!", e );
            }
        }


        @Override
        public void commit() throws ConnectionHandlerException {
            try {
                xaResource.commit( xid, false );
            } catch ( XAException e ) {
                throw new ConnectionHandlerException( "Error while committing transaction on database!", e );
            } finally {
                close();
            }
        }


        @Override
        public void rollback() throws ConnectionHandlerException {
            try {
                xaResource.end( xid, XAResource.TMFAIL );
                xaResource.rollback( xid );
            } catch ( XAException e ) {
                throw new ConnectionHandlerException( "Error while rollback transaction on database!", e );
            } finally {
                close();
            }
        }


        private void close() {
            log.debug( "Closing a transaction handler. Size of freeInstances before closing: {}", freeInstances.size() );
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
                xid = null;
                freeInstances.add( this );
                log.debug( "Size of freeInstances after closing: {}", freeInstances.size() );
            }
        }

    }


}
