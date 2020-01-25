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


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.transaction.xa.Xid;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;


/**
 * Implementation of the ConnectionFactory for non-distributed transactions.
 */
@Slf4j
public class TransactionalConnectionFactory implements ConnectionFactory {

    protected final Map<Xid, TransactionalConnectionHandler> activeInstances;
    protected final Queue<TransactionalConnectionHandler> freeInstances;

    private final int maxConnections;
    private final BasicDataSource dataSource;


    public TransactionalConnectionFactory( BasicDataSource dataSource, int maxConnections ) {
        super();
        this.maxConnections = maxConnections;
        this.dataSource = dataSource;
        this.activeInstances = new ConcurrentHashMap<>();
        this.freeInstances = new ConcurrentLinkedQueue<>();
    }


    @Override
    public TransactionalConnectionHandler getOrCreateConnectionHandler( Xid xid ) throws ConnectionHandlerException {
        if ( !activeInstances.containsKey( xid ) ) {
            TransactionalConnectionHandler transactionHandler = getFreeTransactionHandler();
            transactionHandler.xid = xid;
            activeInstances.put( xid, transactionHandler );
            return transactionHandler;
        }
        return getConnectionHandler( xid );
    }


    @Override
    public boolean hasConnectionHandler( Xid xid ) {
        return activeInstances.containsKey( xid );
    }


    @Override
    public TransactionalConnectionHandler getConnectionHandler( Xid xid ) {
        return activeInstances.get( xid );
    }


    @Override
    public void close() throws SQLException {
        dataSource.close();
    }


    private TransactionalConnectionHandler getFreeTransactionHandler() throws ConnectionHandlerException {
        TransactionalConnectionHandler handler = freeInstances.poll();
        if ( handler == null ) {
            if ( getNumActive() + getNumIdle() < maxConnections ) {
                log.debug( "Creating a new transaction handler. Current freeInstances-Size: {}", freeInstances.size() );
                try {
                    handler = new TransactionalConnectionHandler( dataSource.getConnection() );
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


    public class TransactionalConnectionHandler extends ConnectionHandler {

        private Xid xid;


        TransactionalConnectionHandler( Connection connection ) throws ConnectionHandlerException {
            super();
            try {
                this.connection = connection;
                this.statement = connection.createStatement();
            } catch ( SQLException e ) {
                throw new ConnectionHandlerException( "Error while connecting to database!", e );
            }
        }


        @Override
        public boolean prepare() throws ConnectionHandlerException {
            log.warn( "Trying to prepare a non-XA transaction. Returning true for now!" );
            return true;
        }


        @Override
        public void commit() throws ConnectionHandlerException {
            try {
                connection.commit();
            } catch ( SQLException e ) {
                throw new ConnectionHandlerException( "Error while committing transaction", e );
            } finally {
                close();
            }
        }


        @Override
        public void rollback() throws ConnectionHandlerException {
            try {
                connection.rollback();
            } catch ( SQLException e ) {
                throw new ConnectionHandlerException( "Error while rollback transaction", e );
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
