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

package org.polypheny.db.adapter.jdbc.connection;


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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.sql.language.SqlDialect;


/**
 * Implementation of the ConnectionFactory for distributed transactions.
 */
@Slf4j
public class XaConnectionFactory implements ConnectionFactory {

    protected final Map<Xid, XaConnectionHandler> activeInstances;
    protected final Queue<XaConnectionHandler> freeInstances;

    private final int maxConnections;
    private final XADataSource dataSource;

    @Getter
    private final SqlDialect dialect;


    public XaConnectionFactory( XADataSource dataSource, int maxConnections, SqlDialect dialect ) {
        super();
        this.maxConnections = maxConnections;
        this.dataSource = dataSource;
        this.activeInstances = new ConcurrentHashMap<>();
        this.freeInstances = new ConcurrentLinkedQueue<>();
        this.dialect = dialect;
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
                    handler = new XaConnectionHandler( dataSource.getXAConnection(), dialect );
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

        @Getter
        protected final SqlDialect dialect;


        XaConnectionHandler( XAConnection xaConnection, SqlDialect dialect ) throws ConnectionHandlerException {
            super();
            try {
                xaResource = xaConnection.getXAResource();
                connection = xaConnection.getConnection();
                statement = connection.createStatement();
                this.dialect = dialect;
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
                close();
            } catch ( XAException e ) {
                throw new ConnectionHandlerException( "Error while committing transaction on database!", e );
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
