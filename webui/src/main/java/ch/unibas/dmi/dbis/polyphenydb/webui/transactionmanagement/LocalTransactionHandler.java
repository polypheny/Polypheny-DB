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

package ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement;


import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of the TransactionHandler for local transaction (e.g., reads which must not be executed on all other nodes).
 */
public class LocalTransactionHandler extends TransactionHandler {

    private static final Logger logger = LoggerFactory.getLogger( LocalTransactionHandler.class );

    private static final Queue<LocalTransactionHandler> freeInstances = new ConcurrentLinkedQueue<>();


    private LocalTransactionHandler( final String driver, final String url, final String user, final String pass ) throws CatalogConnectionException {
        super();
        try {
            Class.forName( driver );
        } catch ( ClassNotFoundException e ) {
            throw new CatalogConnectionException( "Could not load postgres driver.", e );
        }

        Properties props = new Properties();
        props.setProperty( user, pass );
        //props.setProperty( "ssl", sslEnabled );
        props.setProperty( "wire_protocol", "PROTO3" );

        try {
            connection = DriverManager.getConnection( url, props );
        } catch ( SQLException e ) {
            throw new CatalogConnectionException( "Could not establish connection to driver", e );
        }

        try {
            connection.setAutoCommit( false );
            statement = connection.createStatement();
        } catch ( SQLException e ) {
            throw new CatalogConnectionException( "Error while connecting to catalog storage", e );
        }

    }


    //todo implement
    @Override
    boolean prepare() throws CatalogTransactionException {
        return false;
    }


    @Override
    public void commit() throws CatalogTransactionException {
        try {
            connection.commit();
        } catch ( SQLException e ) {
            throw new CatalogTransactionException( "Error while committing transaction in catalog storage", e );
        } finally {
            close();
        }
    }


    @Override
    public void rollback() throws CatalogTransactionException {
        try {
            connection.rollback();
        } catch ( SQLException e ) {
            throw new CatalogTransactionException( "Error while rollback transaction in catalog storage", e );
        } finally {
            close();
        }
    }


    @SuppressWarnings("Duplicates")
    private void close() {
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
            freeInstances.add( this );
        }
    }


    /**
     * @param driver driver name
     * @param url url
     * @param user user name
     * @param pass password
     */
    public static LocalTransactionHandler getTransactionHandler( final String driver, final String url, final String user, final String pass ) throws CatalogConnectionException {
        LocalTransactionHandler handler = freeInstances.poll();
        if ( handler == null ) {
            handler = new LocalTransactionHandler( driver, url, user, pass );
        }
        return handler;
    }


}
