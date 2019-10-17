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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;


/**
 * Implementation of the TransactionHandler for local transaction (e.g., reads which must not be executed on all other nodes).
 */
@Slf4j
class LocalTransactionHandler extends TransactionHandler {

    private static final Queue<LocalTransactionHandler> freeInstances = new ConcurrentLinkedQueue<>();


    private LocalTransactionHandler() throws CatalogConnectionException {
        super();
        try {
            connection = Database.getInstance().getConnection();
            connection.setAutoCommit( false );
            statement = connection.createStatement();
        } catch ( SQLException e ) {
            throw new CatalogConnectionException( "Error while connecting to catalog storage", e );
        }
    }


    @Override
    boolean prepare() throws CatalogTransactionException {
        return false;
    }


    @Override
    void commit() throws CatalogTransactionException {
        try {
            connection.commit();
        } catch ( SQLException e ) {
            throw new CatalogTransactionException( "Error while committing transaction in catalog storage", e );
        } finally {
            close();
        }
    }


    @Override
    void rollback() throws CatalogTransactionException {
        try {
            connection.rollback();
        } catch ( SQLException e ) {
            throw new CatalogTransactionException( "Error while rollback transaction in catalog storage", e );
        } finally {
            close();
        }
    }


    private void close() {
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
            freeInstances.add( this );
        }
    }


    static LocalTransactionHandler getTransactionHandler() throws CatalogConnectionException {
        LocalTransactionHandler handler = freeInstances.poll();
        if ( handler == null ) {
            handler = new LocalTransactionHandler();
        }
        return handler;
    }


}
