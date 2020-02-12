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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.CatalogConnectionException;
import org.polypheny.db.catalog.exceptions.CatalogTransactionException;


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
