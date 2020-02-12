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

package ch.unibas.dmi.dbis.polyphenydb.catalog;


import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogTransactionException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;


/**
 * Represents a transaction and provides methods to interact with the database system.
 */
@Slf4j
abstract class TransactionHandler {

    Connection connection;
    Statement statement;

    /**
     * List of all statements which have to be closed to free resources
     */
    ConcurrentLinkedQueue<Statement> openStatements;


    int executeUpdate( final String sql ) throws SQLException {
        log.trace( "Executing query on catalog database: {}", sql );
        return statement.executeUpdate( sql );
    }


    ResultSet executeSelect( final String sql ) throws SQLException {
        log.trace( "Executing query on catalog database: {}", sql );
        return createStatement().executeQuery( sql );
    }


    void execute( final String sql ) throws SQLException {
        log.trace( "Executing query on catalog database: {}", sql );
        statement.execute( sql );
    }


    ResultSet getGeneratedKeys() throws SQLException {
        return statement.executeQuery( "CALL IDENTITY();" );
    }


    abstract boolean prepare() throws CatalogTransactionException;

    abstract void commit() throws CatalogTransactionException;

    abstract void rollback() throws CatalogTransactionException;


    private Statement createStatement() throws SQLException {
        if ( openStatements == null ) {
            openStatements = new ConcurrentLinkedQueue<>();
        }
        Statement statement = connection.createStatement();
        openStatements.add( statement );
        return statement;
    }

}
