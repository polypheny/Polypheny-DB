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


import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.sql.language.SqlDialect;


/**
 * Represents a transaction and provides methods to interact with the database system.
 */
@Slf4j
public abstract class ConnectionHandler {

    protected Connection connection;
    protected Statement statement;


    /**
     * List of all statements which have to be closed to free resources
     */
    protected ConcurrentLinkedQueue<Statement> openStatements;


    public int executeUpdate( final String sql ) throws SQLException {
        log.trace( "Executing query on database: {}", sql );
        return statement.executeUpdate( sql );
    }


    public ResultSet executeQuery( final String sql ) throws SQLException {
        log.trace( "Executing query on database: {}", sql );
        return createStatement().executeQuery( sql );
    }


    public void execute( final String sql ) throws SQLException {
        log.trace( "Executing query on database: {}", sql );
        statement.execute( sql );
    }


    public Array createArrayOf( String typeName, Object[] elements ) throws SQLException {
        return connection.createArrayOf( typeName, elements );
    }


    public Statement getStatement() throws SQLException {
        return createStatement();
    }


    public PreparedStatement prepareStatement( String sql ) throws SQLException {
        if ( openStatements == null ) {
            openStatements = new ConcurrentLinkedQueue<>();
        }
        PreparedStatement preparedStatement = connection.prepareStatement( sql );
        openStatements.add( preparedStatement );
        return preparedStatement;
    }


    public abstract boolean prepare() throws ConnectionHandlerException;

    public abstract void commit() throws ConnectionHandlerException;

    public abstract void rollback() throws ConnectionHandlerException;


    private Statement createStatement() throws SQLException {
        if ( openStatements == null ) {
            openStatements = new ConcurrentLinkedQueue<>();
        }
        Statement statement = connection.createStatement();
        openStatements.add( statement );
        return statement;
    }


    public abstract SqlDialect getDialect();

}
