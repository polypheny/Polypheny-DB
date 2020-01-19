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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;


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


    public Statement getStatement() throws SQLException {
        return createStatement();
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

}
