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


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Represents a transaction and provides methods to interact with the database system.
 */
abstract class TransactionHandler {

    private static final Logger logger = LoggerFactory.getLogger( TransactionHandler.class );

    Connection connection;
    Statement statement;

    /**
     * List of all statements which have to be closed to free resources
     */
    List<Statement> openStatements;


    public int executeUpdate( final String sql ) throws SQLException {
        logger.trace( "Executing query on catalog database: " + sql );
        return statement.executeUpdate( sql );
    }


    public ResultSet executeSelect( final String sql ) throws SQLException {
        logger.trace( "Executing query on catalog database: " + sql );
        return createStatement().executeQuery( sql );
    }


    public void execute( final String sql ) throws SQLException {
        logger.trace( "Executing query on catalog database: " + sql );
        statement.execute( sql );
    }


    abstract boolean prepare() throws CatalogTransactionException;

    abstract void commit() throws CatalogTransactionException;

    abstract void rollback() throws CatalogTransactionException;


    public DatabaseMetaData getMetaData () throws SQLException {
        return connection.getMetaData();
    }


    private Statement createStatement() throws SQLException {
        if ( openStatements == null ) {
            openStatements = new LinkedList<>();
        }
        Statement statement = connection.createStatement();
        openStatements.add( statement );
        return statement;
    }

}
