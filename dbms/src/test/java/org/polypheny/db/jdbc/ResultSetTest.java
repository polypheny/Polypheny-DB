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

package org.polypheny.db.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;
import org.polypheny.db.TestHelper.JdbcConnection;

public class ResultSetTest {

    private static final String TABLE_SQL = "CREATE TABLE IF NOT EXISTS resultset_test (id INT, hex_value VARCHAR(2))";
    private static final String DROP_TABLE_SQL = "DROP TABLE resultset_test";
    private static final String DATA_SQL = buildInsertSql();

    private static final String SELECT_SQL = "SELECT * FROM resultset_test";


    private static String buildInsertSql() {
        StringBuilder sb = new StringBuilder( "INSERT INTO resultset_test VALUES " );

        for ( int i = 1; i <= 25; i++ ) {
            sb.append( "(" ).append( i ).append( ", '" ).append( Integer.toHexString( i ) ).append( "'), " );
        }

        // Remove the last comma and space, add a semicolon
        String finalQuery = sb.toString();
        finalQuery = finalQuery.substring( 0, finalQuery.length() - 2 );
        return finalQuery;
    }


    private void createTableWithData( Connection connection ) throws SQLException {
        try ( Statement statement = connection.createStatement(); ) {
            statement.execute( TABLE_SQL );
            statement.execute( DATA_SQL );
        }
    }


    @Test
    public void cursorInitBeforeFirst() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement()
        ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertTrue( rs.isBeforeFirst() );
            rs.close();
            statement.execute( DROP_TABLE_SQL );
        }
    }


    @Test
    public void cursorIncToFirst() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.next();
            assertTrue( rs.isFirst() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void cursorIncToLast() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            while ( rs.next() ) {
                if ( rs.isLast() ) {
                    assertEquals( 25, rs.getInt( 1 ) );
                }
            }
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void cursorIncToAfterLast() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            while ( rs.next() )
                ;
            assertTrue( rs.isAfterLast() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void cursorAndIterationOrderNextTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            while ( rs.next() ) {
                assertEquals( rs.getString( 2 ), Integer.toHexString( rs.getInt( 1 ) ) );
            }
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void resultSetCloseTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertFalse( rs.isClosed() );
            rs.close();
            assertTrue( rs.isClosed() );
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }

    @Test
    public void lastReadNullTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            statement.executeUpdate( "CREATE TABLE IF NOT EXISTS my_table (id INT, nullvalue VARCHAR(30))" );
            statement.executeUpdate( "INSERT INTO my_table VALUES (1, NULL)" );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table");
            rs.next();
            assertNull( rs.getString( 2 ) );
            assertTrue( rs.wasNull() );
            rs.close();
            assertTrue( rs.isClosed() );
            statement.executeUpdate( "DROP TABLE my_table" );
        }
    }



}
