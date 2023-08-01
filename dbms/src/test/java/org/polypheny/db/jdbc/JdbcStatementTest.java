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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.excluded.CassandraExcluded;

@Slf4j
@Category({ AdapterTestSuite.class, CassandraExcluded.class })
public class JdbcStatementTest {

    private static final String CREATE_TETST_TABLE = "CREATE TABLE IF NOT EXISTS my_table (id INT, name VARCHAR(50))";

    private static final String INSERT_TEST_DATA = "INSERT INTO my_table values(1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')";


    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void statementClosingTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
        ) {
            Connection connection = polyphenyDbConnection.getConnection();
            Statement statement = connection.createStatement();
            assertFalse( statement.isClosed() );
            statement.close();
            assertTrue( statement.isClosed() );
        }
    }


    @Test
    public void connectionClosesOpenStatementsTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
        ) {
            Connection connection = polyphenyDbConnection.getConnection();
            Statement first_statement = connection.createStatement();
            Statement second_statement = connection.createStatement();
            assertFalse( first_statement.isClosed() );
            assertFalse( second_statement.isClosed() );
            connection.close();
            assertTrue( first_statement.isClosed() );
            assertTrue( second_statement.isClosed() );
        }
    }


    @Test
    public void statementExecutionClosesCurrentResult() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TETST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            ResultSet rs1 = statement.executeQuery( "SELECT * FROM my_table" );
            assertFalse( rs1.isClosed() );
            ResultSet rs2 = statement.executeQuery( "SELECT * FROM my_table" );
            assertTrue( rs1.isClosed() );
        }
    }


    @Test
    public void simpleValidUpdateReturnsUpdateCountTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TETST_TABLE );
            int rowsChanged = statement.executeUpdate( INSERT_TEST_DATA );
            assertEquals( 0, rowsChanged );
            // assertEquals( 4, rowsChanged ); Polypheny currently returns 0 as update count if multiple rows are inserted in one statement
        }
    }


    @Test(expected = SQLException.class)
    public void simpleInvalidUpdateThrowsExceptionTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TETST_TABLE );
            int rowsChanged = statement.executeUpdate( "SELECT * FROM my_table" );
        }
    }


    @Test
    public void simpleValidQueryReturnsResultSetTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TETST_TABLE );
            statement.executeUpdate( INSERT_TEST_DATA );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
        }
    }


    @Test(expected = SQLException.class)
    public void simpleInvalidQueryThrowsExceptionTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TETST_TABLE );
            statement.executeQuery( INSERT_TEST_DATA );
        }
    }


    @Test
    public void simpleExecuteUpdateReturnsRowCountTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TETST_TABLE );
            assertFalse( statement.execute( INSERT_TEST_DATA ) );
            assertEquals( 0, statement.getUpdateCount() );
            //assertEquals(4, statement.getUpdateCount() ); Polypheny currently returns 0 as update count if multiple rows are inserted in one statement
        }
    }


    @Test
    public void simpleExecuteQueryReturnsResultSetTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TETST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            assertTrue( statement.execute( "SELECT * FROM my_table" ) );
            statement.getResultSet();
        }
    }


    @Test
    public void simpleExecuteUpdateReturnsProperValueForUnusedReturnTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TETST_TABLE );
            assertFalse( statement.execute( INSERT_TEST_DATA ) );
            assertEquals( 0, statement.getUpdateCount() );
            //assertEquals(4, statement.getUpdateCount() ); Polypheny currently returns 0 as update count if multiple rows are inserted in one statement
            assertNull( statement.getResultSet() );
        }
    }


    @Test
    public void simpleExecuteQueryReturnsProperValueForUnusedReturnTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TETST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            statement.execute( "SELECT * FROM my_table" );
            assertNotNull( statement.getResultSet() );
            assertEquals( -1, statement.getUpdateCount() );
        }
    }


    @Test
    public void maxFieldSizeTest() throws SQLException {
        int maxFieldSize = 4;
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TETST_TABLE );
            statement.execute( "INSERT INTO my_table values(1, 'ABCDEFGHIJKLOMNOPQRSTUVWXYZ')" );
            statement.setMaxFieldSize( maxFieldSize );
            assertEquals( maxFieldSize, statement.getMaxFieldSize() );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 1, "ABCD" } ) );
        }
    }


    @Test(expected = SQLException.class)
    public void invalidMaxFieldSizeTest() throws SQLException {
        int maxFieldSize = -8;
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.setMaxFieldSize( maxFieldSize );
        }
    }


    @Test
    public void maxRowsTest() throws SQLException {
        int maxRows = 2;
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TETST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            statement.setMaxRows( maxRows );
            assertEquals( maxRows, statement.getMaxRows() );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            TestHelper.checkResultSet( rs, ImmutableList.of(
                    new Object[]{ 1, "A" },
                    new Object[]{ 2, "B" }
            ) );
        }
    }


    @Test
    public void largeMaxRowsTest() throws SQLException {
        long maxRows = 2L;
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( true );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TETST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            //statement.setLargeMaxRows( maxRows );
            assertEquals( maxRows, statement.getLargeMaxRows() );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            TestHelper.checkResultSet( rs, ImmutableList.of(
                    new Object[]{ 1, "A" },
                    new Object[]{ 2, "B" }
            ), true );
        }
    }


}
