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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.jdbc.PolyphenyStatement;

@Slf4j
@Category({ AdapterTestSuite.class, CassandraExcluded.class })
public class JdbcStatementTest {

    private static final String CREATE_TEST_TABLE = "CREATE TABLE IF NOT EXISTS my_table (id INT, name VARCHAR(50))";

    private static final String INSERT_TEST_DATA = "INSERT INTO my_table values(1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')";


    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void statementClosingTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
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
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
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
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            ResultSet rs1 = statement.executeQuery( "SELECT * FROM my_table" );
            assertFalse( rs1.isClosed() );
            ResultSet rs2 = statement.executeQuery( "SELECT * FROM my_table" );
            assertTrue( rs1.isClosed() );
            connection.rollback();
        }
    }


    @Test
    public void simpleValidUpdateReturnsUpdateCountTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            int rowsChanged = statement.executeUpdate( INSERT_TEST_DATA );
            assertEquals( 0, rowsChanged );
            connection.rollback();
            // assertEquals( 4, rowsChanged ); Polypheny currently returns 0 as update count if multiple rows are inserted in one statement
        }
    }


    @Test(expected = SQLException.class)
    public void simpleInvalidUpdateThrowsExceptionTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            int rowsChanged = statement.executeUpdate( "SELECT * FROM my_table" );
            connection.rollback();
        }
    }


    @Test
    public void simpleValidQueryReturnsResultSetTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.executeUpdate( INSERT_TEST_DATA );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            connection.rollback();
        }
    }


    @Test(expected = SQLException.class)
    public void simpleInvalidQueryThrowsExceptionTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.executeQuery( INSERT_TEST_DATA );
            connection.rollback();
        }
    }


    @Test
    public void simpleExecuteUpdateReturnsRowCountTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            assertFalse( statement.execute( INSERT_TEST_DATA ) );
            assertEquals( 0, statement.getUpdateCount() );
            connection.rollback();
            //assertEquals(4, statement.getUpdateCount() ); Polypheny currently returns 0 as update count if multiple rows are inserted in one statement
        }
    }


    @Test
    public void simpleExecuteUpdateReturnsLargeRowCountTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            assertFalse( statement.execute( INSERT_TEST_DATA ) );
            assertEquals( 0, statement.getLargeMaxRows() );
            connection.rollback();
            //assertEquals(4, statement.getUpdateCount() ); Polypheny currently returns 0 as update count if multiple rows are inserted in one statement
        }
    }


    @Test
    public void simpleExecuteQueryReturnsResultSetTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            assertTrue( statement.execute( "SELECT * FROM my_table" ) );
            statement.getResultSet();
            connection.rollback();
        }
    }


    @Test
    public void simpleExecuteUpdateReturnsProperValueForUnusedReturnTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            assertFalse( statement.execute( INSERT_TEST_DATA ) );
            assertEquals( 0, statement.getUpdateCount() );
            //assertEquals(4, statement.getUpdateCount() ); Polypheny currently returns 0 as update count if multiple rows are inserted in one statement
            assertNull( statement.getResultSet() );
            connection.rollback();
        }
    }


    @Test
    public void simpleExecuteQueryReturnsProperValueForUnusedReturnTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            statement.execute( "SELECT * FROM my_table" );
            assertNotNull( statement.getResultSet() );
            assertEquals( -1, statement.getUpdateCount() );
            connection.rollback();
        }
    }


    @Test
    public void maxFieldSizeTest() throws SQLException {
        int maxFieldSize = 4;
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( "INSERT INTO my_table values(1, 'ABCDEFGHIJKLOMNOPQRSTUVWXYZ')" );
            statement.setMaxFieldSize( maxFieldSize );
            assertEquals( maxFieldSize, statement.getMaxFieldSize() );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 1, "ABCD" } ) );
            connection.rollback();
        }
    }


    @Test(expected = SQLException.class)
    public void invalidMaxFieldSizeTest() throws SQLException {
        int maxFieldSize = -8;
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
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
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            statement.setMaxRows( maxRows );
            assertEquals( maxRows, statement.getMaxRows() );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            TestHelper.checkResultSet( rs, ImmutableList.of(
                    new Object[]{ 1, "A" },
                    new Object[]{ 2, "B" }
            ), true );
            connection.rollback();
        }
    }


    @Test
    public void largeMaxRowsTest() throws SQLException {
        long maxRows = 2L;
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            statement.setLargeMaxRows( maxRows );
            assertEquals( maxRows, statement.getLargeMaxRows() );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            TestHelper.checkResultSet( rs, ImmutableList.of(
                    new Object[]{ 1, "A" },
                    new Object[]{ 2, "B" }
            ), true );
            connection.rollback();
        }
    }


    @Test
    public void maxRowsNotSetTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            assertEquals( 0, statement.getMaxRows() );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            TestHelper.checkResultSet( rs, ImmutableList.of(
                    new Object[]{ 1, "A" },
                    new Object[]{ 2, "B" },
                    new Object[]{ 3, "C" },
                    new Object[]{ 4, "D" }
            ), true );
            connection.rollback();
        }
    }


    @Test
    public void getWarningsNullTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            assertNull( statement.getWarnings() );
        }
    }


    @Test
    public void clearWarningsTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.clearWarnings();
            assertNull( statement.getWarnings() );
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCursorNameNotSupportedTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.setCursorName( "foo" );
        }
    }


    @Test
    public void getMoreResultsReturnsFalseTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            assertEquals( 0, statement.getMaxRows() );
            statement.executeQuery( "SELECT * FROM my_table" );
            assertFalse( statement.getMoreResults() );
            connection.rollback();
        }
    }


    @Test
    public void setFetchDirectionTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.setFetchDirection( ResultSet.FETCH_FORWARD );
            assertEquals( ResultSet.FETCH_FORWARD, statement.getFetchDirection() );
        }
    }


    @Test(expected = SQLException.class)
    public void setIllegalFetchDirectionTest1() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.setFetchDirection( ResultSet.FETCH_REVERSE );
            assertEquals( ResultSet.FETCH_REVERSE, statement.getFetchDirection() );
        }
    }


    @Test(expected = SQLException.class)
    public void setIllegalFetchDirectionTest2() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.setFetchDirection( ResultSet.FETCH_UNKNOWN );
            assertEquals( ResultSet.FETCH_UNKNOWN, statement.getFetchDirection() );
        }
    }


    @Test
    public void setFetchSizeValueTest() throws SQLException {
        int fetchSize = 250;
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.setFetchSize( fetchSize );
            assertEquals( fetchSize, statement.getFetchSize() );
        }
    }


    @Test
    public void getResultSetConcurrencyDefaultTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            assertEquals( ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency() );
        }
    }


    @Test
    public void getResultSetTypeDefaultTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            assertEquals( ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType() );
        }
    }


    @Test
    public void getResultSetTypeForwardTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY );
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            assertEquals( ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType() );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            assertEquals( ResultSet.TYPE_FORWARD_ONLY, rs.getType() );
            connection.rollback();
        }
    }


    @Test
    public void getResultSetTypeScrollInsensitiveTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY );
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            assertEquals( ResultSet.TYPE_SCROLL_INSENSITIVE, statement.getResultSetType() );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            assertEquals( ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType() );
            connection.rollback();
        }
    }


    @Test
    public void simpleBatchExecuteTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.addBatch( "INSERT INTO my_table VALUES(1, 'A')" );
            statement.addBatch( "INSERT INTO my_table VALUES(2, 'B')" );
            statement.addBatch( "INSERT INTO my_table VALUES(3, 'C')" );
            statement.addBatch( "INSERT INTO my_table VALUES(4, 'D')" );
            int[] rowUpdates = statement.executeBatch();
            int[] expected = { 1, 1, 1, 1 };
            assertArrayEquals( expected, rowUpdates );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            TestHelper.checkResultSet( rs, ImmutableList.of(
                    new Object[]{ 1, "A" },
                    new Object[]{ 2, "B" },
                    new Object[]{ 3, "C" },
                    new Object[]{ 4, "D" }
            ), true );
            connection.rollback();
        }
    }


    @Test
    public void simpleLargeBatchExecuteTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.addBatch( "INSERT INTO my_table VALUES(1, 'A')" );
            statement.addBatch( "INSERT INTO my_table VALUES(2, 'B')" );
            statement.addBatch( "INSERT INTO my_table VALUES(3, 'C')" );
            statement.addBatch( "INSERT INTO my_table VALUES(4, 'D')" );
            long[] rowUpdates = statement.executeLargeBatch();
            long[] expected = { 1, 1, 1, 1 };
            assertArrayEquals( expected, rowUpdates );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            TestHelper.checkResultSet( rs, ImmutableList.of(
                    new Object[]{ 1, "A" },
                    new Object[]{ 2, "B" },
                    new Object[]{ 3, "C" },
                    new Object[]{ 4, "D" }
            ), true );
            connection.rollback();
        }
    }


    @Test
    public void clearBatchTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.addBatch( "INSERT INTO my_table VALUES(1, 'A')" );
            statement.addBatch( "INSERT INTO my_table VALUES(2, 'B')" );
            statement.clearBatch();
            statement.addBatch( "INSERT INTO my_table VALUES(3, 'C')" );
            statement.addBatch( "INSERT INTO my_table VALUES(4, 'D')" );
            statement.executeBatch();
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            TestHelper.checkResultSet( rs, ImmutableList.of(
                    new Object[]{ 3, "C" },
                    new Object[]{ 4, "D" }
            ), true );
            connection.rollback();
        }
    }


    @Test
    public void getConectionTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            assertEquals( connection, statement.getConnection() );
        }
    }


    @Test
    public void getMoreResultsWithBehaviourTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            assertEquals( 0, statement.getMaxRows() );
            statement.executeQuery( "SELECT * FROM my_table" );
            assertFalse( statement.getMoreResults( Statement.CLOSE_CURRENT_RESULT ) );
            connection.rollback();
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getMoreResultsWithInvalidBehaviourKeepThrowsExceptionTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            assertEquals( 0, statement.getMaxRows() );
            statement.executeQuery( "SELECT * FROM my_table" );
            assertFalse( statement.getMoreResults( Statement.KEEP_CURRENT_RESULT ) );
            connection.rollback();
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getMoreResultsWithInvalidBehaviourCloseThrowsExceptionTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            assertEquals( 0, statement.getMaxRows() );
            statement.executeQuery( "SELECT * FROM my_table" );
            assertFalse( statement.getMoreResults( Statement.CLOSE_ALL_RESULTS ) );
            connection.rollback();
        }
    }


    @Test(expected = SQLException.class)
    public void getMoreResultsWithINonsenseThrowsExceptionTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            assertEquals( 0, statement.getMaxRows() );
            statement.executeQuery( "SELECT * FROM my_table" );
            assertFalse( statement.getMoreResults( 1234 ) );
            connection.rollback();
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getGeneratedKeysTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {

            statement.getGeneratedKeys();
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeLargeUpdateWithAutogeneratedKeysTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {

            statement.executeLargeUpdate( "INSERT INTO my_table (id, name) VALUES (1, 'John')", 1 );
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeUpdateWithColumnIndexesTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {

            statement.executeUpdate( "INSERT INTO my_table (id, name) VALUES (2, 'Jane')", new int[]{ 1 } );
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeUpdateWithColumnNamesTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {

            statement.executeUpdate( "INSERT INTO my_table (id, name) VALUES (3, 'Doe')", new String[]{ "id" } );
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeWithIntTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {

            statement.execute( "INSERT INTO my_table (id, name) VALUES (4, 'Smith')", 1 );
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeWithIntArrayTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {

            statement.execute( "INSERT INTO my_table (id, name) VALUES (5, 'Brown')", new int[]{ 1 } );
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeWithStringArrayTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {

            statement.execute( "INSERT INTO my_table (id, name) VALUES (6, 'Taylor')", new String[]{ "id" } );
        }
    }


    @Test
    public void getResultSetHoldabilityTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            assertEquals( ResultSet.CLOSE_CURSORS_AT_COMMIT, statement.getResultSetHoldability() );
        }
    }


    @Test
    public void isPoolableDefaultTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            assertFalse( statement.isPoolable() );
        }
    }


    @Test
    public void closeOnCompletionFalseTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            assertFalse( statement.isCloseOnCompletion() );
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            TestHelper.checkResultSet( rs, ImmutableList.of(
                    new Object[]{ 1, "A" },
                    new Object[]{ 2, "B" },
                    new Object[]{ 3, "C" },
                    new Object[]{ 4, "D" }
            ), true );
            rs.close();
            assertFalse( statement.isClosed() );
            connection.rollback();
        }
    }


    @Test
    public void closeOnCompletionTrueTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( CREATE_TEST_TABLE );
            statement.execute( INSERT_TEST_DATA );
            statement.closeOnCompletion();
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            TestHelper.checkResultSet( rs, ImmutableList.of(
                    new Object[]{ 1, "A" },
                    new Object[]{ 2, "B" },
                    new Object[]{ 3, "C" },
                    new Object[]{ 4, "D" }
            ), true );
            rs.close();
            assertTrue( statement.isClosed() );
            connection.rollback();
        }
    }


    @Test
    public void isWrapperForTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            assertTrue( statement.isWrapperFor( PolyphenyStatement.class ) );
        }
    }


    @Test
    public void unwrapTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            PolyphenyStatement polyphenyStatement = statement.unwrap( PolyphenyStatement.class );
        }
    }


    @Test
    public void isWrapperForFalseTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            assertFalse( statement.isWrapperFor( PolyphenyDb.class ) );
        }
    }


    @Test(expected = SQLException.class)
    public void unwrapExceptionTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            PolyphenyDb polyDb = statement.unwrap( PolyphenyDb.class );
        }
    }

}
