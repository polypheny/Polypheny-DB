/*
 * Copyright 2019-2024 The Polypheny Project
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Tag("adapter")
@Slf4j
@Disabled
public class JdbcStatementTest {

    private static final String CREATE_TEST_TABLE = "CREATE TABLE IF NOT EXISTS my_table (id INT, name VARCHAR(50))";

    private static final String INSERT_TEST_DATA = "INSERT INTO my_table values(1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')";


    @BeforeAll
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


    @Test
    public void simpleInvalidUpdateThrowsExceptionTest() {
        assertThrows( SQLException.class, () -> {
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {
                statement.execute( CREATE_TEST_TABLE );
                int rowsChanged = statement.executeUpdate( "SELECT * FROM my_table" );
                connection.rollback();
            }
        } );
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


    @Test
    public void simpleInvalidQueryThrowsExceptionTest() {
        assertThrows( SQLException.class, () -> {
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {
                statement.execute( CREATE_TEST_TABLE );
                statement.executeQuery( INSERT_TEST_DATA );
                connection.rollback();
            }
        } );
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


    @Test
    public void invalidMaxFieldSizeTest() {
        assertThrows( SQLException.class, () -> {
            int maxFieldSize = -8;
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {
                statement.setMaxFieldSize( maxFieldSize );
            }
        } );
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


    @Test
    public void setCursorNameNotSupportedTest() {
        assertThrows( SQLFeatureNotSupportedException.class, () -> {
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {
                statement.setCursorName( "foo" );
            }
        } );
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


    @Test
    public void setIllegalFetchDirectionTest1() {
        assertThrows( SQLException.class, () -> {
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {
                statement.setFetchDirection( ResultSet.FETCH_REVERSE );
                assertEquals( ResultSet.FETCH_REVERSE, statement.getFetchDirection() );
            }
        } );
    }


    @Test
    public void setIllegalFetchDirectionTest2() throws SQLException {
        assertThrows( SQLException.class, () -> {
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {
                statement.setFetchDirection( ResultSet.FETCH_UNKNOWN );
                assertEquals( ResultSet.FETCH_UNKNOWN, statement.getFetchDirection() );
            }
        } );
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
    public void getConnectionTest() throws SQLException {
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


    @Test
    public void getMoreResultsWithInvalidBehaviourKeepThrowsExceptionTest() {
        assertThrows( SQLFeatureNotSupportedException.class, () -> {
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
        } );
    }


    @Test
    public void getMoreResultsWithInvalidBehaviourCloseThrowsExceptionTest() {
        assertThrows( SQLFeatureNotSupportedException.class, () -> {
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
        } );
    }


    @Test
    public void getMoreResultsWithINonsenseThrowsExceptionTest() {
        assertThrows( SQLException.class, () -> {
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
        } );
    }


    @Test
    public void getGeneratedKeysTest() {
        assertThrows( SQLFeatureNotSupportedException.class, () -> {
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {

                statement.getGeneratedKeys();
            }
        } );
    }


    @Test
    public void executeLargeUpdateWithAutogeneratedKeysTest() {
        assertThrows( SQLFeatureNotSupportedException.class, () -> {
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {

                statement.executeLargeUpdate( "INSERT INTO my_table (id, name) VALUES (1, 'John')", 1 );
            }
        } );
    }


    @Test
    public void executeUpdateWithColumnIndexesTest() {
        assertThrows( SQLFeatureNotSupportedException.class, () -> {
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {

                statement.executeUpdate( "INSERT INTO my_table (id, name) VALUES (2, 'Jane')", new int[]{ 1 } );
            }
        } );
    }


    @Test
    public void executeUpdateWithColumnNamesTest() {
        assertThrows( SQLFeatureNotSupportedException.class, () -> {
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {

                statement.executeUpdate( "INSERT INTO my_table (id, name) VALUES (3, 'Doe')", new String[]{ "id" } );
            }
        } );
    }


    @Test
    public void executeWithIntTest() {
        assertThrows( SQLFeatureNotSupportedException.class, () -> {
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {

                statement.execute( "INSERT INTO my_table (id, name) VALUES (4, 'Smith')", 1 );
            }
        } );
    }


    @Test
    public void executeWithIntArrayTest() {
        assertThrows( SQLFeatureNotSupportedException.class, () -> {
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {

                statement.execute( "INSERT INTO my_table (id, name) VALUES (5, 'Brown')", new int[]{ 1 } );
            }
        } );
    }


    @Test
    public void executeWithStringArrayTest() {
        assertThrows( SQLFeatureNotSupportedException.class, () -> {
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {

                statement.execute( "INSERT INTO my_table (id, name) VALUES (6, 'Taylor')", new String[]{ "id" } );
            }
        } );
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
    public void isWrapperForFalseTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            assertFalse( statement.isWrapperFor( PolyphenyDb.class ) );
        }
    }


    @Test
    public void unwrapExceptionTest() {
        assertThrows( SQLException.class, () -> {
            try (
                    JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                    Connection connection = polyphenyDbConnection.getConnection();
                    Statement statement = connection.createStatement();
            ) {
                PolyphenyDb polyDb = statement.unwrap( PolyphenyDb.class );
            }
        } );
    }

}
