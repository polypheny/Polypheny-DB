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
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.jdbc.PolyhenyResultSet;

public class JdbcResultSetTest {

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
            ResultSet rs = statement.executeQuery( "SELECT * FROM my_table" );
            rs.next();
            assertNull( rs.getString( 2 ) );
            assertTrue( rs.wasNull() );
            rs.close();
            assertTrue( rs.isClosed() );
            statement.executeUpdate( "DROP TABLE my_table" );
        }
    }


    @Test
    public void getWarningsReturnsNullTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertNull( rs.getWarnings() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void clearWarningsTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.clearWarnings();
            assertNull( rs.getWarnings() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void getCursorNameThrowsExceptionTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.getCursorName();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void findColumnTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertEquals( 1, rs.findColumn( "id" ) );
            assertEquals( 2, rs.findColumn( "hex_value" ) );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void beforeFirstThrowsWhenForwardOnlyTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.next();
            rs.beforeFirst();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void beforeFirstTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.next();
            assertFalse( rs.isBeforeFirst() );
            rs.beforeFirst();
            assertTrue( rs.isBeforeFirst() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void afterLastThrowsWhenForwardOnlyTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.afterLast();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void afterLastTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertFalse( rs.isAfterLast() );
            rs.afterLast();
            assertTrue( rs.isAfterLast() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void firstThrowsWhenForwardOnlyTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertFalse( rs.isFirst() );
            rs.first();
            assertTrue( rs.isFirst() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void firstTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertFalse( rs.isFirst() );
            rs.first();
            assertTrue( rs.isFirst() );
            assertEquals( 1, rs.getInt( 1 ) );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void lastThrowsWhenForwardOnlyTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.last();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void lastTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertFalse( rs.isLast() );
            rs.last();
            assertTrue( rs.isLast() );
            assertEquals( 25, rs.getInt( 1 ) );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void readingBeforeFirstThrowsException() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertTrue( rs.isBeforeFirst() );
            rs.getInt( 1 );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void readingAfterLastThrowsException() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.afterLast();
            assertTrue( rs.isAfterLast() );
            rs.getInt( 1 );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void accessWithColumnNamesTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.first();
            assertEquals( 1, rs.getInt( "id" ) );
            assertEquals( "1", rs.getString( "hex_value" ) );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void rowIndexTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            int expected_row_index = 1; //jdbc starts enumeration with 1
            assertEquals( 0, rs.getRow() );
            while ( rs.next() ) {
                assertEquals( expected_row_index, rs.getRow() );
                expected_row_index++;
            }
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void absoluteThrowsWhenForwardOnlyTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.absolute( 5 );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void absoluteValidPositionTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertTrue( rs.absolute( 5 ) );
            assertEquals( 5, rs.getRow() );
            assertEquals( 5, rs.getInt( 1 ) );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void absoluteBeforeFirstTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertTrue( rs.absolute( 0 ) );
            assertEquals( 0, rs.getRow() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void absoluteAccessFromEndTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertTrue( rs.absolute( -5 ) );
            assertEquals( 46, rs.getRow() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void absoluteCurrentPositionTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.next();
            assertTrue( rs.absolute( 1 ) );
            assertEquals( 1, rs.getRow() );
            assertEquals( 1, rs.getInt( 1 ) );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void absoluteOvershootPositionTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.next();
            assertFalse( rs.absolute( 800 ) );
            assertTrue( rs.isAfterLast() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void absoluteNegativeOvershootPositionTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.next();
            assertFalse( rs.absolute( -800 ) );
            assertTrue( rs.isBeforeFirst() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void relativeThrowsWhenForwardOnlyTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.relative( 5 );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void relativeCurrentPositionTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.next();
            assertTrue( rs.relative( 0 ) );
            assertEquals( 1, rs.getRow() );
            assertEquals( 1, rs.getInt( 1 ) );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void relativeValidPositiveTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertTrue( rs.relative( 5 ) );
            assertEquals( 5, rs.getRow() );
            assertEquals( 5, rs.getInt( 1 ) );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void relativeValidNegativeTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.absolute( 10 );
            assertTrue( rs.relative( -2 ) );
            assertEquals( 8, rs.getRow() );
            assertEquals( 8, rs.getInt( 1 ) );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void relativeOverflowNegativeTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.absolute( 10 );
            assertFalse( rs.relative( -800 ) );
            assertEquals( 0, rs.getRow() );
            assertTrue( rs.isBeforeFirst() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void relativeOverflowPositiveTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.absolute( 10 );
            assertFalse( rs.relative( 800 ) );
            assertTrue( rs.isAfterLast() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void reverseIterationWithPreviousTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.afterLast();
            int expected_index = 25;
            while ( rs.previous() ) {
                assertEquals( expected_index, rs.getRow() );
                assertEquals( expected_index, rs.getInt( 1 ) );
                expected_index--;
            }
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void illegalFetchDirectionThrowsExceptionTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.setFetchDirection( ResultSet.FETCH_REVERSE );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void fetchDirectionForwardTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.setFetchDirection( ResultSet.FETCH_FORWARD );
            assertEquals( ResultSet.FETCH_FORWARD, rs.getFetchDirection() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void fetchDirectionReverseTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.setFetchDirection( ResultSet.FETCH_REVERSE );
            assertEquals( ResultSet.FETCH_REVERSE, rs.getFetchDirection() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void fetchDirectionUnknownTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.setFetchDirection( ResultSet.FETCH_UNKNOWN );
            assertEquals( ResultSet.FETCH_UNKNOWN, rs.getFetchDirection() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void illegalFetchSizeThrowsExceptionTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.setFetchSize( -42 );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void fetchSizeZeroTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.setFetchSize( 0 );
            assertEquals( 0, rs.getFetchSize() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void validFetchSizeTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.setFetchSize( 250 );
            assertEquals( 250, rs.getFetchSize() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void typeTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertEquals( ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType() );
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void concurrencyTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY ) ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertEquals( ResultSet.CONCUR_READ_ONLY, rs.getConcurrency() );
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void updatedNotSupportedTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.rowUpdated();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void insertedNotSupportedTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.rowInserted();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void deletedNotSupportedTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.deleteRow();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void insertThrowsWhenReadOnlyTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.insertRow();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void updateThrowsWhenReadOnly() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.updateRow();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void deleteThrowsWhenReadOnly() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.deleteRow();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void refreshThrowsWhenReadOnly() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.refreshRow();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void cancelThrowsWhenReadOnly() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.cancelRowUpdates();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void moveToInsertRowThrowsWhenReadOnly() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.moveToInsertRow();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test(expected = SQLException.class)
    public void moveToCurrentRowThrowsWhenReadOnly() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            rs.moveToCurrentRow();
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void getStatementTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertEquals( statement, rs.getStatement() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void getHoldabilityTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true );
                Connection connection = jdbcConnection.getConnection();
                Statement statement = connection.createStatement() ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertEquals( ResultSet.CLOSE_CURSORS_AT_COMMIT, rs.getHoldability() );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void isWrapperForTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertTrue( rs.isWrapperFor( PolyResultSet.class ) );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test
    public void unwrapTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            PolyhenyResultSet polyRs = rs.unwrap( PolyhenyResultSet.class );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }

    @Test
    public void isuWrapperForFalseTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            assertFalse( rs.isWrapperFor( PolyphenyDb.class ) );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }


    @Test (expected = SQLException.class)
    public void unwrapExceptionTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
                Statement statement = connection.createStatement();
        ) {
            createTableWithData( connection );
            ResultSet rs = statement.executeQuery( SELECT_SQL );
            PolyphenyDb polyDb = rs.unwrap( PolyphenyDb.class );
            rs.close();
            statement.executeUpdate( DROP_TABLE_SQL );
        }
    }

}


