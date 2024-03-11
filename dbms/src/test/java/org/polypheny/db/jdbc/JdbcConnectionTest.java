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


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableList;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.jdbctypes.PolyphenyBlob;
import org.polypheny.jdbc.jdbctypes.PolyphenyClob;

@Slf4j
public class JdbcConnectionTest {

    private final static String dbHost = "localhost";
    private final static int port = 20590;


    @BeforeAll
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        try {
            Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );
        } catch ( ClassNotFoundException e ) {
            log.error( "Polypheny JDBC Driver not found", e );
        }
    }


    public Connection jdbcConnect( String url ) throws SQLException {
        log.debug( "Connecting to database @ {}", url );
        return DriverManager.getConnection( url );
    }


    public Connection jdbcConnect( String url, Properties properties ) throws SQLException {
        log.debug( "Connecting to database @ {} with properties {}", url, properties );
        return DriverManager.getConnection( url, properties );
    }


    @Test
    public void timeZoneTestNoTimezone() throws SQLException {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port;
        String tableDdl = "CREATE TABLE test(id INTEGER PRIMARY KEY, testtime TIME NOT NULL)";
        String insert = "INSERT INTO test VALUES (0, time '11:59:32')";
        String select = "SELECT testtime FROM test";
        try ( Connection con = jdbcConnect( url );
                Statement statement = con.createStatement() ) {
            statement.execute( tableDdl );
            statement.execute( insert );
            ResultSet rs = statement.executeQuery( select );
            TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ Time.valueOf( "11:59:32" ) } ) );
        }
    }


    @Test
    public void autocommitUrlTest() throws SQLException {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port + "/public?autocommit=false";
        try ( Connection con = jdbcConnect( url ) ) {
            assertFalse( con.getAutoCommit() );
        }
    }


    @Test
    public void readOnlyUrlTest() throws SQLException {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port + "/public?readonly=true";
        try ( Connection con = jdbcConnect( url ) ) {
            assertTrue( con.isReadOnly() );
        }
    }


    @Test
    public void isolationSerializableUrlTest() {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port + "/public?isolation=SERIALIZABLE";
        assertThrows( SQLException.class, () -> jdbcConnect( url ) );
    }


    @Test
    public void isolationDirtyUrlTest() {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port + "/public?isolation=DIRTY";
        assertThrows( SQLException.class, () -> jdbcConnect( url ) );
    }


    @Test
    public void isolationCommittedUrlTest() throws SQLException {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port + "/public?isolation=COMMITTED";
        try ( Connection con = jdbcConnect( url ) ) {
            assertEquals( Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation() );
        }
    }


    @Test
    public void isolationRepeatableReadUrlTest() {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port + "/public?isolation=REPEATABLE_READ";
        assertThrows( SQLException.class, () -> jdbcConnect( url ) );
    }


    @Test
    public void networkTimeoutUrlTest() throws SQLException {
        int expectedTimeout = 250000;
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port + "/public?nwtimeout=" + expectedTimeout;
        try ( Connection con = jdbcConnect( url ) ) {
            assertEquals( expectedTimeout, con.getNetworkTimeout() );
        }
    }


    @Test
    public void resultHoldabilityHoldUrlTest() {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port + "/public?holdability=HOLD";
        assertThrows( SQLException.class, () -> jdbcConnect( url ) );
    }


    @Test
    public void resultHoldabilityCloseUrlTest() throws SQLException {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port + "/public?holdability=CLOSE";
        try ( Connection con = jdbcConnect( url ) ) {
            assertEquals( ResultSet.CLOSE_CURSORS_AT_COMMIT, con.getHoldability() );
        }
    }


    @Test
    public void autocommitPropertyTest() throws SQLException {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "autocommit", "false" );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertFalse( con.getAutoCommit() );
        }
    }


    @Test
    public void readOnlyPropertyTest() throws SQLException {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "readonly", "true" );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertTrue( con.isReadOnly() );
        }
    }


    @Test
    public void isolationSerializablePropertyTest() {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "isolation", "SERIALIZABLE" );
        assertThrows( SQLException.class, () -> jdbcConnect( url, properties ) );
    }


    @Test
    public void isolationDirtyPropertyTest() {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "isolation", "DIRTY" );
        assertThrows( SQLException.class, () -> jdbcConnect( url, properties ) );
    }


    @Test
    public void isolationCommittedPropertyTest() throws SQLException {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "isolation", "COMMITTED" );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertEquals( Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation() );
        }
    }


    @Test
    public void isolationRepeatableReadPropertyTest() {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "isolation", "REPEATABLE_READ" );
        assertThrows( SQLException.class, () -> jdbcConnect( url, properties ) );
    }


    @Test
    public void networkTimeoutPropertyTest() throws SQLException {
        int expectedTimeout = 250;
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "nwtimeout", String.valueOf( expectedTimeout ) );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertEquals( expectedTimeout, con.getNetworkTimeout() );
        }
    }


    @Test
    public void resultHoldabilityHoldPropertyTest() {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "holdability", "HOLD" );
        assertThrows( SQLException.class, () -> jdbcConnect( url, properties ) );
    }


    @Test
    public void resultHoldabilityClosePropertyTest() throws SQLException {
        String url = "jdbc:polypheny://pa:@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "holdability", "CLOSE" );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertEquals( ResultSet.CLOSE_CURSORS_AT_COMMIT, con.getHoldability() );
        }
    }


    @Test
    public void isWrapperForTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            PolyConnection polyphenyConnection = jdbcConnection.getConnection().unwrap( PolyConnection.class );
        }
    }


    @Test
    public void unwrapTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            PolyConnection polyphenyConnection = jdbcConnection.getConnection().unwrap( PolyConnection.class );
        }
    }


    @Test
    public void isWrapperForFalseTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
        ) {
            assertFalse( connection.isWrapperFor( PolyphenyDb.class ) );
        }
    }


    @Test
    public void unwrapExceptionTest() throws SQLException {
        try (
                JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection();
        ) {
            assertThrows( SQLException.class, () -> connection.unwrap( PolyphenyDb.class ) );
        }
    }


    @Test
    public void illegalTimeoutBelowZeroTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            assertThrows( SQLException.class, () -> jdbcConnection.getConnection().setNetworkTimeout( null, -42 ) );
        }
    }


    @Test
    public void validTimeoutTest() throws SQLException {
        int timeout = 20000;
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setNetworkTimeout( null, timeout );
            assertEquals( timeout, connection.getNetworkTimeout() );
        }
    }


    @Test
    public void abortNotSupportedTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertThrows( SQLFeatureNotSupportedException.class, () -> connection.abort( null ) );
        }
    }


    @Test
    public void setInvalidNamespaceTest() {
        String namespaceName = "nonexistentNamespace";
        assertThrows( ProtoInterfaceServiceException.class, () -> {
            try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
                Connection connection = jdbcConnection.getConnection();
                connection.setSchema( namespaceName );
            }
        } );
    }


    @Test
    public void setValidNamespaceTest() throws SQLException {
        String namespaceName = "public";
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setSchema( namespaceName );
            assertEquals( namespaceName, connection.getSchema() );
        }
    }


    @Test
    public void createStructTest() throws SQLException {
        String typeName = "INT_STRING";
        Object[] objects = { 5, "Hi" };
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            Struct struct = connection.createStruct( typeName, objects );
            Object[] values = struct.getAttributes();
            for ( int i = 0; i < objects.length; i++ ) {
                assertEquals( objects[i], values[i] );
            }
            assertEquals( struct.getSQLTypeName(), typeName );
        }
    }


    @Test
    public void createArrayTest() throws SQLException {
        String typeName = "INTEGER";
        Object[] objects = { 5, -42 };
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            Array array = connection.createArrayOf( typeName, objects );
            if ( array.getArray() instanceof Integer[] ) {
                Integer[] values = (Integer[]) array.getArray();
                for ( int i = 0; i < objects.length; i++ ) {
                    assertEquals( objects[i], values[i] );
                }
            }
            assertEquals( array.getBaseTypeName(), typeName );
        }
    }


    @Test
    public void validClientInfoStringTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setClientInfo( "k1", "v1" );
            connection.setClientInfo( "k2", "v2" );
            assertEquals( "v1", connection.getClientInfo( "k1" ) );
            assertEquals( "v2", connection.getClientInfo( "k2" ) );
        }
    }


    @Test
    public void validClientInfoPropertiesTest() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty( "k1", "v1" );
        properties.setProperty( "k2", "v2" );
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setClientInfo( properties );
            assertEquals( "v1", connection.getClientInfo( "k1" ) );
            assertEquals( "v2", connection.getClientInfo( "k2" ) );
        }
    }


    @Test
    public void validClientInfoPropertiesGetterTest() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty( "k1", "v1" );
        properties.setProperty( "k2", "v2" );
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setClientInfo( properties );
            Properties info = connection.getClientInfo();
            assertEquals( properties.getProperty( "k1" ), info.getProperty( "k1" ) );
            assertEquals( properties.getProperty( "k2" ), info.getProperty( "k2" ) );
        }
    }


    @Test
    public void clientInfoPropertiesDefaultsTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertEquals( "", connection.getClientInfo( "ApplicationName" ) );
            assertEquals( "", connection.getClientInfo( "ApplicationVersionString" ) );
            assertEquals( "", connection.getClientInfo( "ClientHostname" ) );
            assertEquals( "", connection.getClientInfo( "ClientUser" ) );
        }
    }


    @Test
    public void invalidClientInfoPropertiesTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertNull( connection.getClientInfo( "notExistingKeys" ) );
        }
    }


    @Test
    public void connectionValidTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertTrue( connection.isValid( 120000 ) );
        }
    }


    @Test
    public void sqlxmlNotSupportedTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertThrows( SQLFeatureNotSupportedException.class, connection::createSQLXML );
        }
    }


    @Test
    public void createClobTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            Clob clob = connection.createClob();
            assertTrue( clob instanceof PolyphenyClob );
        }
    }


    @Test
    public void createBlobTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            Blob blob = connection.createBlob();
            assertTrue( blob instanceof PolyphenyBlob );
        }
    }


    @Test
    public void createNClobTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            NClob nclob = connection.createNClob();
            assertTrue( nclob instanceof PolyphenyClob );
        }
    }


    @Test
    public void prepareCallWithParamsNotSupportedTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertThrows( SQLFeatureNotSupportedException.class, () -> connection.prepareCall( "CALL testProcedure()" ) );
        }
    }


    @Test
    public void getTypeMapTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();

            Map<String, Class<?>> expectedTypeMap = new HashMap<>();
            expectedTypeMap.put( "MY_TYPE", String.class );

            connection.setTypeMap( expectedTypeMap );

            Map<String, Class<?>> retrievedTypeMap = connection.getTypeMap();
            assertEquals( expectedTypeMap, retrievedTypeMap );
        }
    }


    @Test
    public void getWarningsWhenOpenTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();

            SQLWarning warning = connection.getWarnings();
            assertNull( warning );
        }
    }


    @Test
    public void getWarningsWhenClosedTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.close();
            assertThrows( SQLException.class, connection::getWarnings );
        }
    }


    @Test
    public void clearWarningsWhenOpenTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.clearWarnings();
        }
    }


    @Test
    public void clearWarningsWhenClosedTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.close();
            assertThrows( SQLException.class, () -> connection.clearWarnings() );
        }
    }


    @Test
    public void setSavepointTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertThrows( SQLFeatureNotSupportedException.class, connection::setSavepoint );
        }
    }


    @Test
    public void setSavepointWithNameTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertThrows( SQLFeatureNotSupportedException.class, () -> connection.setSavepoint( "testSavepoint" ) );
        }
    }


    @Test
    public void rollbackWithSavepointTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            Savepoint savepoint = null;
            assertThrows( SQLFeatureNotSupportedException.class, () -> connection.rollback( savepoint ) );
        }
    }


    @Test
    public void releaseSavepointTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            Savepoint savepoint = null;
            assertThrows( SQLFeatureNotSupportedException.class, () -> connection.releaseSavepoint( savepoint ) );
        }
    }


    @Test
    public void setHoldabilityCloseTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try {
                connection.setHoldability( ResultSet.CLOSE_CURSORS_AT_COMMIT );
            } catch ( SQLException e ) {
                fail( "Exception should not be thrown when setting holdability to CLOSE_CURSORS_AT_COMMIT" );
            }
        }
    }


    @Test
    public void setHoldabilityHoldThrowsErrorTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertThrows( SQLException.class, () -> connection.setHoldability( ResultSet.HOLD_CURSORS_OVER_COMMIT ) );
        }
    }


    @Test
    public void setHoldabilityCloseOnClosedErrorTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.close();
            assertThrows( SQLException.class, () -> connection.setHoldability( ResultSet.CLOSE_CURSORS_AT_COMMIT ) );
        }
    }


    @Test
    public void setTransactionIsolationWithValidValueTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED );
        } catch ( SQLException e ) {
            fail( e.getMessage() );
        }
    }


    @Test
    public void setTransactionIsolationWithRepeatableReadTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertThrows( SQLException.class, () -> connection.setTransactionIsolation( Connection.TRANSACTION_REPEATABLE_READ ) );
        }
    }


    @Test
    public void setTransactionIsolationWithSerializableTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertThrows( SQLException.class, () -> connection.setTransactionIsolation( Connection.TRANSACTION_SERIALIZABLE ) );
        }
    }


    @Test
    public void setTransactionIsolationWithReadUncommittedTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertThrows( SQLException.class, () -> connection.setTransactionIsolation( Connection.TRANSACTION_READ_UNCOMMITTED ) );
        }
    }


    @Test
    public void setTransactionOnClosedTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.close();
            assertThrows( SQLException.class, () -> connection.setTransactionIsolation( Connection.TRANSACTION_REPEATABLE_READ ) );
        }
    }


    @Test
    public void setAndGetCatalogTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setCatalog( "TestCatalog" );
            assertEquals( "TestCatalog", connection.getCatalog() );
        }
    }


    @Test
    public void getCatalogWithoutSettingTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertNull( connection.getCatalog() );
        }
    }


    @Test
    public void setCatalogWhenClosedTest() throws SQLException {
        JdbcConnection jdbcConnection = new JdbcConnection( true );
        Connection connection = jdbcConnection.getConnection();
        connection.close();
        assertThrows( SQLException.class, () -> connection.setCatalog( "TestCatalog" ) );
    }


    @Test
    public void closeTest() throws SQLException {
        JdbcConnection jdbcConnection = new JdbcConnection( true );
        Connection connection = jdbcConnection.getConnection();
        assertFalse( connection.isClosed() );
        connection.close();
        assertTrue( connection.isClosed() );
    }


    @Test
    public void setAndCheckReadOnlyTrueTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setReadOnly( true );
            assertTrue( connection.isReadOnly() );
        }
    }


    @Test
    public void setAndCheckReadOnlyFalseTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();

            connection.setReadOnly( false );
            assertFalse( connection.isReadOnly() );
        }
    }


    @Test
    public void setReadOnlyWhenClosedTest() throws SQLException {
        JdbcConnection jdbcConnection = new JdbcConnection( true );
        Connection connection = jdbcConnection.getConnection();
        connection.close();
        assertThrows( SQLException.class, () -> connection.setReadOnly( true ) );
    }


    @Test
    public void isReadOnlyWhenClosedTest() throws SQLException {
        JdbcConnection jdbcConnection = new JdbcConnection( true );
        Connection connection = jdbcConnection.getConnection();
        connection.close();
        assertThrows( SQLException.class, connection::isReadOnly );
    }


    @Test
    public void nativeSQLNotSupportedTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertThrows( SQLFeatureNotSupportedException.class, () -> connection.nativeSQL( "SELECT * FROM table_name" ) );
        }
    }


    @Test
    public void commitTransactionTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.commit();
        }
    }


    @Test
    public void commitTransactionOnAutocommitTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true, true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertThrows( SQLException.class, connection::commit );
        }
    }


    @Test
    public void rollbackTransactionTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.rollback();
        }
    }


    @Test
    public void rollbackTransactionOnAutocommitTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true, true ) ) {
            Connection connection = jdbcConnection.getConnection();
            assertThrows( SQLException.class, connection::rollback );
        }
    }


    @Test
    public void commitWhenClosedTest() throws SQLException {
        JdbcConnection jdbcConnection = new JdbcConnection( true, true );
        Connection connection = jdbcConnection.getConnection();
        connection.close();
        assertThrows( SQLException.class, connection::commit );
    }


    @Test
    public void rollbackWhenClosedTest() throws SQLException {
        JdbcConnection jdbcConnection = new JdbcConnection( true, true );
        Connection connection = jdbcConnection.getConnection();
        connection.close();
        assertThrows( SQLException.class, connection::rollback );
    }


    @Test
    public void commitWithAutoCommitTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true, true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setAutoCommit( true );
            assertThrows( SQLException.class, connection::commit );
        }
    }


    @Test
    public void rollbackWithAutoCommitTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true, true ) ) {
            Connection connection = jdbcConnection.getConnection();
            connection.setAutoCommit( true );
            assertThrows( SQLException.class, connection::rollback );
        }
    }

}

