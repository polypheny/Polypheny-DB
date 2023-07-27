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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;

@Slf4j
public class JdbcConnectionTest {

    private final static String dbHost = "localhost";
    private final static int port = 20590;

    @BeforeClass
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
    public void autocommitUrlTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port + "/public?autocommit=false";
        try ( Connection con = jdbcConnect( url ) ) {
            assertFalse( con.getAutoCommit() );
        }
    }


    @Test
    public void readOnlyUrlTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port + "/public?readonly=true";
        try ( Connection con = jdbcConnect( url ) ) {
            assertTrue( con.isReadOnly() );
        }
    }


    @Test
    public void isolationSerializableUrlTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port + "/public?isolation=SERIALIZABLE";
        try ( Connection con = jdbcConnect( url ) ) {
            assertEquals( Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation() );
        }
    }


    @Test
    public void isolationDirtyUrlTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port + "/public?isolation=DIRTY";
        try ( Connection con = jdbcConnect( url ) ) {
            assertEquals( Connection.TRANSACTION_READ_UNCOMMITTED, con.getTransactionIsolation() );
        }
    }


    @Test
    public void isolationCommittedUrlTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port + "/public?isolation=COMMITTED";
        try ( Connection con = jdbcConnect( url ) ) {
            assertEquals( Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation() );
        }
    }


    @Test
    public void isolationRepeatableReadUrlTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port + "/public?isolation=REPEATABLE_READ";
        try ( Connection con = jdbcConnect( url ) ) {
            assertEquals( Connection.TRANSACTION_REPEATABLE_READ, con.getTransactionIsolation() );
        }
    }


    @Test
    public void networkTimeoutUrlTest() throws SQLException {
        int expectedTimeout = 250;
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port + "/public?nwtimeout=250" + expectedTimeout;
        try ( Connection con = jdbcConnect( url ) ) {
            assertEquals( expectedTimeout, con.getNetworkTimeout() );
        }
    }


    @Test
    public void resultHoldabilityHoldUrlTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port + "/public?holdability=HOLD";
        try ( Connection con = jdbcConnect( url ) ) {
            assertEquals( ResultSet.HOLD_CURSORS_OVER_COMMIT, con.getHoldability() );
        }
    }


    @Test
    public void resultHoldabilityCloseUrlTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port + "/public?holdability=CLOSE";
        try ( Connection con = jdbcConnect( url ) ) {
            assertEquals( ResultSet.CLOSE_CURSORS_AT_COMMIT, con.getHoldability() );
        }
    }


    @Test
    public void autocommitPropertyTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "autocommit", "false" );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertFalse( con.getAutoCommit() );
        }
    }


    @Test
    public void readOnlyPropertyTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "readonly", "true" );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertTrue( con.isReadOnly() );
        }
    }


    @Test
    public void isolationSerializablePropertyTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "isolation", "SERIALIZABLE" );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertEquals( Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation() );
        }
    }


    @Test
    public void isolationDirtyPropertyTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "isolation", "DIRTY" );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertEquals( Connection.TRANSACTION_READ_UNCOMMITTED, con.getTransactionIsolation() );
        }
    }


    @Test
    public void isolationCommittedPropertyTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "isolation", "COMMITTED" );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertEquals( Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation() );
        }
    }


    @Test
    public void isolationRepeatableReadPropertyTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "isolation", "REPEATABLE_READ" );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertEquals( Connection.TRANSACTION_REPEATABLE_READ, con.getTransactionIsolation() );
        }
    }


    @Test
    public void networkTimeoutPropertyTest() throws SQLException {
        int expectedTimeout = 250;
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "nwtimeout", String.valueOf( expectedTimeout ) );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertEquals( expectedTimeout, con.getNetworkTimeout() );
        }
    }


    @Test
    public void resultHoldabilityHoldPropertyTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "holdability", "HOLD" );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertEquals( ResultSet.HOLD_CURSORS_OVER_COMMIT, con.getHoldability() );
        }
    }


    @Test
    public void resultHoldabilityClosePropertyTest() throws SQLException {
        String url = "jdbc:polypheny://pa:pa@" + dbHost + ":" + port;
        Properties properties = new Properties();
        properties.setProperty( "holdability", "CLOSE" );
        try ( Connection con = jdbcConnect( url, properties ) ) {
            assertEquals( ResultSet.CLOSE_CURSORS_AT_COMMIT, con.getHoldability() );
        }
    }

}
