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


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.type.PolyType;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class JdbcDdlTest {


    private final static String DDLTEST_SQL = "CREATE TABLE ddltest( "
            + "tbigint BIGINT NOT NULL, "
            + "tboolean BOOLEAN NOT NULL, "
            + "tdate DATE NOT NULL, "
            + "tdecimal DECIMAL(5,2) NOT NULL, "
            + "tdouble DOUBLE NOT NULL, "
            + "tinteger INTEGER NOT NULL, "
            + "treal REAL NOT NULL, "
            + "tsmallint SMALLINT NOT NULL, "
            + "ttime TIME NOT NULL, "
            + "ttimestamp TIMESTAMP NOT NULL, "
            + "ttinyint TINYINT NOT NULL, "
            + "tvarchar VARCHAR(20) NOT NULL, "
            + "PRIMARY KEY (tinteger) )";

    private final static String DDLTEST_DATA_SQL = "INSERT INTO ddltest VALUES ("
            + "1234,"
            + "true,"
            + "date '2020-07-03',"
            + "123.45,"
            + "1.999999,"
            + "9876,"
            + "0.3333,"
            + "45,"
            + "time '11:59:32',"
            + "timestamp '2021-01-01 10:11:15',"
            + "22,"
            + "'hallo'"
            + ")";

    private final static Object[] DDLTEST_DATA = new Object[]{
            1234L,
            true,
            Date.valueOf( "2020-07-03" ),
            new BigDecimal( "123.45" ),
            1.999999,
            9876,
            0.3333f,
            (short) 45,
            Time.valueOf( "11:59:32" ),
            Timestamp.valueOf( "2021-01-01 10:11:15" ),
            (byte) 22,
            "hallo" };


    @BeforeAll
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void testTypes() throws SQLException {
        // Check if there are new types missing in this test
        assertEquals( 18, PolyType.allowedFieldTypes().size(), "Unexpected number of available types" );

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( DDLTEST_SQL );
                statement.executeUpdate( DDLTEST_DATA_SQL );

                try {
                    // Checks
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM ddltest" ),
                            ImmutableList.of( DDLTEST_DATA ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tbigint FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[0] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tboolean FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[1] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tdate FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[2] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tdecimal FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[3] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tdouble FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[4] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tinteger FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[5] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT treal FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[6] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tsmallint FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[7] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ttime FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[8] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ttimestamp FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[9] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ttinyint FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[10] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tvarchar FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[11] } ) );

                    connection.commit();
                } finally {
                    // Drop ddltest table
                    statement.executeUpdate( "DROP TABLE ddltest" );
                }
            }
        }
    }


    @Test
    public void testDateType() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( DDLTEST_SQL );
                statement.executeUpdate( DDLTEST_DATA_SQL );

                try {
                    // Checks
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tdate FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[2] } ) );

                    connection.commit();
                } finally {
                    // Drop ddltest table
                    statement.executeUpdate( "DROP TABLE ddltest" );
                }
            }
        }
    }


    @Test
    public void testTimestampType() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( DDLTEST_SQL );
                statement.executeUpdate( DDLTEST_DATA_SQL );

                try {
                    // Checks
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ttimestamp FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[9] } ) );

                    connection.commit();
                } finally {
                    // Drop ddltest table
                    statement.executeUpdate( "DROP TABLE ddltest" );
                }
            }
        }
    }


    @Test
    public void viewTestTypes() throws SQLException {
        // Check if there are new types missing in this test
        assertEquals( 18, PolyType.allowedFieldTypes().size(), "Unexpected number of available types" );

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( DDLTEST_SQL );
                statement.executeUpdate( DDLTEST_DATA_SQL );
                statement.executeUpdate( "CREATE VIEW ddltestview as SELECT * FROM ddltest" );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM ddltestview" ),
                            ImmutableList.of( DDLTEST_DATA ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tbigint FROM ddltestview" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[0] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tboolean FROM ddltestview" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[1] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tdate FROM ddltestview" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[2] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tdecimal FROM ddltestview" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[3] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tdouble FROM ddltestview" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[4] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tinteger FROM ddltestview" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[5] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT treal FROM ddltestview" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[6] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tsmallint FROM ddltestview" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[7] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ttime FROM ddltestview" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[8] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ttimestamp FROM ddltestview" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[9] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ttinyint FROM ddltestview" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[10] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tvarchar FROM ddltestview" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[11] } ) );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP VIEW ddltestview" );
                    statement.executeUpdate( "DROP TABLE ddltest" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void materializedTestTime() throws SQLException {
        // Check if there are new types missing in this test
        assertEquals( 18, PolyType.allowedFieldTypes().size(), "Unexpected number of available types" );

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( "CREATE TABLE ddltest( "
                        + "ttime TIME NOT NULL, "
                        + "PRIMARY KEY (ttime) )" );
                statement.executeUpdate( "INSERT INTO ddltest VALUES ("
                        + "time '11:59:32'"
                        + ")" );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW ddltestMaterialized as SELECT * FROM ddltest FRESHNESS MANUAL" );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ttime FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[8] } ) );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW ddltestMaterialized" );
                    statement.executeUpdate( "DROP TABLE ddltest" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void materializedTestTimeStamp() throws SQLException {
        // Check if there are new types missing in this test
        assertEquals( 18, PolyType.allowedFieldTypes().size(), "Unexpected number of available types" );

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( "CREATE TABLE ddltest( "
                        + "ttimestamp TIMESTAMP NOT NULL, "
                        + "PRIMARY KEY (ttimestamp))" );
                statement.executeUpdate( "INSERT INTO ddltest VALUES ("
                        + "timestamp '2021-01-01 10:11:15'"
                        + ")" );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW ddltestMaterialized as SELECT * FROM ddltest FRESHNESS MANUAL" );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ttimestamp FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[9] } ) );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW ddltestMaterialized" );
                    statement.executeUpdate( "DROP TABLE ddltest" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void materializedTestTypes() throws SQLException {
        // Check if there are new types missing in this test
        assertEquals( 18, PolyType.allowedFieldTypes().size(), "Unexpected number of available types" );

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( DDLTEST_SQL );
                statement.executeUpdate( DDLTEST_DATA_SQL );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW ddltestMaterialized as SELECT * FROM ddltest FRESHNESS MANUAL" );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tbigint FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[0] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tboolean FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[1] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tdate FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[2] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tdecimal FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[3] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tdouble FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[4] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tinteger FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[5] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT treal FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[6] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tsmallint FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[7] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ttime FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[8] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ttimestamp FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[9] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ttinyint FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[10] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tvarchar FROM ddltestMaterialized" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[11] } ) );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW ddltestMaterialized" );
                    statement.executeUpdate( "DROP TABLE ddltest" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void nullTest() throws SQLException {
        // Check if there are new types missing in this test
        assertEquals( 18, PolyType.allowedFieldTypes().size(), "Unexpected number of available types" );

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( "CREATE TABLE ddltest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tbigint BIGINT NULL, "
                        + "tboolean BOOLEAN NULL, "
                        + "tdate DATE NULL, "
                        + "tdecimal DECIMAL(5,2) NULL, "
                        + "tdouble DOUBLE NULL, "
                        + "tinteger INTEGER NULL, "
                        + "treal REAL NULL, "
                        + "tsmallint SMALLINT NULL, "
                        + "ttime TIME NULL, "
                        + "ttimestamp TIMESTAMP NULL, "
                        + "ttinyint TINYINT NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "tfile FILE NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    statement.executeUpdate( "INSERT INTO ddltest(tprimary) VALUES (1)" );
                    statement.executeUpdate( "INSERT INTO ddltest(tprimary) VALUES (2, null, null, null, null, null, null, null, null, null, null, null, null)" );
                    // Checks
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM ddltest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, null, null, null, null, null, null, null, null, null, null, null, null, null },
                                    new Object[]{ 2, null, null, null, null, null, null, null, null, null, null, null, null, null } ) );
                } finally {
                    // Drop ddltest table
                    statement.executeUpdate( "DROP TABLE ddltest" );
                }
            }
        }
    }


    @Test
    public void notNullTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table
                statement.executeUpdate( DDLTEST_SQL );
                try {
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "INSERT INTO ddltest(tprimary) VALUES ( null, null, null, null, null, 1, null, null, null, null, null )" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    assertTrue( failed );
                } finally {
                    // Drop ddltest table
                    statement.executeUpdate( "DROP TABLE ddltest" );
                }
            }
        }
    }


    @Test
    public void renameColumnTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( DDLTEST_SQL );
                statement.executeUpdate( DDLTEST_DATA_SQL );

                try {
                    // Rename columns
                    statement.executeUpdate( "ALTER TABLE ddltest RENAME COLUMN tbigint TO rtbigint" );
                    statement.executeUpdate( "ALTER TABLE ddltest RENAME COLUMN tboolean TO rtboolean" );
                    statement.executeUpdate( "ALTER TABLE ddltest RENAME COLUMN tdate TO rtdate" );
                    statement.executeUpdate( "ALTER TABLE ddltest RENAME COLUMN tdecimal TO rtdecimal" );
                    statement.executeUpdate( "ALTER TABLE ddltest RENAME COLUMN tdouble TO rtdouble" );
                    statement.executeUpdate( "ALTER TABLE ddltest RENAME COLUMN tinteger TO rtinteger" );
                    statement.executeUpdate( "ALTER TABLE ddltest RENAME COLUMN treal TO rtreal" );
                    statement.executeUpdate( "ALTER TABLE ddltest RENAME COLUMN tsmallint TO rtsmallint" );
                    statement.executeUpdate( "ALTER TABLE ddltest RENAME COLUMN ttime TO rttime" );
                    statement.executeUpdate( "ALTER TABLE ddltest RENAME COLUMN ttimestamp TO rttimestamp" );
                    statement.executeUpdate( "ALTER TABLE ddltest RENAME COLUMN ttinyint TO rttinyint" );
                    statement.executeUpdate( "ALTER TABLE ddltest RENAME COLUMN tvarchar TO rtvarchar" );

                    // Checks
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT rtbigint FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[0] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT rtboolean FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[1] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT rtdate FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[2] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT rtdecimal FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[3] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT rtdouble FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[4] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT rtinteger FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[5] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT rtreal FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[6] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT rtsmallint FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[7] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT rttime FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[8] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT rttimestamp FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[9] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT rttinyint FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[10] } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT rtvarchar FROM ddltest" ),
                            ImmutableList.of( new Object[]{ DDLTEST_DATA[11] } ) );
                } finally {
                    // Drop ddltest table
                    statement.executeUpdate( "DROP TABLE ddltest" );
                }
            }
        }
    }


    @Test
    public void renameTableTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( DDLTEST_SQL );
                statement.executeUpdate( DDLTEST_DATA_SQL );

                try {
                    // Rename table
                    statement.executeUpdate( "ALTER TABLE ddltest RENAME TO foobartest" );

                    // Check
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM foobartest" ),
                            ImmutableList.of( DDLTEST_DATA ) );
                } finally {
                    // Drop ddltest table
                    statement.executeUpdate( "DROP TABLE foobartest" );
                }
            }
        }
    }


    @Test
    public void dropColumnTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( DDLTEST_SQL );
                statement.executeUpdate( DDLTEST_DATA_SQL );

                try {
                    // Rename table
                    statement.executeUpdate( "ALTER TABLE ddltest DROP COLUMN tbigint" );
                    statement.executeUpdate( "ALTER TABLE ddltest DROP COLUMN tdate" );
                    statement.executeUpdate( "ALTER TABLE ddltest DROP COLUMN tvarchar" );

                    // Check
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM ddltest" ),
                            ImmutableList.of( new Object[]{
                                    DDLTEST_DATA[1],
                                    DDLTEST_DATA[3],
                                    DDLTEST_DATA[4],
                                    DDLTEST_DATA[5],
                                    DDLTEST_DATA[6],
                                    DDLTEST_DATA[7],
                                    DDLTEST_DATA[8],
                                    DDLTEST_DATA[9],
                                    DDLTEST_DATA[10],
                            } ) );
                } finally {
                    // Drop ddltest table
                    statement.executeUpdate( "DROP TABLE ddltest" );
                }
            }
        }
    }


    @Test
    public void addColumnTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( DDLTEST_SQL );
                statement.executeUpdate( DDLTEST_DATA_SQL );

                try {
                    // Add columns
                    statement.executeUpdate( "ALTER TABLE ddltest ADD COLUMN foo1 INTEGER NOT NULL DEFAULT 5 BEFORE tbigint" );
                    statement.executeUpdate( "ALTER TABLE ddltest ADD COLUMN foo2 VARCHAR(10) NULL AFTER tinteger" );
                    statement.executeUpdate( "ALTER TABLE ddltest ADD COLUMN foo3 BOOLEAN NOT NULL DEFAULT false AFTER tvarchar" );

                    // Check
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM ddltest" ),
                            ImmutableList.of( new Object[]{
                                    5,
                                    DDLTEST_DATA[0],
                                    DDLTEST_DATA[1],
                                    DDLTEST_DATA[2],
                                    DDLTEST_DATA[3],
                                    DDLTEST_DATA[4],
                                    DDLTEST_DATA[5],
                                    null,
                                    DDLTEST_DATA[6],
                                    DDLTEST_DATA[7],
                                    DDLTEST_DATA[8],
                                    DDLTEST_DATA[9],
                                    DDLTEST_DATA[10],
                                    DDLTEST_DATA[11],
                                    false
                            } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT foo1 FROM ddltest" ),
                            ImmutableList.of( new Object[]{ 5 } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT foo2 FROM ddltest" ),
                            ImmutableList.of( new Object[]{ null } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT foo3 FROM ddltest" ),
                            ImmutableList.of( new Object[]{ false } ) );
                } finally {
                    // Drop ddltest table
                    statement.executeUpdate( "DROP TABLE ddltest" );
                }
            }
        }
    }


    @Test
    public void addColumnArrayTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( DDLTEST_SQL );
                statement.executeUpdate( DDLTEST_DATA_SQL );

                try {
                    // Add column
                    statement.executeUpdate( "ALTER TABLE ddltest ADD COLUMN bar INTEGER ARRAY(1,3) NULL AFTER ttime" );

                    // Check
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM ddltest" ),
                            ImmutableList.of( new Object[]{
                                    DDLTEST_DATA[0],
                                    DDLTEST_DATA[1],
                                    DDLTEST_DATA[2],
                                    DDLTEST_DATA[3],
                                    DDLTEST_DATA[4],
                                    DDLTEST_DATA[5],
                                    DDLTEST_DATA[6],
                                    DDLTEST_DATA[7],
                                    DDLTEST_DATA[8],
                                    null,
                                    DDLTEST_DATA[9],
                                    DDLTEST_DATA[10],
                                    DDLTEST_DATA[11],
                            } ) );
                } finally {
                    // Drop ddltest table
                    statement.executeUpdate( "DROP TABLE ddltest" );
                }
            }
        }
    }


    @Test
    @Tag("fileExcluded") // we have to add convert methods for all PolyValues, then we can enable
    public void changeColumnTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( DDLTEST_SQL );
                statement.executeUpdate( DDLTEST_DATA_SQL );

                try {
                    // BigInt --> Integer
                    statement.executeUpdate( "ALTER TABLE ddltest MODIFY COLUMN tbigint SET TYPE INTEGER" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tbigint FROM ddltest" ),
                            ImmutableList.of( new Object[]{ ((Long) DDLTEST_DATA[0]).intValue() } ) );

                    // Decimal --> Double
                    statement.executeUpdate( "ALTER TABLE ddltest MODIFY COLUMN tdecimal SET TYPE DOUBLE" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tdecimal FROM ddltest" ),
                            ImmutableList.of( new Object[]{ ((BigDecimal) DDLTEST_DATA[3]).doubleValue() } ) );

                    // Double --> DECIMAL
                    statement.executeUpdate( "ALTER TABLE ddltest MODIFY COLUMN tdouble SET TYPE DECIMAL(15,6)" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tdouble FROM ddltest" ),
                            ImmutableList.of( new Object[]{ BigDecimal.valueOf( (double) DDLTEST_DATA[4] ) } ),
                            false,
                            true );

                    // Real --> Double
                    statement.executeUpdate( "ALTER TABLE ddltest MODIFY COLUMN treal SET TYPE DOUBLE" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT treal FROM ddltest" ),
                            ImmutableList.of( new Object[]{ Double.parseDouble( Float.valueOf( (float) DDLTEST_DATA[6] ).toString() ) } ) );

                    // SmallInt --> Integer
                    statement.executeUpdate( "ALTER TABLE ddltest MODIFY COLUMN tsmallint SET TYPE INTEGER" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tsmallint FROM ddltest" ),
                            ImmutableList.of( new Object[]{ (int) (short) DDLTEST_DATA[7] } ) );

                    // TinyInt --> SmallInt
                    statement.executeUpdate( "ALTER TABLE ddltest MODIFY COLUMN ttinyint SET TYPE SMALLINT" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ttinyint FROM ddltest" ),
                            ImmutableList.of( new Object[]{ (short) (byte) DDLTEST_DATA[10] } ) );
                } finally {
                    // Drop ddltest table
                    statement.executeUpdate( "DROP TABLE ddltest" );
                }
            }
        }
    }


    @Test
    public void testReorderColumns() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( DDLTEST_SQL );
                statement.executeUpdate( DDLTEST_DATA_SQL );

                try {
                    // Reorder columns
                    statement.executeUpdate( "ALTER TABLE ddltest MODIFY COLUMN tbigint SET POSITION AFTER tboolean" );
                    statement.executeUpdate( "ALTER TABLE ddltest MODIFY COLUMN tdecimal SET POSITION BEFORE tdate" );
                    statement.executeUpdate( "ALTER TABLE \"ddltest\" MODIFY COLUMN \"ttinyint\" SET POSITION AFTER \"tvarchar\"" );

                    // Check
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ddltest.* FROM ddltest" ),
                            ImmutableList.of( new Object[]{
                                    DDLTEST_DATA[1],
                                    DDLTEST_DATA[0],
                                    DDLTEST_DATA[3],
                                    DDLTEST_DATA[2],
                                    DDLTEST_DATA[4],
                                    DDLTEST_DATA[5],
                                    DDLTEST_DATA[6],
                                    DDLTEST_DATA[7],
                                    DDLTEST_DATA[8],
                                    DDLTEST_DATA[9],
                                    DDLTEST_DATA[11],
                                    DDLTEST_DATA[10],
                            } ) );
                } finally {
                    // Drop ddltest table
                    statement.executeUpdate( "DROP TABLE ddltest" );
                }
            }
        }
    }


    @Test
    public void testTruncate() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create ddltest table and insert data
                statement.executeUpdate( DDLTEST_SQL );
                statement.executeUpdate( DDLTEST_DATA_SQL );

                try {
                    // Check
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ddltest.* FROM ddltest" ),
                            ImmutableList.of( new Object[]{
                                    DDLTEST_DATA[0],
                                    DDLTEST_DATA[1],
                                    DDLTEST_DATA[2],
                                    DDLTEST_DATA[3],
                                    DDLTEST_DATA[4],
                                    DDLTEST_DATA[5],
                                    DDLTEST_DATA[6],
                                    DDLTEST_DATA[7],
                                    DDLTEST_DATA[8],
                                    DDLTEST_DATA[9],
                                    DDLTEST_DATA[10],
                                    DDLTEST_DATA[11],
                            } ) );

                    // Truncate
                    statement.executeUpdate( "TRUNCATE TABLE ddltest" );

                    // Check
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT ddltest.* FROM ddltest" ),
                            ImmutableList.of() );
                } finally {
                    // Drop ddltest table
                    statement.executeUpdate( "DROP TABLE ddltest" );
                }
            }
        }
    }


    @Test
    public void testExists() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE ddlexiststest(id integer not null, primary key(id))" );
                statement.executeUpdate( "CREATE TABLE IF NOT EXISTS ddlexiststest(id integer not null, primary key(id))" );
                statement.executeUpdate( "DROP TABLE ddlexiststest" );
                statement.executeUpdate( "DROP TABLE IF EXISTS ddlexiststest" );

                statement.executeUpdate( "CREATE NAMESPACE ddlexiststest" );
                statement.executeUpdate( "CREATE NAMESPACE IF NOT EXISTS ddlexiststest" );
                statement.executeUpdate( "DROP NAMESPACE ddlexiststest" );
                statement.executeUpdate( "DROP NAMESPACE IF EXISTS ddlexiststest" );

                // There should be aliases to use the SQL term SCHEMA instead of NAMESPACE
                statement.executeUpdate( "CREATE SCHEMA ddlexiststest" );
                statement.executeUpdate( "CREATE SCHEMA IF NOT EXISTS ddlexiststest" );
                statement.executeUpdate( "DROP SCHEMA ddlexiststest" );
                statement.executeUpdate( "DROP SCHEMA IF EXISTS ddlexiststest" );
            }
        }
    }


}
