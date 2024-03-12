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

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.util.ArrayFactoryImpl;
import org.apache.calcite.avatica.util.Unsafe;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Tag("adapter")
public class JdbcPreparedStatementsTest {


    private final static String SCHEMA_SQL = "CREATE TABLE pstest( "
            + "tbigint BIGINT NULL, "
            + "tboolean BOOLEAN NULL, "
            + "tdate DATE NULL, "
            + "tdecimal DECIMAL(5,2) NULL, "
            + "tdouble DOUBLE NULL, "
            + "tinteger INTEGER NOT NULL, "
            + "treal REAL NULL, "
            + "tsmallint SMALLINT NULL, "
            + "ttime TIME NULL, "
            + "ttimestamp TIMESTAMP NULL, "
            + "ttinyint TINYINT NULL, "
            + "tvarchar VARCHAR(20) NOT NULL, "
            + "PRIMARY KEY (tinteger) )";

    private final static Object[] TEST_DATA = new Object[]{
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
    public void basicTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( SCHEMA_SQL );

                try {
                    PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO pstest(tinteger,tvarchar) VALUES (?, ?)" );

                    preparedInsert.setInt( 1, 1 );
                    preparedInsert.setString( 2, "Foo" );
                    preparedInsert.execute();

                    preparedInsert.setInt( 1, 2 );
                    preparedInsert.setString( 2, "Bar" );
                    preparedInsert.execute();

                    PreparedStatement preparedSelect = connection.prepareStatement( "SELECT tinteger,tvarchar FROM pstest WHERE tinteger = ?" );
                    preparedSelect.setInt( 1, 1 );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of( new Object[]{ 1, "Foo" } ) );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE pstest" );
                }
            }
        }
    }


    @Test
    public void nullValueTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( SCHEMA_SQL );

                try {
                    PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO pstest(tinteger,tbigint,tvarchar) VALUES (?, ?, ?)" );

                    preparedInsert.setInt( 1, 1 );
                    preparedInsert.setNull( 2, SqlType.BIGINT.id );
                    preparedInsert.setString( 3, "Alice" );
                    preparedInsert.execute();

                    preparedInsert.setInt( 1, 2 );
                    preparedInsert.setNull( 2, SqlType.BIGINT.id );
                    preparedInsert.setString( 3, "Bob" );
                    preparedInsert.execute();

                    PreparedStatement preparedSelect = connection.prepareStatement( "SELECT tinteger,tbigint,tvarchar FROM pstest WHERE tinteger = ?" );
                    preparedSelect.setInt( 1, 1 );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of( new Object[]{ 1, null, "Alice" } ) );

                    boolean exceptionThrown = false;
                    try {
                        PreparedStatement preparedInsert2 = connection.prepareStatement( "INSERT INTO pstest(tinteger,tvarchar) VALUES (?, ?)" );

                        preparedInsert2.setInt( 1, 3 );
                        preparedInsert2.setNull( 2, SqlType.VARCHAR.id );
                        preparedInsert2.execute();

                        preparedInsert2.setInt( 1, 4 );
                        preparedInsert2.setNull( 2, SqlType.VARCHAR.id );
                        preparedInsert2.execute();
                    } catch ( Exception e ) {
                        exceptionThrown = true;
                    }

                    assertTrue( exceptionThrown, "Excepted null value for a non-nullable column" );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE pstest" );
                }
            }
        }
    }


    @Test
    public void batchInsertTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( SCHEMA_SQL );

                try {
                    PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO pstest(tinteger,tvarchar) VALUES (?, ?)" );

                    preparedInsert.setInt( 1, 1 );
                    preparedInsert.setString( 2, "Foo" );
                    preparedInsert.addBatch();

                    preparedInsert.setInt( 1, 2 );
                    preparedInsert.setString( 2, "Bar" );
                    preparedInsert.addBatch();

                    preparedInsert.executeBatch();
                    connection.commit();

                    PreparedStatement preparedSelect = connection.prepareStatement( "SELECT tinteger,tvarchar FROM pstest WHERE tinteger >= ? ORDER BY tinteger" );
                    preparedSelect.setInt( 1, 1 );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of(
                                    new Object[]{ 1, "Foo" },
                                    new Object[]{ 2, "Bar" } ),
                            true );

                } finally {
                    statement.executeUpdate( "DROP TABLE pstest" );
                }
            }
        }
    }


    @Test
    public void batchInsertDefaultValuesTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE pstest(tinteger integer not null, tvarchar varchar(29) default 'hans', primary key(tinteger) )" );

                try {
                    PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO pstest(tinteger) VALUES (?)" );

                    preparedInsert.setInt( 1, 1 );
                    preparedInsert.addBatch();

                    preparedInsert.setInt( 1, 2 );
                    preparedInsert.addBatch();

                    preparedInsert.executeBatch();
                    connection.commit();

                    PreparedStatement preparedSelect = connection.prepareStatement( "SELECT tinteger,tvarchar FROM pstest ORDER BY tinteger" );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of(
                                    new Object[]{ 1, "hans" },
                                    new Object[]{ 2, "hans" } ),
                            true );
                } finally {
                    statement.executeUpdate( "DROP TABLE pstest" );
                }
            }
        }
    }


    @Test
    public void dataTypesTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( SCHEMA_SQL );

                try {
                    PreparedStatement preparedInsert = connection.prepareStatement(
                            "INSERT INTO pstest VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" );

                    preparedInsert.setLong( 1, (long) TEST_DATA[0] );
                    preparedInsert.setBoolean( 2, (boolean) TEST_DATA[1] );
                    preparedInsert.setDate( 3, (Date) TEST_DATA[2] );
                    preparedInsert.setBigDecimal( 4, (BigDecimal) TEST_DATA[3] );
                    preparedInsert.setDouble( 5, (double) TEST_DATA[4] );
                    preparedInsert.setInt( 6, (int) TEST_DATA[5] );
                    preparedInsert.setFloat( 7, (float) TEST_DATA[6] );
                    preparedInsert.setShort( 8, (short) TEST_DATA[7] );
                    preparedInsert.setTime( 9, (Time) TEST_DATA[8] );
                    preparedInsert.setTimestamp( 10, (Timestamp) TEST_DATA[9] );
                    preparedInsert.setByte( 11, (byte) TEST_DATA[10] );
                    preparedInsert.setString( 12, (String) TEST_DATA[11] );
                    preparedInsert.execute();

                    connection.commit();

                    PreparedStatement preparedSelect = connection.prepareStatement(
                            "SELECT * FROM pstest WHERE "
                                    + "tbigint = ? AND "
                                    + "tboolean = ? AND "
                                    + "tdate = ? AND "
                                    + "tdecimal = ? AND "
                                    + "tdouble = ? AND "
                                    + "tinteger = ? AND "
                                    + "treal = ? AND "
                                    + "tsmallint = ? AND "
                                    + "ttime = ? AND "
                                    + "ttimestamp = ? AND "
                                    + "ttinyint = ? AND "
                                    + "tvarchar = ?" );
                    preparedSelect.setLong( 1, (long) TEST_DATA[0] );
                    preparedSelect.setBoolean( 2, (boolean) TEST_DATA[1] );
                    preparedSelect.setDate( 3, (Date) TEST_DATA[2] );
                    preparedSelect.setBigDecimal( 4, (BigDecimal) TEST_DATA[3] );
                    preparedSelect.setDouble( 5, (double) TEST_DATA[4] );
                    preparedSelect.setInt( 6, (int) TEST_DATA[5] );
                    preparedSelect.setFloat( 7, (float) TEST_DATA[6] );
                    preparedSelect.setShort( 8, (short) TEST_DATA[7] );
                    preparedSelect.setTime( 9, (Time) TEST_DATA[8] );
                    preparedSelect.setTimestamp( 10, (Timestamp) TEST_DATA[9] );
                    preparedSelect.setByte( 11, (byte) TEST_DATA[10] );
                    preparedSelect.setString( 12, (String) TEST_DATA[11] );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of( TEST_DATA ) );

                } finally {
                    statement.executeUpdate( "DROP TABLE pstest" );
                }
            }
        }
    }


    @Test
    public void batchDataTypesTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( SCHEMA_SQL );

                try {
                    PreparedStatement preparedInsert = connection.prepareStatement(
                            "INSERT INTO pstest VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" );

                    preparedInsert.setLong( 1, (long) TEST_DATA[0] );
                    preparedInsert.setBoolean( 2, (boolean) TEST_DATA[1] );
                    preparedInsert.setDate( 3, (Date) TEST_DATA[2] );
                    preparedInsert.setBigDecimal( 4, (BigDecimal) TEST_DATA[3] );
                    preparedInsert.setDouble( 5, (double) TEST_DATA[4] );
                    preparedInsert.setInt( 6, (int) TEST_DATA[5] );
                    preparedInsert.setFloat( 7, (float) TEST_DATA[6] );
                    preparedInsert.setShort( 8, (short) TEST_DATA[7] );
                    preparedInsert.setTime( 9, (Time) TEST_DATA[8] );
                    preparedInsert.setTimestamp( 10, (Timestamp) TEST_DATA[9] );
                    preparedInsert.setByte( 11, (byte) TEST_DATA[10] );
                    preparedInsert.setString( 12, (String) TEST_DATA[11] );
                    preparedInsert.addBatch();

                    preparedInsert.executeBatch();
                    connection.commit();

                    PreparedStatement preparedSelect = connection.prepareStatement(
                            "SELECT * FROM pstest WHERE "
                            + "tbigint = ? AND "
                            + "tboolean = ? AND "
                            + "tdate = ? AND "
                            + "tdecimal = ? AND "
                            + "tdouble = ? AND "
                            + "tinteger = ? AND "
                            + "treal = ? AND "
                            + "tsmallint = ? AND "
                            + "ttime = ? AND "
                            + "ttimestamp = ? AND "
                            + "ttinyint = ? AND "
                            + "tvarchar = ?" );
                    preparedSelect.setLong( 1, (long) TEST_DATA[0] );
                    preparedSelect.setBoolean( 2, (boolean) TEST_DATA[1] );
                    preparedSelect.setDate( 3, (Date) TEST_DATA[2] );
                    preparedSelect.setBigDecimal( 4, (BigDecimal) TEST_DATA[3] );
                    preparedSelect.setDouble( 5, (double) TEST_DATA[4] );
                    preparedSelect.setInt( 6, (int) TEST_DATA[5] );
                    preparedSelect.setFloat( 7, (float) TEST_DATA[6] );
                    preparedSelect.setShort( 8, (short) TEST_DATA[7] );
                    preparedSelect.setTime( 9, (Time) TEST_DATA[8] );
                    preparedSelect.setTimestamp( 10, (Timestamp) TEST_DATA[9] );
                    preparedSelect.setByte( 11, (byte) TEST_DATA[10] );
                    preparedSelect.setString( 12, (String) TEST_DATA[11] );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of( TEST_DATA ) );

                } finally {
                    statement.executeUpdate( "DROP TABLE pstest" );
                }
            }
        }
    }


    @Test
    public void nullTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( SCHEMA_SQL );

                try {
                    PreparedStatement preparedInsert = connection.prepareStatement(
                            "INSERT INTO pstest VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" );
                    PreparedStatement preparedSelect = connection.prepareStatement(
                            "SELECT * FROM pstest WHERE tinteger = ?" );

                    preparedInsert.setNull( 1, SqlType.BIGINT.id );
                    preparedInsert.setNull( 2, SqlType.BOOLEAN.id );
                    preparedInsert.setNull( 3, SqlType.DATE.id );
                    preparedInsert.setNull( 4, SqlType.DECIMAL.id );
                    preparedInsert.setNull( 5, SqlType.DOUBLE.id );
                    preparedInsert.setNull( 7, SqlType.FLOAT.id );
                    preparedInsert.setNull( 9, SqlType.TIME.id );
                    preparedInsert.setNull( 10, SqlType.TIMESTAMP.id );

                    preparedInsert.setInt( 6, 1 );
                    preparedInsert.setString( 12, "Foo" );
                    preparedInsert.setShort( 8, (short) 55 );
                    preparedInsert.setByte( 11, (byte) 11 );

                    preparedInsert.execute();
                    connection.commit();

                    preparedSelect.setInt( 1, 1 );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of( new Object[]{ null, null, null, null, null, 1, null, (short) 55, null, null, (byte) 11, "Foo" } ) );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE pstest" );
                }
            }
        }
    }


    @Test
    public void updateTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( SCHEMA_SQL );

                try {
                    // Insert data
                    PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO pstest(tinteger,tsmallint,tvarchar) VALUES (?,?,?)" );
                    preparedInsert.setInt( 1, 1 );
                    preparedInsert.setShort( 2, (short) 5 );
                    preparedInsert.setString( 3, "Foo" );
                    preparedInsert.execute();

                    preparedInsert.setInt( 1, 2 );
                    preparedInsert.setShort( 2, (short) 5 );
                    preparedInsert.setString( 3, "Bar" );
                    preparedInsert.execute();
                    connection.commit();

                    // Update
                    PreparedStatement preparedUpdate = connection.prepareStatement( "UPDATE pstest SET tsmallint = tsmallint + ? WHERE tinteger = ?" );
                    preparedUpdate.setInt( 1, 3 );
                    preparedUpdate.setInt( 2, 1 );
                    preparedUpdate.executeUpdate();

                    connection.commit();
                    // Check
                    PreparedStatement preparedSelect = connection.prepareStatement( "SELECT tinteger,tsmallint,tvarchar FROM pstest WHERE tinteger = ? OR tinteger = ? ORDER BY tinteger" );
                    preparedSelect.setInt( 1, 1 );
                    preparedSelect.setInt( 2, 2 );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of( new Object[]{ 1, (short) 8, "Foo" }, new Object[]{ 2, (short) 5, "Bar" } ) );

                    // Update again
                    preparedUpdate.setInt( 1, 5 );
                    preparedUpdate.setInt( 2, 1 );
                    preparedUpdate.executeUpdate();

                    // Check
                    preparedSelect.setInt( 1, 1 );
                    preparedSelect.setInt( 2, 2 );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of( new Object[]{ 1, (short) 13, "Foo" }, new Object[]{ 2, (short) 5, "Bar" } ) );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE pstest" );
                }
            }
        }
    }


    @Test
    @Tag("cottontailExcluded") // leads to BatchQuery is unimplemented
    public void batchUpdateTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( SCHEMA_SQL );

                try {
                    // Insert data
                    PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO pstest(tinteger,tsmallint,tvarchar) VALUES (?,?,?)" );
                    preparedInsert.setInt( 1, 1 ); // integer
                    preparedInsert.setShort( 2, (short) 5 ); // smallint + 3 + 1
                    preparedInsert.setString( 3, "Foo" ); // varchar
                    preparedInsert.addBatch();

                    preparedInsert.setInt( 1, 2 ); // integer
                    preparedInsert.setShort( 2, (short) 5 ); // smallint
                    preparedInsert.setString( 3, "Bar" ); // varchar
                    preparedInsert.addBatch();

                    // Update
                    PreparedStatement preparedUpdate = connection.prepareStatement( "UPDATE pstest SET tsmallint = tsmallint + ? WHERE tinteger = ?" );
                    preparedUpdate.setShort( 1, (short) 3 );
                    preparedUpdate.setInt( 2, 1 );
                    preparedUpdate.addBatch();

                    preparedUpdate.setInt( 1, 1 );
                    preparedUpdate.setInt( 2, 1 );
                    preparedUpdate.addBatch();

                    preparedInsert.executeBatch();
                    preparedUpdate.executeBatch();
                    connection.commit();

                    // Check
                    PreparedStatement preparedSelect = connection.prepareStatement( "SELECT tinteger,tsmallint,tvarchar FROM pstest WHERE tinteger = ? OR tinteger = ? ORDER BY tinteger" );
                    preparedSelect.setInt( 1, 1 );
                    preparedSelect.setInt( 2, 2 );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of(
                                    new Object[]{ 1, (short) 9, "Foo" },
                                    new Object[]{ 2, (short) 5, "Bar" } ),
                            true );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE pstest" );
                }
            }
        }
    }


    @Test
    public void commitTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( SCHEMA_SQL );

                try {
                    // Insert data
                    PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO pstest(tinteger,tvarchar) VALUES (?,?)" );
                    preparedInsert.setInt( 1, 1 );
                    preparedInsert.setString( 2, "Foo" );
                    preparedInsert.execute();

                    connection.commit();

                    // Update
                    PreparedStatement preparedUpdate = connection.prepareStatement( "UPDATE pstest SET tvarchar = ? WHERE tinteger = ?" );
                    preparedUpdate.setString( 1, "Bar" );
                    preparedUpdate.setInt( 2, 1 );
                    preparedUpdate.executeUpdate();

                    connection.commit();

                    // Check
                    PreparedStatement preparedSelect = connection.prepareStatement( "SELECT tinteger,tvarchar FROM pstest WHERE tinteger = ?" );
                    preparedSelect.setInt( 1, 1 );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of( new Object[]{ 1, "Bar" } ) );

                    connection.commit();

                    // Update again
                    preparedUpdate.setString( 1, "FooBar" );
                    preparedUpdate.setInt( 2, 1 );
                    preparedUpdate.executeUpdate();

                    connection.commit();

                    // Check
                    preparedSelect.setInt( 1, 1 );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of( new Object[]{ 1, "FooBar" } ) );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE pstest" );
                }
            }
        }
    }


    @Test
    public void arrayTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE psarrtest( "
                        + "tinteger INTEGER NOT NULL, "
                        + "tintarr INTEGER ARRAY(1,2) NOT NULL, "
                        + "PRIMARY KEY (tinteger) )" );

                try {
                    PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO psarrtest(tinteger,tintarr) VALUES (?, ?)" );

                    final ArrayFactoryImpl arrayFactory = new ArrayFactoryImpl( Unsafe.localCalendar().getTimeZone() );

                    preparedInsert.setInt( 1, 1 );
                    preparedInsert.setArray( 2, arrayFactory.createArray(
                            ColumnMetaData.scalar( Types.INTEGER, "INTEGER", Rep.INTEGER ),
                            ImmutableList.of( 1, 2 ) ) );
                    preparedInsert.execute();

                    preparedInsert.setInt( 1, 2 );
                    preparedInsert.setArray( 2, arrayFactory.createArray(
                            ColumnMetaData.scalar( Types.INTEGER, "INTEGER", Rep.INTEGER ),
                            ImmutableList.of( 4, 5 ) ) );
                    preparedInsert.execute();

                    PreparedStatement preparedSelect = connection.prepareStatement( "SELECT tinteger,tintarr FROM psarrtest WHERE tinteger < ?" );
                    preparedSelect.setInt( 1, 3 );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of(
                                    new Object[]{ 1, new Object[]{ 1, 2 } },
                                    new Object[]{ 2, new Object[]{ 4, 5 } } ),
                            true );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE psarrtest" );
                }
            }
        }
    }


    @Test
    public void arrayBatchTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE psarrtest( "
                        + "tinteger INTEGER NOT NULL, "
                        + "tvarchararr VARCHAR ARRAY(1,2) NOT NULL, "
                        + "PRIMARY KEY (tinteger) )" );

                try {
                    PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO psarrtest(tinteger,tvarchararr) VALUES (?, ?)" );

                    final ArrayFactoryImpl arrayFactory = new ArrayFactoryImpl( Unsafe.localCalendar().getTimeZone() );

                    preparedInsert.setInt( 1, 1 );
                    preparedInsert.setArray( 2, arrayFactory.createArray(
                            ColumnMetaData.scalar( Types.VARCHAR, "VARCHAR", Rep.STRING ),
                            ImmutableList.of( "Hans", "Georg" ) ) );
                    preparedInsert.addBatch();

                    preparedInsert.setInt( 1, 2 );
                    preparedInsert.setArray( 2, arrayFactory.createArray(
                            ColumnMetaData.scalar( Types.VARCHAR, "VARCHAR", Rep.STRING ),
                            ImmutableList.of( "Lisa", "Livia" ) ) );
                    preparedInsert.addBatch();

                    preparedInsert.executeBatch();

                    PreparedStatement preparedSelect = connection.prepareStatement( "SELECT tinteger,tvarchararr FROM psarrtest WHERE tinteger < ?" );
                    preparedSelect.setInt( 1, 3 );
                    TestHelper.checkResultSet(
                            preparedSelect.executeQuery(),
                            ImmutableList.of(
                                    new Object[]{ 1, new Object[]{ "Hans", "Georg" } },
                                    new Object[]{ 2, new Object[]{ "Lisa", "Livia" } } ),
                            true );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE psarrtest" );
                }
            }
        }
    }

}
