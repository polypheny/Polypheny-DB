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


import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlNoDataSourceInspection", "SqlDialectInspection" })
@Slf4j
@Tag("adapter")
public class JdbcArrayTest {


    private final static String ARRAYTEST_SQL = "CREATE TABLE arraytest( "
            + "id INTEGER NOT NULL, "
            + "bigintarray BIGINT ARRAY(1,2), "
            + "booleanarray BOOLEAN ARRAY(1,2), "
            + "decimalarray DECIMAL(3,1) ARRAY(1,2), "
            + "doublearray DOUBLE ARRAY(1,2), "
            + "intarray INTEGER ARRAY(1,2), "
            + "realarray REAL ARRAY(1,2), "
            + "smallintarray SMALLINT ARRAY(1,2), "
            + "tinyintarray TINYINT ARRAY(1,2), "
            + "varchararray VARCHAR(20) ARRAY(1,2), "
            + "PRIMARY KEY (id) )";

    private final static String ARRAYTEST_DATA_SQL = "INSERT INTO arraytest(id, bigintarray, booleanarray, decimalarray, doublearray, intarray, realarray, smallintarray, tinyintarray, varchararray) VALUES ("
            + "1,"
            + "ARRAY[9999999,8888888],"
            + "ARRAY[true,false],"
            + "ARRAY[22.2,11.1],"
            + "ARRAY[2.0, 2.5],"
            + "ARRAY[1,2],"
            + "ARRAY[2.0, 2.5],"
            + "ARRAY[56,44],"
            + "ARRAY[33,22],"
            + "ARRAY[CAST('foo' as VARCHAR), CAST('bar' as VARCHAR)]"
            + ")";

    @SuppressWarnings({ "SqlNoDataSourceInspection" })
    private final static Object[] ARRAYTEST_DATA = new Object[]{
            1,
            new Object[]{ 9999999L, 8888888L },
            new Object[]{ true, false },
            new Object[]{ BigDecimal.valueOf( 22.2 ), BigDecimal.valueOf( 11.1 ) },
            new Object[]{ 2.0, 2.5 },
            new Object[]{ 1, 2 },
            new Object[]{ 2.0f, 2.5f },
            new Object[]{ (short) 56, (short) 44 },
            new Object[]{ (byte) 33, (byte) 22 },
            new Object[]{ "foo", "bar" } };


    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void basicTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ARRAY[1, 2] = ARRAY[1, 2], ARRAY[2, 4] = ARRAY[2, 3]" ),
                        ImmutableList.of( new Object[]{ true, false } ) );
            }
        }
    }


    @Test
    public void basicViewTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ARRAY[1, 2] = ARRAY[1, 2], ARRAY[2, 4] = ARRAY[2, 3]" ),
                        ImmutableList.of( new Object[]{ true, false } ) );

                statement.executeUpdate( "CREATE VIEW basicViewTest as SELECT ARRAY[1, 2] = ARRAY[1, 2], ARRAY[2, 4] = ARRAY[2, 3]" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM basicViewTest" ),
                        ImmutableList.of( new Object[]{ true, false } ) );
                statement.executeUpdate( "DROP VIEW basicViewTest" );
            }
        }
    }


    @Test
    public void arrayTypesTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( ARRAYTEST_SQL );

                try {
                    statement.executeUpdate( ARRAYTEST_DATA_SQL );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT id FROM arraytest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[0] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT bigintarray FROM arraytest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[1] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT booleanarray FROM arraytest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[2] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT decimalarray FROM arraytest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[3] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT doublearray FROM arraytest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[4] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT intarray FROM arraytest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[5] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT realarray FROM arraytest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[6] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT smallintarray FROM arraytest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[7] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tinyintarray FROM arraytest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[8] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT varchararray FROM arraytest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[9] } ) );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE arraytest" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void arrayTypesViewTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( ARRAYTEST_SQL );
                statement.executeUpdate( ARRAYTEST_DATA_SQL );
                statement.executeUpdate( "CREATE VIEW arrayTypesViewTest AS SELECT * FROM arraytest" );
                try {

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT id FROM arrayTypesViewTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[0] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT bigintarray FROM arrayTypesViewTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[1] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT booleanarray FROM arrayTypesViewTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[2] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT decimalarray FROM arrayTypesViewTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[3] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT doublearray FROM arrayTypesViewTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[4] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT intarray FROM arrayTypesViewTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[5] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT realarray FROM arrayTypesViewTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[6] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT smallintarray FROM arrayTypesViewTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[7] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tinyintarray FROM arrayTypesViewTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[8] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT varchararray FROM arrayTypesViewTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[9] } ) );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP VIEW arrayTypesViewTest" );
                    statement.executeUpdate( "DROP TABLE arraytest" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void arrayTypesMaterializedTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( ARRAYTEST_SQL );
                statement.executeUpdate( ARRAYTEST_DATA_SQL );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW arrayTypesMaterializedTest AS SELECT * FROM arraytest FRESHNESS MANUAL" );
                try {

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT id FROM arrayTypesMaterializedTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[0] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT bigintarray FROM arrayTypesMaterializedTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[1] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT booleanarray FROM arrayTypesMaterializedTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[2] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT decimalarray FROM arrayTypesMaterializedTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[3] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT doublearray FROM arrayTypesMaterializedTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[4] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT intarray FROM arrayTypesMaterializedTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[5] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT realarray FROM arrayTypesMaterializedTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[6] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT smallintarray FROM arrayTypesMaterializedTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[7] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT tinyintarray FROM arrayTypesMaterializedTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[8] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT varchararray FROM arrayTypesMaterializedTest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[9] } ) );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW arrayTypesMaterializedTest" );
                    statement.executeUpdate( "DROP TABLE arraytest" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void itemOperatorTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( ARRAYTEST_SQL );

                try {
                    statement.executeUpdate( ARRAYTEST_DATA_SQL );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT id, intarray[1] FROM arraytest" ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[0], ((Object[]) ARRAYTEST_DATA[5])[0] } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT b[2] FROM (SELECT ARRAY[1, 2] as a, ARRAY[2, 4] as b)" ),
                            ImmutableList.of( new Object[]{ 4 } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT varchararray[1] FROM arraytest WHERE varchararray[1] = '" + ((Object[]) ARRAYTEST_DATA[9])[0] + "'" ),
                            ImmutableList.of( new Object[]{ ((Object[]) ARRAYTEST_DATA[9])[0] } ) );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE arraytest" );
                    connection.commit();
                }

            }
        }
    }


    @Test
    public void itemOperatorTest2() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( ARRAYTEST_SQL );

                try {
                    statement.executeUpdate( ARRAYTEST_DATA_SQL );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT bigintarray FROM arraytest WHERE bigintarray[1] = " + ((Object[]) ARRAYTEST_DATA[1])[0] ),
                            ImmutableList.of( new Object[]{ ARRAYTEST_DATA[1] } ) );

                    /*TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT varchararray FROM arraytest ORDER BY varchararray[1]" ),
                            ImmutableList.of( new Object[] { ARRAYTEST_DATA[3] } ) );*/

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE arraytest" );
                    connection.commit();
                }

            }
        }
    }


    @Test
    public void nullTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( ARRAYTEST_SQL );

                try {
                    statement.executeUpdate( ARRAYTEST_DATA_SQL );
                    statement.executeUpdate( "UPDATE arraytest SET intarray = null" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT intarray FROM arraytest" ),
                            ImmutableList.of( new Object[]{ null } ) );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE arraytest" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void arrayFilterTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( ARRAYTEST_SQL );

                try {
                    statement.executeUpdate( ARRAYTEST_DATA_SQL );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT id FROM arraytest WHERE intarray = array[1,2]" ),
                            ImmutableList.of( new Object[]{ 1 } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT id FROM arraytest WHERE bigintarray = array[CAST(9999999 as BIGINT),CAST(8888888 as BIGINT)]" ),
                            ImmutableList.of( new Object[]{ 1 } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT id FROM arraytest WHERE booleanarray = array[true,false]" ),
                            ImmutableList.of( new Object[]{ 1 } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT id FROM arraytest WHERE decimalarray = array[22.2,11.1]" ),
                            ImmutableList.of( new Object[]{ 1 } ) );

                    /*TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT id FROM arraytest WHERE doublearray = array[cast(2.0 as double),cast(2.5 as double)]" ),
                            ImmutableList.of( new Object[]{ 1 } ) );*/// this is not really deterministic and depends on the precision

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT id FROM arraytest WHERE smallintarray = array[CAST(56 as SMALLINT),CAST(44 as SMALLINT)]" ),
                            ImmutableList.of( new Object[]{ 1 } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT intarray FROM arraytest WHERE varchararray = array[CAST('foo' as VARCHAR), CAST('bar' as VARCHAR)]" ),
                            ImmutableList.of( new Object[]{ new Object[]{ 1, 2 } } ) );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP TABLE arraytest" );
                    connection.commit();
                }

            }
        }
    }


    @Test
    public void multiDimArrayTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE multidimarray(id integer not null, arr INTEGER ARRAY(3, 2), primary key(id))" );
                statement.executeUpdate( "INSERT INTO multidimarray VALUES ( 1, array[ [[111,112],[121,122]], [[211,212],[221,222]] ])" );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT arr FROM multidimarray" ),
                            ImmutableList.of( new Object[]{
                                    new Object[]{
                                            new Object[]{
                                                    new Object[]{ 111, 112 },
                                                    new Object[]{ 121, 122 }
                                            },
                                            new Object[]{
                                                    new Object[]{ 211, 212 },
                                                    new Object[]{ 221, 222 }
                                            }
                                    }
                            } ) );

                    statement.executeUpdate( "UPDATE multidimarray SET arr = array[ [[999,999],[999,999]], [[999,999],[999,999]] ] WHERE id = 1" );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT arr FROM multidimarray" ),
                            ImmutableList.of( new Object[]{
                                    new Object[]{
                                            new Object[]{
                                                    new Object[]{ 999, 999 },
                                                    new Object[]{ 999, 999 }
                                            },
                                            new Object[]{
                                                    new Object[]{ 999, 999 },
                                                    new Object[]{ 999, 999 }
                                            }
                                    }
                            } ) );

                    connection.commit();
                } finally {
                    connection.rollback();
                    statement.executeUpdate( "DROP TABLE IF EXISTS multidimarray" );
                    connection.commit();
                }
            }
        }
    }

}
