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

package org.polypheny.db.sql.fun;


import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class CeilFloorSignFunctionsTest {


    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestData();
    }


    private static void addTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE TableDecimal( ID INTEGER NOT NULL,Data DECIMAL(2,1), PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO TableDecimal VALUES (0, -2.1)" );
                statement.executeUpdate( "INSERT INTO TableDecimal VALUES (1, 3.0)" );
                statement.executeUpdate( "INSERT INTO TableDecimal VALUES (2, 4.1)" );

                statement.executeUpdate( "CREATE TABLE TableDouble( ID INTEGER NOT NULL, Data DOUBLE , PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO TableDouble VALUES (0, 2.1)" );
                statement.executeUpdate( "INSERT INTO TableDouble VALUES (1, -3.0)" );
                statement.executeUpdate( "INSERT INTO TableDouble VALUES (2, 4.1)" );

                statement.executeUpdate( "CREATE TABLE TableInteger( ID INTEGER NOT NULL, Data INTEGER, PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO TableInteger VALUES (0, 2)" );
                statement.executeUpdate( "INSERT INTO TableInteger VALUES (1, 3)" );
                statement.executeUpdate( "INSERT INTO TableInteger VALUES (2, -4)" );

                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE TableDecimal" );
                statement.executeUpdate( "DROP TABLE TableDouble" );
                statement.executeUpdate( "DROP TABLE TableInteger" );
            }

            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    public void ceilTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                BigDecimal a = BigDecimal.valueOf( -2 );
                BigDecimal b = BigDecimal.valueOf( 3 );
                BigDecimal c = BigDecimal.valueOf( 5 );

                // For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, a },
                        new Object[]{ 1, b },
                        new Object[]{ 2, c }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, CEIL(Data) FROM TableDecimal" ),
                        expectedResult,
                        true
                );

                // For Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 3.0 },
                        new Object[]{ 1, -3.0 },
                        new Object[]{ 2, 5.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, CEIL(Data) FROM TableDouble" ),
                        expectedResult,
                        true
                );

                // For Integer
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 2 },
                        new Object[]{ 1, 3 },
                        new Object[]{ 2, -4 }

                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID,CEIL(Data) FROM TableInteger" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void floorTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                BigDecimal a = BigDecimal.valueOf( -3 );
                BigDecimal b = BigDecimal.valueOf( 3 );
                BigDecimal c = BigDecimal.valueOf( 4 );

                // For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, a },
                        new Object[]{ 1, b },
                        new Object[]{ 2, c }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, FLOOR(Data) FROM TableDecimal" ),
                        expectedResult,
                        true
                );

                // For Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 2.0 },
                        new Object[]{ 1, -3.0 },
                        new Object[]{ 2, 4.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, FLOOR(Data) FROM TableDouble" ),
                        expectedResult,
                        true
                );

                // For Integer
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 2 },
                        new Object[]{ 1, 3 },
                        new Object[]{ 2, -4 }

                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID,FLOOR(Data) FROM TableInteger" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void signTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                BigDecimal a = BigDecimal.valueOf( -1.0 );
                BigDecimal b = BigDecimal.valueOf( 1.0 );
                BigDecimal c = BigDecimal.valueOf( 1.0 );

                // For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, a },
                        new Object[]{ 1, b },
                        new Object[]{ 2, c }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, SIGN(Data) FROM TableDecimal" ),
                        expectedResult,
                        true
                );

                // For Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1.0 },
                        new Object[]{ 1, -1.0 },
                        new Object[]{ 2, 1.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, SIGN(Data) FROM TableDouble" ),
                        expectedResult,
                        true
                );

                // For Integer
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1 },
                        new Object[]{ 1, 1 },
                        new Object[]{ 2, -1 }

                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID,SIGN(Data) FROM TableInteger" ),
                        expectedResult,
                        true
                );
            }
        }
    }

}

