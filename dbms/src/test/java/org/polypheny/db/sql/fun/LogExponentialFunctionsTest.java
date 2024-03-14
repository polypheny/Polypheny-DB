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
public class LogExponentialFunctionsTest {


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
                statement.executeUpdate( "INSERT INTO TableDecimal VALUES (0, -2.0)" );
                statement.executeUpdate( "INSERT INTO TableDecimal VALUES (1, 3.0)" );
                statement.executeUpdate( "INSERT INTO TableDecimal VALUES (2, 4.0)" );

                statement.executeUpdate( "CREATE TABLE TableDouble( ID INTEGER NOT NULL, Data DOUBLE , PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO TableDouble VALUES (0, 2.0)" );
                statement.executeUpdate( "INSERT INTO TableDouble VALUES (1, -3.0)" );
                statement.executeUpdate( "INSERT INTO TableDouble VALUES (2, 4.0)" );

                statement.executeUpdate( "CREATE TABLE TableInteger( ID INTEGER NOT NULL, Data INTEGER, PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO TableInteger VALUES (0, 2)" );
                statement.executeUpdate( "INSERT INTO TableInteger VALUES (1, 3)" );
                statement.executeUpdate( "INSERT INTO TableInteger VALUES (2, -4)" );

                statement.executeUpdate( "CREATE TABLE TableTinyInt( ID INTEGER NOT NULL, Data TinyINT, PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO TableTinyInt VALUES (0, -128)" );
                statement.executeUpdate( "INSERT INTO TableTinyInt VALUES (1, 23)" );
                statement.executeUpdate( "INSERT INTO TableTinyInt VALUES (2, 127)" );

                statement.executeUpdate( "CREATE TABLE TableBigInt( ID INTEGER NOT NULL, Data BigInt, PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO TableBigInt VALUES (0, 1241241)" );
                statement.executeUpdate( "INSERT INTO TableBigInt VALUES (1, 1)" );
                statement.executeUpdate( "INSERT INTO TableBigInt VALUES (2, -1241241)" );

                statement.executeUpdate( "CREATE TABLE TableSmallInt( ID INTEGER NOT NULL, Data INTEGER, PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO TableSmallInt VALUES (0, -32768)" );
                statement.executeUpdate( "INSERT INTO TableSmallInt VALUES (1, 3)" );
                statement.executeUpdate( "INSERT INTO TableSmallInt VALUES (2, 32767)" );

                statement.executeUpdate( "CREATE TABLE TableReal( ID INTEGER NOT NULL, Data INTEGER, PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO TableReal VALUES (0, 1.401)" );
                statement.executeUpdate( "INSERT INTO TableReal VALUES (1, 3)" );
                statement.executeUpdate( "INSERT INTO TableReal VALUES (2, 3.402)" );

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
                statement.executeUpdate( "DROP TABLE TableTinyInt" );
                statement.executeUpdate( "DROP TABLE TableSmallInt" );
                statement.executeUpdate( "DROP TABLE TableBigInt" );
                statement.executeUpdate( "DROP TABLE TableReal" );

            }
            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    public void logTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 1.0986122886681098 },
                        new Object[]{ 2, 1.3862943611198906 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, LN(Data) FROM TableDecimal where data > 0" ),
                        expectedResult,
                        true
                );

                // LOG10 FOR DECIMAL
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, 0.47712125471966244 },
                        new Object[]{ 2, 0.6020599913279624 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, LOG10(Data) FROM TableDecimal where data > 0" ),
                        expectedResult,
                        true
                );

                // For Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.6931471805599453 },
                        new Object[]{ 2, 1.3862943611198906 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, LN(Data) FROM TableDouble where data > 0" ),
                        expectedResult,
                        true
                );

                // LOG10 FOR DOUBLE
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.3010299956639812 },
                        new Object[]{ 2, 0.6020599913279624 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, LOG10(Data) FROM TableDouble where data > 0" ),
                        expectedResult,
                        true
                );

                // For Integer
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.6931471805599453 },
                        new Object[]{ 1, 1.0986122886681098 }

                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, LN(Data) FROM TableInteger where Data > 0" ),
                        expectedResult,
                        true
                );

                // LOG10 FOR INTEGER
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.3010299956639812 },
                        new Object[]{ 1, 0.47712125471966244 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, LOG10(Data) FROM TableInteger where data > 0" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void exponentialTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.13533528323661270231781372785917483270168304443359375 },
                        new Object[]{ 1, 20.08553692318766792368478490971028804779052734375 },
                        new Object[]{ 2, 54.59815003314423620395245961844921112060546875 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, EXP(Data) FROM TableDecimal" ),
                        expectedResult, true
                );

                // For Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 7.389056 },
                        new Object[]{ 1, 0.049787 },
                        new Object[]{ 2, 54.59815 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, ROUND(EXP(Data),6) FROM TableDouble" ),
                        expectedResult,
                        true
                );

                // For Integer
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 7.38905609893065 },
                        new Object[]{ 1, 20.085536923187668 },
                        new Object[]{ 2, 0.01831563888873418 }

                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, ROUND(EXP(Data),6) FROM TableInteger" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void sqrtTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 1.732050807568877193176604123436845839023590087890625 },
                        new Object[]{ 2, 2.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, SQRT(Data) FROM TableDecimal where data > 0" ),
                        expectedResult,
                        true
                );

                // For Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1.4142135623730951 },
                        new Object[]{ 2, 2.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, SQRT(Data) FROM TableDouble where data > 0" ),
                        expectedResult,
                        true
                );

                // For Integer
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1.4142135623730951 },
                        new Object[]{ 1, 1.732050807568877193176604123436845839023590087890625 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, SQRT(Data) FROM TableInteger where data > 0" ),
                        expectedResult,
                        true
                );

                // For TinyInt
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, 4.795831523312719 },
                        new Object[]{ 2, 11.269427669584644 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, SQRT(Data) FROM TableTinyInt where data > 0" ),
                        expectedResult,
                        true
                );

                // For BigInt
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1114.1099586665582 },
                        new Object[]{ 1, 1.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, SQRT(Data) FROM TableBigInt where data > 0" ),
                        expectedResult,
                        true
                );

                // For SmallInt
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, 1.732050807568877193176604123436845839023590087890625 },
                        new Object[]{ 2, 181.01657382681842 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, SQRT(Data) FROM TableSmallInt where data > 0" ),
                        expectedResult,
                        true
                );

                // For Real
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1.0 },
                        new Object[]{ 1, 1.7320508075688772 },
                        new Object[]{ 2, 1.7320508075688772 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, SQRT(Data) FROM TableReal where data > 0" ),
                        expectedResult,
                        true
                );
            }
        }
    }

}

