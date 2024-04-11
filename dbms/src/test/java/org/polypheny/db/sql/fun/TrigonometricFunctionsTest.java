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
public class TrigonometricFunctionsTest {


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
                statement.executeUpdate( "CREATE TABLE trigotestdecimal( AngleinDegree INTEGER NOT NULL,AngleinRadian DECIMAL(6,4), PRIMARY KEY (AngleinDegree) )" );
                statement.executeUpdate( "INSERT INTO trigotestdecimal VALUES (0, 0)" );
                statement.executeUpdate( "INSERT INTO trigotestdecimal VALUES (30, 0.52)" );
                statement.executeUpdate( "INSERT INTO trigotestdecimal  VALUES (45, 0.61)" );

                statement.executeUpdate( "CREATE TABLE trigotestdouble( AngleinDegree INTEGER NOT NULL, AngleinRadian DOUBLE , PRIMARY KEY (AngleinDegree) )" );
                statement.executeUpdate( "INSERT INTO trigotestdouble VALUES (0, 0)" );
                statement.executeUpdate( "INSERT INTO trigotestdouble VALUES (30, 0.52)" );
                statement.executeUpdate( "INSERT INTO trigotestdouble VALUES (45, 0.61)" );

                statement.executeUpdate( "CREATE TABLE trigotestinteger( AngleinDegree INTEGER NOT NULL, AngleinRadian INTEGER, PRIMARY KEY (AngleinDegree) )" );
                statement.executeUpdate( "INSERT INTO trigotestinteger VALUES (0,  0)" );
                statement.executeUpdate( "INSERT INTO trigotestinteger VALUES (58, 1)" );

                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE trigotestdecimal" );
                statement.executeUpdate( "DROP TABLE trigotestdouble" );
                statement.executeUpdate( "DROP TABLE trigotestinteger" );
            }
            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    public void sine() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0 },
                        new Object[]{ 30, 0.49688013784373675 },
                        new Object[]{ 45, 0.5728674601004813 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT Angleindegree, SIN(AngleinRadian) FROM trigotestdecimal" ),
                        expectedResult,
                        true
                );

                // For Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0 },
                        new Object[]{ 30, 0.49688 },
                        new Object[]{ 45, 0.572867 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(SIN(AngleinRadian),6) FROM trigotestdouble" ),
                        expectedResult,
                        true
                );

                // For Integer
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0 },
                        new Object[]{ 58, 0.841471 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree,SIN(AngleinRadian) FROM trigotestinteger" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void cosine() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1.0 },
                        new Object[]{ 30, 0.8678191796776499 },
                        new Object[]{ 45, 0.8196480178454796 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, COS(AngleinRadian) FROM trigotestdecimal" ),
                        expectedResult,
                        true
                );

                // For Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1.0 },
                        new Object[]{ 30, 0.867819 },
                        new Object[]{ 45, 0.819648 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(COS(AngleinRadian),6) FROM trigotestdouble" ),
                        expectedResult,
                        true
                );

                // For Integer
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1.0 },
                        new Object[]{ 58, 0.540302 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, COS(AngleinRadian) FROM trigotestinteger" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void tangent() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0 },
                        new Object[]{ 30, 0.5725618302516684 },
                        new Object[]{ 45, 0.698918862277391 }

                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, TAN(AngleinRadian) FROM trigotestdecimal" ),
                        expectedResult,
                        true
                );

                // For Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0 },
                        new Object[]{ 30, 0.572562 },
                        new Object[]{ 45, 0.698919 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(TAN(AngleinRadian),6) FROM trigotestdouble" ),
                        expectedResult,
                        true
                );

                // For Integer
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0 },
                        new Object[]{ 58, 1.557408 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(TAN(AngleinRadian),6) FROM trigotestinteger" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void arcsine() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0 },
                        new Object[]{ 30, 0.5468509506959441 },
                        new Object[]{ 45, 0.6560605909249226 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ASIN(AngleinRadian) FROM trigotestdecimal" ),
                        expectedResult,
                        true
                );

                // For Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0 },
                        new Object[]{ 30, 0.546851 },
                        new Object[]{ 45, 0.656061 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ASIN(AngleinRadian),6) FROM trigotestdouble" ),
                        expectedResult,
                        true
                );

                // For Integer
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0 },
                        new Object[]{ 58, 1.570796 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ASIN(AngleinRadian),6) FROM trigotestinteger" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void cot() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 30, Double.valueOf( 1.7465362641453972 ) },
                        new Object[]{ 45, Double.valueOf( 1.4307812451098423 ) }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, COT(AngleinRadian) FROM trigotestdecimal where AngleinDegree <> 0" ),
                        expectedResult,
                        true
                );

                // For Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 30, Double.valueOf( 1.7465362641453972 ) },
                        new Object[]{ 45, Double.valueOf( 1.4307812451098423 ) }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, COT(AngleinRadian) FROM trigotestdouble where AngleinDegree <> 0" ),
                        expectedResult,
                        true
                );

                // For Integer
                expectedResult = ImmutableList.of(
                        new Object[]{ 58, 0.6420926159343306 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, COT(AngleinRadian) FROM trigotestInteger where AngleinDegree <> 0" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void arccosine() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1.570796000000000081087137004942633211612701416015625 },
                        new Object[]{ 30, 1.0239450000000001050892706189188174903392791748046875 },
                        new Object[]{ 45, 0.914735999999999993548271959298290312290191650390625 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ACOS(AngleinRadian) FROM trigotestdecimal" ),
                        expectedResult,
                        true
                );

                // For Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1.570796 },
                        new Object[]{ 30, 1.023945 },
                        new Object[]{ 45, 0.914736 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ACOS(AngleinRadian),6) FROM trigotestdouble" ),
                        expectedResult,
                        true
                );

                // For Integer
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1.570796 },
                        new Object[]{ 58, 0.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ACOS(AngleinRadian),6) FROM trigotestinteger" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void arctangent() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0 },
                        new Object[]{ 30, 0.4795192919925962 },
                        new Object[]{ 45, 0.5477400137159024 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ATAN(AngleinRadian) FROM trigotestdecimal" ),
                        expectedResult,
                        true
                );

                // For Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0 },
                        new Object[]{ 30, 0.479519 },
                        new Object[]{ 45, 0.54774 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ATAN(AngleinRadian),6) FROM trigotestdouble" ),
                        expectedResult,
                        true
                );

                // For Integer
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0 },
                        new Object[]{ 58, 0.785398 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ATAN(AngleinRadian),6) FROM trigotestinteger" ),
                        expectedResult,
                        true
                );

                // ATan2 For Decimal
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0 },
                        new Object[]{ 58, 1.553557 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ATAN2(AngleinDegree,AngleinRadian),6) FROM trigotestinteger" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void degreeRadianPiTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // PI
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 2.1415926535897931 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT PI - 1" ),
                        expectedResult
                );

                // Degree() with TRUNCATE and Round
                expectedResult = ImmutableList.of(
                        new Object[]{ 30.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT TRUNCATE(ROUND(DEGREES(0.523599),1),1)" ),
                        expectedResult
                );

                // Radian with TRUNCATE and Round
                expectedResult = ImmutableList.of(
                        new Object[]{ 0.5235 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT TRUNCATE(ROUND(RADIANS(30),6),4)" ),
                        expectedResult
                );
            }
        }
    }

}
