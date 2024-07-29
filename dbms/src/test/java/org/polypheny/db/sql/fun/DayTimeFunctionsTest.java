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
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class DayTimeFunctionsTest {


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
                statement.executeUpdate( "CREATE TABLE DateTestTable(ID INTEGER NOT NULL, DateData DATE,  PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO DateTestTable VALUES (1,Date '2000-01-05')" );
                statement.executeUpdate( "INSERT INTO DateTestTable VALUES (2,Date '2001-02-02')" );
                statement.executeUpdate( "INSERT INTO DateTestTable VALUES (3,Date '2002-03-03')" );

                statement.executeUpdate( "CREATE TABLE TimeTestTable(ID INTEGER NOT NULL, TimeData TIME,  PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TimeTestTable VALUES (1,TIME '12:30:35')" );
                statement.executeUpdate( "INSERT INTO TimeTestTable VALUES (2,TIME '06:34:59')" );
                statement.executeUpdate( "INSERT INTO TimeTestTable VALUES (3,TIME '23:59:59')" );

                statement.executeUpdate( "CREATE TABLE TimeStampTestTable(ID INTEGER NOT NULL, TimeStampData TIMESTAMP,TimeStampDataToo TIMESTAMP,  PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TimeStampTestTable VALUES (1,TIMESTAMP '2000-01-05 12:30:35',TIMESTAMP '2002-03-03 23:59:59')" );
                statement.executeUpdate( "INSERT INTO TimeStampTestTable VALUES (2,TIMESTAMP '2001-02-02 06:34:59',TIMESTAMP '2001-02-02 06:34:59')" );
                statement.executeUpdate( "INSERT INTO TimeStampTestTable VALUES (3,TIMESTAMP '2002-03-03 23:59:59',TIMESTAMP '2000-01-01 12:30:35')" );

                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE DateTestTable" );
                statement.executeUpdate( "DROP TABLE TimeTestTable" );
                statement.executeUpdate( "DROP TABLE TimeStampTestTable" );
                connection.commit();
            }
        }
    }

    // --------------- Tests ---------------


    @Test
    public void dateTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Select Test
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, Date.valueOf( "2000-01-05" ) },
                        new Object[]{ 2, Date.valueOf( "2001-02-02" ) },
                        new Object[]{ 3, Date.valueOf( "2002-03-03" ) }

                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id,DateData FROM DateTestTable" ),
                        expectedResult,
                        true
                );

                // YEAR() Equivalent to EXTRACT(YEAR FROM date)
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, 2000L },
                        new Object[]{ 2, 2001L },
                        new Object[]{ 3, 2002L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, YEAR(DateData) FROM DateTestTable" ),
                        expectedResult,
                        true
                );

                // QUARTER(date) Equivalent to  EXTRACT(MONTH FROM date)
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, 1L },
                        new Object[]{ 2, 1L },
                        new Object[]{ 3, 1L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, QUARTER(DateData) FROM DateTestTable" ),
                        expectedResult,
                        true
                );

                // MONTH(date) Equivalent to EXTRACT(MONTH FROM date)
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, 1L },
                        new Object[]{ 2, 2L },
                        new Object[]{ 3, 3L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, MONTH(DateData) FROM DateTestTable" ),
                        expectedResult,
                        true
                );

                // WEEK(date) Equivalent to EXTRACT(WEEK FROM date)
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, 1L },
                        new Object[]{ 2, 5L },
                        new Object[]{ 3, 9L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, WEEK(DateData) FROM DateTestTable" ),
                        expectedResult,
                        true
                );

                // DAYOFYEAR(date) Equivalent to EXTRACT(DOY FROM date)
                List<Object[]> expectedResult1 = ImmutableList.of(
                        new Object[]{ 5L },
                        new Object[]{ 33L },
                        new Object[]{ 62L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT DAYOFYEAR(DateData) FROM DateTestTable" ),
                        expectedResult1,
                        true
                );

                // DAYOFMONTH(date) Equivalent to EXTRACT(DAY FROM date)
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, 5L },
                        new Object[]{ 2, 2L },
                        new Object[]{ 3, 3L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id,DAYOFMONTH(DateData) FROM DateTestTable" ),
                        expectedResult,
                        true
                );

                // DAYOFWEEK(date) Equivalent to EXTRACT(DOW FROM date)
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, 4L },
                        new Object[]{ 2, 6L },
                        new Object[]{ 3, 1L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id,DAYOFWEEK(DateData) FROM DateTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void selectTimeTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Select is working
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, Time.valueOf( "12:30:35" ) },
                        new Object[]{ 2, Time.valueOf( "6:34:59" ) },
                        new Object[]{ 3, Time.valueOf( "23:59:59" ) }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, TimeData FROM TimeTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void extractHourTimeTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // HOUR() Equivalent to EXTRACT(HOUR FROM date)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 12L },
                        new Object[]{ 2, 6L },
                        new Object[]{ 3, 23L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, HOUR(TimeData) FROM TimeTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void extractMinuteTimeTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // MINUTE() Equivalent to EXTRACT(HOUR FROM date)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 30L },
                        new Object[]{ 2, 34L },
                        new Object[]{ 3, 59L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, MINUTE(TimeData) FROM TimeTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void extractSecTimeTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // SECOND() Equivalent to EXTRACT(HOUR FROM date)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 35L },
                        new Object[]{ 2, 59L },
                        new Object[]{ 3, 59L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, SECOND(TimeData) FROM TimeTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void timeStampTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Select IS working
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, Timestamp.valueOf( "2000-01-05 12:30:35" ) },
                        new Object[]{ 2, Timestamp.valueOf( "2001-02-02 06:34:59" ) },
                        new Object[]{ 3, Timestamp.valueOf( "2002-03-03 23:59:59" ) }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, TimeStampData FROM TimeStampTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void timeStampExtractTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // EXTRACT(timeUnit FROM timestamp) Extracts and returns the value of a specified timestamp field from a timestamp value expression.
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, Long.valueOf( "2000" ) },
                        new Object[]{ 2, Long.valueOf( "2001" ) },
                        new Object[]{ 3, Long.valueOf( "2002" ) }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, EXTRACT(Year FROM TimeStampData) FROM TimeStampTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void timeStampQuarterTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // QUARTER(date) Equivalent to  EXTRACT(MONTH FROM date)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 1L },
                        new Object[]{ 2, 1L },
                        new Object[]{ 3, 1L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, QUARTER(TimeStampData) FROM TimeStampTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void timeStampMonthTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // MONTH(date) Equivalent to EXTRACT(MONTH FROM date)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 1L },
                        new Object[]{ 2, 2L },
                        new Object[]{ 3, 3L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, MONTH(TimeStampData) FROM TimeStampTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void timeStampWeekTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // WEEK(date) Equivalent to EXTRACT(WEEK FROM date)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 1L },
                        new Object[]{ 2, 5L },
                        new Object[]{ 3, 9L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, WEEK(TimeStampData) FROM TimeStampTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void timeStampDoyTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // DAYOFYEAR(date) Equivalent to EXTRACT(DOY FROM date)
                List<Object[]> expectedResult1 = ImmutableList.of(
                        new Object[]{ 5L },
                        new Object[]{ 33L },
                        new Object[]{ 62L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT DAYOFYEAR(TimeStampData) FROM TimeStampTestTable" ),
                        expectedResult1,
                        true
                );
            }
        }
    }


    @Test
    public void timeStampDomTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // DAYOFMONTH(date) Equivalent to EXTRACT(DAY FROM date)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 5L },
                        new Object[]{ 2, 2L },
                        new Object[]{ 3, 3L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id,DAYOFMONTH(TimeStampData) FROM TimeStampTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void timeStampDowTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // DAYOFWEEK(date) Equivalent to EXTRACT(DOW FROM date)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 4L },
                        new Object[]{ 2, 6L },
                        new Object[]{ 3, 1L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id,DAYOFWEEK(TimeStampData) FROM TimeStampTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void timeStampHourTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // HOUR() Equivalent to EXTRACT(HOUR FROM date)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 12L },
                        new Object[]{ 2, 6L },
                        new Object[]{ 3, 23L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, HOUR(TimeStampData) FROM TimeStampTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void timeStampMinuteTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // MINUTE() Equivalent to EXTRACT(HOUR FROM date)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 30L },
                        new Object[]{ 2, 34L },
                        new Object[]{ 3, 59L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, MINUTE(TimeStampData) FROM TimeStampTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void timeStampSecondsTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // SECOND() Equivalent to EXTRACT(HOUR FROM date)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 35L },
                        new Object[]{ 2, 59L },
                        new Object[]{ 3, 59L }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, SECOND(TimeStampData) FROM TimeStampTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void timeStampFloorTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // FLOOR(timestamp TO timeUnit) Rounds timestamp down to timeUnit
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, Timestamp.valueOf( "2000-01-01 00:00:00.0" ) },
                        new Object[]{ 2, Timestamp.valueOf( "2001-01-01 00:00:00.0" ) },
                        new Object[]{ 3, Timestamp.valueOf( "2002-01-01 00:00:00.0" ) }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, FLOOR(TimeStampData TO YEAR) FROM TimeStampTestTable" ),
                        expectedResult,
                        true
                );

            }
        }
    }


    @Test
    public void timeStampAddTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // TIMESTAMPADD(timeUnit, integer, timestamp) Returns timestamp with an interval of (signed) integer timeUnits added.
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, Timestamp.valueOf( "2005-01-05 12:30:35" ) },
                        new Object[]{ 2, Timestamp.valueOf( "2006-02-02 06:34:59" ) },
                        new Object[]{ 3, Timestamp.valueOf( "2007-03-03 23:59:59" ) }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, TIMESTAMPADD(YEAR,5,TimeStampData) FROM TimeStampTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Disabled
    @Test
    public void timeStampDiff() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // TIMESTAMPDIFF(timeUnit, timestamp, timestamp2) Returns the (signed) number of timeUnit intervals between timestamp and timestamp2.
                // Equivalent to (timestamp2 - timestamp) timeUnit
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, Timestamp.valueOf( "2000-01-01 12:30:35" ) },
                        new Object[]{ 2, Timestamp.valueOf( "2001-02-02 06:34:59" ) },
                        new Object[]{ 3, Timestamp.valueOf( "2002-03-03 23:59:59" ) }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, TIMESTAMPDIFF(YEAR,TimeStampData,TimeStampDataToo) FROM TimeStampTestTable" ),
                        expectedResult,
                        false
                );
            }
        }
    }

}

