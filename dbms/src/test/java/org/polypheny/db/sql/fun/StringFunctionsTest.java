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
public class StringFunctionsTest {


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
                statement.executeUpdate( "CREATE TABLE TableA(ID INTEGER NOT NULL, DataA VARCHAR(255), PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TableA VALUES (1, 'First')" );
                statement.executeUpdate( "INSERT INTO TableA VALUES (2, 'Second')" );
                statement.executeUpdate( "INSERT INTO TableA VALUES (3, 'Third')" );

                statement.executeUpdate( "CREATE TABLE TableB(ID INTEGER NOT NULL, DataB VARCHAR(255), PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TableB VALUES (1, 'Fourth')" );
                statement.executeUpdate( "INSERT INTO TableB VALUES (2, 'Fifth')" );
                statement.executeUpdate( "INSERT INTO TableB VALUES (3, 'Sixth')" );

                statement.executeUpdate( "CREATE TABLE TableC(ID INTEGER NOT NULL, DataC VARCHAR(255), PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TableC VALUES (1, ' Seventh')" );
                statement.executeUpdate( "INSERT INTO TableC VALUES (2, 'Eighth ')" );
                statement.executeUpdate( "INSERT INTO TableC VALUES (3, ' ninth ')" );

                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE TableA" );
                statement.executeUpdate( "DROP TABLE TableB" );
                statement.executeUpdate( "DROP TABLE TableC" );
            }
            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    public void concatenatesTwoString() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // String Concatenate ||
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "FirstFourth" },
                        new Object[]{ 2, "SecondFifth" },
                        new Object[]{ 3, "ThirdSixth" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,S.DataA||T.DataB FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void stringLength() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                //,CHARLENGTH() WITH String Concatenate ||
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 11 },
                        new Object[]{ 2, 11 },
                        new Object[]{ 3, 10 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,CHAR_LENGTH(S.DataA||T.DataB) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );

                expectedResult = ImmutableList.of(
                        new Object[]{ 1, 5 },
                        new Object[]{ 2, 6 },
                        new Object[]{ 3, 5 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,CHAR_LENGTH(S.DataA) FROM TableA AS S" ),
                        expectedResult,
                        true );

            }
        }
    }


    @Test
    public void caseSensitive() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // UPPER(CONCATENATED STRING) MAKES STRING UPPERCASE
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "FIRSTFOURTH" },
                        new Object[]{ 2, "SECONDFIFTH" },
                        new Object[]{ 3, "THIRDSIXTH" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id , Upper(S.DataA||T.DataB) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );

                // LOWER(CONCATENATED STRING) MAKES STRING LOWER CASE
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "firstfourth" },
                        new Object[]{ 2, "secondfifth" },
                        new Object[]{ 3, "thirdsixth" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id , LOWER(S.DataA||T.DataB) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void stringPosition() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // POSITION(string1 IN string2) Returns the position of the first occurrence of string1 in string2
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 10 },
                        new Object[]{ 2, 10 },
                        new Object[]{ 3, 9 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,POSITION('th' IN S.DataA||T.DataB) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );

                // POSITION(string1 IN string2 FROM integer) Returns the position of the first occurrence of string1 in string2 starting at a given point (not standard SQL)
                /*expectedResult = ImmutableList.of(
                        new Object[]{ 1, 2 },
                        new Object[]{ 2, 2 },
                        new Object[]{ 3, 1 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,POSITION('th' IN S.DataA||T.DataB FROM 9) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult );*/
            }
        }
    }


    @Test
    public void stringTrim() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // TRIM( { BOTH | LEADING | TRAILING } string1 FROM string2) Removes the longest string containing only the characters in string1 from the start/end/both ends of string1
                // TODO check whitespace
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, " Sevent" },
                        new Object[]{ 2, "Eighth " },
                        new Object[]{ 3, " ninth " }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT U.id, TRIM(TRAILING 'h' FROM U.dataC) FROM TableC AS U " ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void stringOverlay() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // OVERLAY(string1 PLACING string2 FROM integer [ FOR integer2 ])
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Foutyh" },
                        new Object[]{ 2, "Fifty" },
                        new Object[]{ 3, "Sixty" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT T.id ,Overlay(T.DataB placing 'ty' from 4) FROM TableB AS T" ),
                        expectedResult,
                        true );

                // OVERLAY() with Concatenated String Inside
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "FirstFoutyh" },
                        new Object[]{ 2, "SecondFityh" },
                        new Object[]{ 3, "ThirdSixty" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,Overlay(S.DataA||T.DataB placing 'ty' from 9) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void subString() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // SUBSTRING(string FROM integer) Returns a substring of a character string starting at a given point
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "First" },
                        new Object[]{ 2, "Second" },
                        new Object[]{ 3, "Third" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,SUBSTRING(S.DataA,1) FROM TableA AS S" ),
                        expectedResult,
                        true );

                // SUBSTRING(string FROM integer FOR integer) Returns a substring of a character string starting at a given point with a given length
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "First" },
                        new Object[]{ 2, "Secon" },
                        new Object[]{ 3, "Third" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,SUBSTRING(S.DataA||T.DataB,1,5) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void stringInitcap() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // INITCAP(string)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "First" },
                        new Object[]{ 2, "Second" },
                        new Object[]{ 3, "Third" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id , INITCAP(DataA) FROM TableA" ),
                        expectedResult,
                        true );

                // INITCAP(string) With String concatenation
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Firstfourth" },
                        new Object[]{ 2, "Secondfifth" },
                        new Object[]{ 3, "Thirdsixth" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id , INITCAP(S.DataA||T.DataB) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );
            }
        }
    }

}
