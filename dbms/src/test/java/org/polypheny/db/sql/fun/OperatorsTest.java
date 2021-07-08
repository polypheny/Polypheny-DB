/*
 * Copyright 2019-2021 The Polypheny Project
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@Slf4j
public class OperatorsTest {


    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestData();
    }


    private static void addTestData() throws SQLException {
        try (JdbcConnection jdbcConnection = new JdbcConnection(false)) {
            Connection connection = jdbcConnection.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE TestTable( ID INTEGER NOT NULL,NumberData INTEGER,TextData VARCHAR(20), PRIMARY KEY (ID) )");
                statement.executeUpdate("INSERT INTO TestTable VALUES (0, 0,  'dataA')");
                statement.executeUpdate("INSERT INTO TestTable VALUES (1, 10, 'dataB')");
                statement.executeUpdate("INSERT INTO TestTable VALUES (2, 1,  'dataC')");
                statement.executeUpdate("CREATE TABLE TestTableB( ID INTEGER NOT NULL,NumberData INTEGER, TextDataA VARCHAR(20), TextDataB VARCHAR(20), PRIMARY KEY (ID) )");
                statement.executeUpdate("INSERT INTO TestTableB VALUES (4, 2,  'dataA','dataA')");
                statement.executeUpdate("INSERT INTO TestTableB VALUES (5, 11, 'dataB','dataD')");
                statement.executeUpdate("INSERT INTO TestTableB VALUES (6, 7,  'dataC', 'dataE')");
                connection.commit();
            }
        }
    }


    @AfterClass
    public static void stop() throws SQLException {
        try (JdbcConnection jdbcConnection = new JdbcConnection(true)) {
            Connection connection = jdbcConnection.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP TABLE TestTable");
                statement.executeUpdate("DROP TABLE TestTableB");
            }
        }
    }

    // --------------- Tests ---------------


    @Test
    public void comparsionOperatorsTest() throws SQLException {
        try (TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection(true)) {
            Connection connection = polyphenyDbConnection.getConnection();

            try (Statement statement = connection.createStatement()) {
                //Equals (=)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{0, "dataA"}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable AS A Where A.ID=A.NumberData"),
                        expectedResult
                );

                //Not Equal (<>)
                List<Object[]> expectedResult1 = ImmutableList.of(
                        new Object[]{1, "dataB"},
                        new Object[]{2, "dataC"}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable AS A Where A.ID<>A.NumberData"),
                        expectedResult1
                );

                //NOT Equal(!=) only available at some conformance levels
                //                        List<Object[]> expectedResult2 = ImmutableList.of(
                //                        new Object[]{1, "dataB"},
                //                        new Object[]{2, "dataC"}
                //
                //                );
                //
                //                TestHelper.checkResultSet(
                //                        statement.executeQuery("SELECT ID, TextData FROM TestTable AS A Where A.ID!=A.NumberData"),
                //                        expectedResult2
                //                );

                //Greater than or Equal (>=)
                List<Object[]> expectedResult3 = ImmutableList.of(
                        new Object[]{0, "dataA"},
                        new Object[]{1, "dataB"}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable AS A Where A.ID >= A.NumberData"),
                        expectedResult3
                );

                //Less than or Equal (<=)
                List<Object[]> expectedResult4 = ImmutableList.of(
                        new Object[]{0, "dataA"},
                        new Object[]{2, "dataC"}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable AS A Where A.ID <= A.NumberData"),
                        expectedResult4
                );


                //Not Null (IS NOT NULL)
                List<Object[]> expectedResult5 = ImmutableList.of(
                        new Object[]{0, "dataA"},
                        new Object[]{1, "dataB"},
                        new Object[]{2, "dataC"}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable AS A Where A.NumberData IS NOT NULL"),
                        expectedResult5
                );


                //Null (IS NULL)
                List<Object[]> expectedResult6 = ImmutableList.of(

                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable AS A Where A.NumberData IS NULL"),
                        expectedResult6
                );

                //DISTINCT (IS DISTINCT FROM)
                List<Object[]> expectedResult7 = ImmutableList.of(
                        new Object[]{1, "dataB"},
                        new Object[]{2, "dataC"}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable AS A Where A.NumberData IS DISTINCT FROM ID"),
                        expectedResult7
                );

                //NOT DISTINCT (IS NOT DISTINCT FROM)
                List<Object[]> expectedResult8 = ImmutableList.of(
                        new Object[]{0, "dataA"}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable AS A Where A.NumberData IS NOT DISTINCT FROM ID"),
                        expectedResult8
                );
                //BETWEEN (BETWEEN VALUE1 AND VALUE2)
                List<Object[]> expectedResult9 = ImmutableList.of(
                        new Object[]{1, "dataB"},
                        new Object[]{2, "dataC"}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable AS A Where A.NumberData BETWEEN 1 AND 11"),
                        expectedResult9
                );


                //NOT BETWEEN (NOT BETWEEN VALUE1 AND VALUE2)
                List<Object[]> expectedResult10 = ImmutableList.of(
                        new Object[]{0, "dataA"}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable AS A Where A.NumberData NOT BETWEEN 1 AND 11"),
                        expectedResult10
                );

                //LIKE (string1 LIKE string 2)
                List<Object[]> expectedResult11 = ImmutableList.of(
                        new Object[]{5, "dataA"}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextDataA FROM TestTableB AS B Where B.TextDataA LIKE B.TextDataB"),
                        expectedResult11
                );

                //NOT LIKE (string1 NOT LIKE string 2)
                List<Object[]> expectedResult12 = ImmutableList.of(
                        new Object[]{6, "dataB"},
                        new Object[]{7, "dataC"}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextDataA FROM TestTableB AS B Where B.TextDataA NOT LIKE B.TextDataB"),
                        expectedResult12
                );


                //NOT WORKING
                //SIMILAR(string1 SIMILAR TO string 2)
//                List<Object[]> expectedResult13 = ImmutableList.of(
//                        new Object[]{5, "data"},
//                        new Object[]{6, "data"},
//                        new Object[]{7, "data"}
//                );
//
//                TestHelper.checkResultSet(
//                        statement.executeQuery("SELECT ID, TextDataB FROM TestTableB AS B WHERE TextDataA SIMILAR TO TextDataB"),
//                        expectedResult13
//                );

                //NOT WORKING
                //NOT SIMILAR(string1 NOT SIMILAR TO string 2)
//                List<Object[]> expectedResult14 = ImmutableList.of(
//                );
//
//                TestHelper.checkResultSet(
//                        statement.executeQuery("SELECT ID, TextDataA FROM TestTableB AS B Where B.TextDataA NOT SIMILAR TO B.TextDataB"),
//                        expectedResult14
//                );


            }
        }

    }


    @Test
    public void logicalOperators() throws SQLException {
        try (TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection(true)) {
            Connection connection = polyphenyDbConnection.getConnection();

            try (Statement statement = connection.createStatement()) {

                //OR(boolean1 OR boolean2)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{0, "DataA"},
                        new Object[]{1, "DataB"},
                        new Object[]{2, "DataC"}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable Where TRUE or FALSE"),
                        expectedResult
                );


                //AND(boolean1 AND boolean2)
                List<Object[]> expectedResult1 = ImmutableList.of(

                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable Where TRUE AND FALSE"),
                        expectedResult1
                );

                //NOT(NOT boolean)
                List<Object[]> expectedResult2 = ImmutableList.of(
                        new Object[]{0, "DataA"},
                        new Object[]{1, "DataB"},
                        new Object[]{2, "DataC"}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable Where NOT FALSE "),
                        expectedResult2
                );


                //IS FALSE(boolean is FALSE)
                List<Object[]> expectedResult3 = ImmutableList.of(

                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable Where TRUE is FALSE"),
                        expectedResult3
                );


                //IS NOT FALSE(boolean is NOT FALSE)
                List<Object[]> expectedResult4 = ImmutableList.of(
                        new Object[]{0, "DataA"},
                        new Object[]{1, "DataB"},
                        new Object[]{2, "DataC"}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable Where TRUE IS NOT FALSE "),
                        expectedResult4
                );


                //IS TRUE(boolean is TRUE)
                List<Object[]> expectedResult5 = ImmutableList.of(
                        new Object[]{0, "DataA"},
                        new Object[]{1, "DataB"},
                        new Object[]{2, "DataC"}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable Where TRUE IS TRUE"),
                        expectedResult4
                );

                //NOT TRUE(boolean is NOT TRUE)
                List<Object[]> expectedResult6 = ImmutableList.of(
                        new Object[]{0, "DataA"},
                        new Object[]{1, "DataB"},
                        new Object[]{2, "DataC"}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, TextData FROM TestTable Where FALSE IS NOT TRUE"),
                        expectedResult6
                );


            }

        }

    }

    @Test
    public void arithmeticOperators() throws SQLException {
        try (TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection(true)) {
            Connection connection = polyphenyDbConnection.getConnection();

            try (Statement statement = connection.createStatement()) {

                // + (numeric1 + numeric2)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{0, 6},
                        new Object[]{1, 16},
                        new Object[]{2, 13}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, NumberData + ID FROM TestTableB"),
                        expectedResult
                );


                // - (numeric1 - numeric2)
                List<Object[]> expectedResult1 = ImmutableList.of(
                        new Object[]{0, 2},
                        new Object[]{1, -6},
                        new Object[]{2, -1}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, -(NumberData - ID) FROM TestTableB"),
                        expectedResult1
                );

                // * (numeric1 * numeric2)
                List<Object[]> expectedResult2 = ImmutableList.of(
                        new Object[]{0, 8},
                        new Object[]{1, 55},
                        new Object[]{2, 42}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, NumberData*ID FROM TestTableB"),
                        expectedResult2
                );


                // / (numeric/numeric2)
                List<Object[]> expectedResult3 = ImmutableList.of(
                        new Object[]{0, 2},
                        new Object[]{1, 0},
                        new Object[]{2, 7}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, ID/NumberData FROM TestTableB"),
                        expectedResult3
                );


                //% (numeric1 % numeric2)
                List<Object[]> expectedResult4 = ImmutableList.of(
                        new Object[]{0, 0},
                        new Object[]{1, 1},
                        new Object[]{2, 1}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, NumberData/ID FROM TestTableB"),
                        expectedResult4
                );

            }

        }

    }


}

