/*
 * Copyright 2019-2022 The Polypheny Project
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.db.excluded.FileExcluded;
import org.polypheny.db.excluded.MonetdbExcluded;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Category({ AdapterTestSuite.class, CassandraExcluded.class })
public class OperatorsTest {


    @BeforeClass
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
                statement.executeUpdate( "CREATE TABLE TestTable( ID INTEGER NOT NULL,NumberData INTEGER,TextData VARCHAR(20), PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO TestTable VALUES (0, 0,  'dataA')" );
                statement.executeUpdate( "INSERT INTO TestTable VALUES (1, 10, 'dataB')" );
                statement.executeUpdate( "INSERT INTO TestTable VALUES (2, 1,  'dataC')" );

                statement.executeUpdate( "CREATE TABLE TestTableB( ID INTEGER NOT NULL,NumberData INTEGER, TextDataA VARCHAR(20), TextDataB VARCHAR(20), PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO TestTableB VALUES (4, 2,  'dataA','dataA')" );
                statement.executeUpdate( "INSERT INTO TestTableB VALUES (5, 11, 'dataB','dataD')" );
                statement.executeUpdate( "INSERT INTO TestTableB VALUES (6, 7,  'dataC', 'dataE')" );
                connection.commit();
            }
        }
    }


    @AfterClass
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE TestTable" );
                statement.executeUpdate( "DROP TABLE TestTableB" );
            }
            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    @Category({ FileExcluded.class, MonetdbExcluded.class })
    public void comparisonOperatorsTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Equals (=)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, "dataA" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable AS A Where A.ID=A.NumberData" ),
                        expectedResult,
                        true
                );

                // Not Equal (<>)
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "dataB" },
                        new Object[]{ 2, "dataC" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable AS A Where A.ID<>A.NumberData" ),
                        expectedResult,
                        true
                );

                // NOT Equal(!=)
                List<Object[]> expectedResult2 = ImmutableList.of(
                        new Object[]{ 1, "dataB" },
                        new Object[]{ 2, "dataC" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable AS A Where A.ID != A.NumberData" ),
                        expectedResult2
                );

                // Greater than or Equal (>=)
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, "dataA" },
                        new Object[]{ 2, "dataC" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable AS A Where A.ID >= A.NumberData" ),
                        expectedResult,
                        true
                );

                // Less than or Equal (<=)
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, "dataA" },
                        new Object[]{ 1, "dataB" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable AS A Where A.ID <= A.NumberData" ),
                        expectedResult,
                        true
                );

                // Not Null (IS NOT NULL)
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, "dataA" },
                        new Object[]{ 1, "dataB" },
                        new Object[]{ 2, "dataC" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable AS A Where A.NumberData IS NOT NULL" ),
                        expectedResult,
                        true
                );

                // Null (IS NULL)
                expectedResult = ImmutableList.of(

                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable AS A Where A.NumberData IS NULL" ),
                        expectedResult
                );

                // DISTINCT (IS DISTINCT FROM)
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "dataB" },
                        new Object[]{ 2, "dataC" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable AS A Where A.NumberData IS DISTINCT FROM ID" ),
                        expectedResult,
                        true
                );

                // NOT DISTINCT (IS NOT DISTINCT FROM)
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, "dataA" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable AS A Where A.NumberData IS NOT DISTINCT FROM ID" ),
                        expectedResult,
                        true
                );

                // BETWEEN (BETWEEN VALUE1 AND VALUE2)
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "dataB" },
                        new Object[]{ 2, "dataC" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable AS A Where A.NumberData BETWEEN 1 AND 11" ),
                        expectedResult,
                        true
                );

                // NOT BETWEEN (NOT BETWEEN VALUE1 AND VALUE2)
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, "dataA" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable AS A Where A.NumberData NOT BETWEEN 1 AND 11" ),
                        expectedResult,
                        true
                );

                // LIKE (string1 LIKE string 2)
                expectedResult = ImmutableList.of(
                        new Object[]{ 4, "dataA" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextDataA FROM TestTableB AS B Where B.TextDataA LIKE B.TextDataB" ),
                        expectedResult,
                        true
                );

                // NOT LIKE (string1 NOT LIKE string 2)
                expectedResult = ImmutableList.of(
                        new Object[]{ 5, "dataB" },
                        new Object[]{ 6, "dataC" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextDataA FROM TestTableB AS B Where B.TextDataA NOT LIKE B.TextDataB" ),
                        expectedResult,
                        true
                );

                // LIKE (string1 LIKE string 2) ESCAPE escape_character
                expectedResult = ImmutableList.of(
                        new Object[]{ 4, "dataA" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextDataA FROM TestTableB AS B Where B.TextDataA LIKE B.TextDataB ESCAPE '%' " ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    @Category(FileExcluded.class)
    public void logicalOperators() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // OR(boolean1 OR boolean2)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, "dataA" },
                        new Object[]{ 1, "dataB" },
                        new Object[]{ 2, "dataC" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable Where TRUE or FALSE" ),
                        expectedResult,
                        true
                );

                // AND(boolean1 AND boolean2)
                expectedResult = ImmutableList.of();
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable Where TRUE AND FALSE" ),
                        expectedResult
                );

                // NOT(NOT boolean)
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, "dataA" },
                        new Object[]{ 1, "dataB" },
                        new Object[]{ 2, "dataC" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable Where NOT FALSE " ),
                        expectedResult,
                        true
                );

                // IS FALSE (boolean IS FALSE)
                expectedResult = ImmutableList.of();
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable Where TRUE is FALSE" ),
                        expectedResult
                );

                // IS TRUE (boolean IS TRUE)
                expectedResult = ImmutableList.of();
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable Where FALSE is TRUE" ),
                        expectedResult
                );

                // IS NOT FALSE(boolean is NOT FALSE)
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, "dataA" },
                        new Object[]{ 1, "dataB" },
                        new Object[]{ 2, "dataC" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable Where TRUE IS NOT FALSE " ),
                        expectedResult,
                        true
                );

                // IS TRUE(boolean is TRUE)
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, "dataA" },
                        new Object[]{ 1, "dataB" },
                        new Object[]{ 2, "dataC" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable Where TRUE IS TRUE" ),
                        expectedResult,
                        true
                );

                // NOT TRUE(boolean is NOT TRUE)
                expectedResult = ImmutableList.of(
                        new Object[]{ 0, "dataA" },
                        new Object[]{ 1, "dataB" },
                        new Object[]{ 2, "dataC" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, TextData FROM TestTable Where FALSE IS NOT TRUE" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    @Category(FileExcluded.class)
    public void arithmeticOperators() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // + (numeric1 + numeric2)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 4, 6 },
                        new Object[]{ 5, 16 },
                        new Object[]{ 6, 13 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, NumberData + ID FROM TestTableB" ),
                        expectedResult,
                        true
                );

                // - (numeric1 - numeric2)
                expectedResult = ImmutableList.of(
                        new Object[]{ 4, 2 },
                        new Object[]{ 5, -6 },
                        new Object[]{ 6, -1 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, -(NumberData - ID) FROM TestTableB" ),
                        expectedResult,
                        true
                );

                // * (numeric1 * numeric2)
                expectedResult = ImmutableList.of(
                        new Object[]{ 4, 8 },
                        new Object[]{ 5, 55 },
                        new Object[]{ 6, 42 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, NumberData*ID FROM TestTableB" ),
                        expectedResult,
                        true
                );

                // / (numeric/numeric2)
                expectedResult = ImmutableList.of(
                        new Object[]{ 4, 2 },
                        new Object[]{ 5, 0 },
                        new Object[]{ 6, 0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, ID/NumberData FROM TestTableB" ),
                        expectedResult,
                        true
                );

                // % (numeric1 % numeric2)
                List<Object[]> expectedResult4 = ImmutableList.of(
                        new Object[]{ 4, 2 },
                        new Object[]{ 5, 1 },
                        new Object[]{ 6, 1 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, NumberData%ID FROM TestTableB" ),
                        expectedResult4
                );
            }
        }
    }


    @Test
    public void randTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // RAND([seed])
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0.1181129375521941 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "Select Rand(5)" ),
                        expectedResult,
                        true
                );

                // RAND_INTEGER([seed, ] numeric)
                expectedResult = ImmutableList.of(
                        new Object[]{ 2 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT RAND_INTEGER(5,5)" ),
                        expectedResult,
                        true
                );
            }
        }
    }

}
