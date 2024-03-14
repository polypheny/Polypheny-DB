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


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class CaseTest {


    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestData();
    }


    private static void addTestData() throws SQLException {
        try ( TestHelper.JdbcConnection jdbcConnection = new TestHelper.JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE CaseTestTable( ID INTEGER NOT NULL,ProductId INTEGER,Quantity INTEGER, PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO CaseTestTable VALUES (1,11,12)" );
                statement.executeUpdate( "INSERT INTO CaseTestTable VALUES (2,42,51)" );
                statement.executeUpdate( "INSERT INTO CaseTestTable VALUES (3,72,6)" );
                statement.executeUpdate( "INSERT INTO CaseTestTable VALUES (4,14,82)" );
                statement.executeUpdate( "INSERT INTO CaseTestTable VALUES (5,51,26)" );
                statement.executeUpdate( "INSERT INTO CaseTestTable VALUES (6,6,26)" );

                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( TestHelper.JdbcConnection jdbcConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE CaseTestTable" );
            }

            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    public void simpleCaseTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 11, 12, "The quantity is under 30       " },
                        new Object[]{ 2, 42, 51, "The quantity is greater than 30" },
                        new Object[]{ 3, 72, 6, "The quantity is under 30       " },
                        new Object[]{ 4, 14, 82, "The quantity is greater than 30" },
                        new Object[]{ 5, 51, 26, "The quantity is under 30       " },
                        new Object[]{ 6, 6, 26, "The quantity is under 30       " }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID,ProductID, Quantity, CASE WHEN Quantity > 30 THEN 'The quantity is greater than 30' WHEN Quantity = 30 THEN 'The quantity is 30' ELSE 'The quantity is under 30' END AS QuantityText FROM CaseTestTable" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void searchedCaseTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 11, 12, "The quantity is 12         " },
                        new Object[]{ 2, 42, 51, "The quantity is not 6 or 12" },
                        new Object[]{ 3, 72, 6, "The quantity is 6          " },
                        new Object[]{ 4, 14, 82, "The quantity is not 6 or 12" },
                        new Object[]{ 5, 51, 26, "The quantity is not 6 or 12" },
                        new Object[]{ 6, 6, 26, "The quantity is not 6 or 12" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID,ProductID, Quantity, CASE Quantity WHEN 6 THEN 'The quantity is 6' WHEN 12 THEN 'The quantity is 12' ELSE 'The quantity is not 6 or 12' END AS QuantityText FROM CaseTestTable ORDER BY ID" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void nullIfTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 11, 12, 1 },
                        new Object[]{ 2, 42, 51, 2 },
                        new Object[]{ 3, 72, 6, 3 },
                        new Object[]{ 4, 14, 82, 4 },
                        new Object[]{ 5, 51, 26, 5 },
                        new Object[]{ 6, 6, 26, null }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID,ProductID,Quantity, NULLIF(ID,ProductID) FROM CaseTestTable ORDER BY ID" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void coalesceTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // COALESCE(value, value [, value ]*) Returns first non null value if the first value is null
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 11, 12, 2 },
                        new Object[]{ 2, 42, 51, 2 },
                        new Object[]{ 3, 72, 6, 2 },
                        new Object[]{ 4, 14, 82, 2 },
                        new Object[]{ 5, 51, 26, 2 },
                        new Object[]{ 6, 6, 26, 2 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID,ProductID,Quantity, Coalesce(NULL,NULL,2,1) FROM CaseTestTable ORDER BY ID" ),
                        expectedResult,
                        true
                );

                // COALESCE(NULLIF(expr1,expr2),value2) with NULLIF INSIDE
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, 11, 12, 1 },
                        new Object[]{ 2, 42, 51, 2 },
                        new Object[]{ 3, 72, 6, 3 },
                        new Object[]{ 4, 14, 82, 4 },
                        new Object[]{ 5, 51, 26, 5 },
                        new Object[]{ 6, 6, 26, 1 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID,ProductID,Quantity, Coalesce(NULLIF(Id,ProductID),1) FROM CaseTestTable ORDER BY ID" ),
                        expectedResult,
                        true );
            }
        }
    }

}
