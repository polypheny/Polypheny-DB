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
public class OperatorPrecedenceTest {


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
                // Create table
                statement.executeUpdate( "CREATE TABLE PrecedenceTest (id INT not null, val INT, txt VARCHAR(50), arr INTEGER ARRAY(1,2), primary key (id))" );

                // Insert test data
                statement.executeUpdate( "INSERT INTO PrecedenceTest (id, val, txt, arr) VALUES (1, 10, 'A', ARRAY[1, 2])" );
                statement.executeUpdate( "INSERT INTO PrecedenceTest (id, val, txt, arr) VALUES (2, 20, 'B', ARRAY[3, 4])" );
                statement.executeUpdate( "INSERT INTO PrecedenceTest (id, val, txt, arr) VALUES (3, 30, 'C', ARRAY[5, 6])" );

                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( TestHelper.JdbcConnection jdbcConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // DROP TABLEs
                statement.executeUpdate( "DROP TABLE PrecedenceTest" );
            }

            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    public void unaryAndBinaryOperatorsPrecedenceTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 9 }, // 10 - 1
                        new Object[]{ 2, 19 }, // 20 - 1
                        new Object[]{ 3, 29 }  // 30 - 1
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, val - +1 FROM PrecedenceTest" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void betweenOperatorPrecedenceTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, true },
                        new Object[]{ 2, false },
                        new Object[]{ 3, false }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, val * 2 BETWEEN 20 AND 40 FROM PrecedenceTest" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void logicalOperatorsPrecedenceTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, true },
                        new Object[]{ 2, true },
                        new Object[]{ 3, false }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, val < 15 OR val > 15 AND val < 25 FROM PrecedenceTest" ),
                        expectedResult,
                        true
                );
            }
        }
    }


}
