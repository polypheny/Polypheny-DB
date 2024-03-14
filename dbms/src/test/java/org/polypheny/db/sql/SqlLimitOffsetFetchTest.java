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

package org.polypheny.db.sql;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Tag("adapter")
public class SqlLimitOffsetFetchTest {

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
                statement.executeUpdate( "CREATE TABLE limittest( id INTEGER NOT NULL, name VARCHAR(39), foo INTEGER, PRIMARY KEY (id))" );
                statement.executeUpdate( "INSERT INTO limittest VALUES (1, 'Hans', 5)" );
                statement.executeUpdate( "INSERT INTO limittest VALUES (2, 'Alice', 7)" );
                statement.executeUpdate( "INSERT INTO limittest VALUES (3, 'Bob', 4)" );
                statement.executeUpdate( "INSERT INTO limittest VALUES (4, 'Saskia', 6)" );
                statement.executeUpdate( "INSERT INTO limittest VALUES (5, 'Rebecca', 3)" );
                statement.executeUpdate( "INSERT INTO limittest VALUES (6, 'Georg', 9)" );
                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE limittest" );
            }
        }
    }

    // --------------- Tests ---------------


    @Test
    public void simpleLimitTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Hans", 5 },
                        new Object[]{ 2, "Alice", 7 },
                        new Object[]{ 3, "Bob", 4 },
                        new Object[]{ 4, "Saskia", 6 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY id LIMIT 4" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Hans", 5 },
                        new Object[]{ 2, "Alice", 7 },
                        new Object[]{ 3, "Bob", 4 },
                        new Object[]{ 4, "Saskia", 6 },
                        new Object[]{ 5, "Rebecca", 3 },
                        new Object[]{ 6, "Georg", 9 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY id LIMIT 8" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Hans", 5 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY id LIMIT 1" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Hans", 5 },
                        new Object[]{ 2, "Alice", 7 },
                        new Object[]{ 3, "Bob", 4 },
                        new Object[]{ 4, "Saskia", 6 },
                        new Object[]{ 5, "Rebecca", 3 },
                        new Object[]{ 6, "Georg", 9 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY id LIMIT ALL" ),
                        expectedResult
                );
            }
        }
    }


    @Test
    public void orderLimitTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 5, "Rebecca", 3 },
                        new Object[]{ 3, "Bob", 4 },
                        new Object[]{ 1, "Hans", 5 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY foo LIMIT 3" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 6, "Georg", 9 },
                        new Object[]{ 2, "Alice", 7 },
                        new Object[]{ 4, "Saskia", 6 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY foo desc LIMIT 3" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 5, "Rebecca", 3 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY foo LIMIT 1" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 5, "Rebecca", 3 },
                        new Object[]{ 3, "Bob", 4 },
                        new Object[]{ 1, "Hans", 5 },
                        new Object[]{ 4, "Saskia", 6 },
                        new Object[]{ 2, "Alice", 7 },
                        new Object[]{ 6, "Georg", 9 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY foo LIMIT 8" ),
                        expectedResult
                );
            }
        }
    }


    @Test
    public void projectedLimitTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Hans" },
                        new Object[]{ 2, "Alice" },
                        new Object[]{ 3, "Bob" },
                        new Object[]{ 4, "Saskia" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, name FROM limittest ORDER BY id LIMIT 4" ),
                        expectedResult
                );
            }
        }
    }


    @Test
    public void startLimitTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 4, "Saskia", 6 },
                        new Object[]{ 5, "Rebecca", 3 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY id LIMIT 3,2" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 4, "Saskia", 6 },
                        new Object[]{ 5, "Rebecca", 3 },
                        new Object[]{ 6, "Georg", 9 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY id LIMIT 3,25" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Hans", 5 },
                        new Object[]{ 2, "Alice", 7 },
                        new Object[]{ 3, "Bob", 4 },
                        new Object[]{ 4, "Saskia", 6 },
                        new Object[]{ 5, "Rebecca", 3 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY id LIMIT 0,5" ),
                        expectedResult
                );
            }
        }
    }


    @Test
    public void offsetLimitTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Hans", 5 },
                        new Object[]{ 4, "Saskia", 6 },
                        new Object[]{ 2, "Alice", 7 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY foo LIMIT 3 OFFSET 2" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 6, "Georg", 9 },
                        new Object[]{ 2, "Alice", 7 },
                        new Object[]{ 4, "Saskia", 6 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY foo desc LIMIT 3 OFFSET 0 ROWS" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY foo desc LIMIT 3 OFFSET 20" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 4, "Saskia", 6 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY foo desc LIMIT 1 OFFSET 2 ROW" ),
                        expectedResult
                );
            }
        }
    }


    @Test
    public void fetchTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Hans", 5 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY id FETCH FIRST 1 ROWS ONLY" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 6, "Georg", 9 },
                        new Object[]{ 2, "Alice", 7 },
                        new Object[]{ 4, "Saskia", 6 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY foo desc FETCH FIRST 3 ROWS ONLY" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 6, "Georg", 9 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY foo desc FETCH NEXT 1 ROW ONLY" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 6, "Georg", 9 },
                        new Object[]{ 2, "Alice", 7 },
                        new Object[]{ 4, "Saskia", 6 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY foo desc FETCH NEXT 3 ROWS ONLY" ),
                        expectedResult
                );

                expectedResult = ImmutableList.of(
                        new Object[]{ 4, "Saskia", 6 },
                        new Object[]{ 5, "Rebecca", 3 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM limittest ORDER BY id OFFSET 3 FETCH NEXT 2 ROWS ONLY" ),
                        expectedResult
                );
            }
        }
    }

    // TODO:
    //  INSERT with LIMIT, OFFSET, FETCH (?)
    //  UPDATE with LIMIT, OFFSET, FETCH (?)  --> Add to document if this is working
    //  DELETE with LIMIT, OFFSET, FETCH (?)  --> Add to document if this is working
    //  INNER SELECTS with LIMIT, OFFSET, FETCH

}

