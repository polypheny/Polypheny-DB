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

package org.polypheny.db.sql.clause;


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


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Category({ AdapterTestSuite.class, CassandraExcluded.class })
public class JoinTest {


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
                statement.executeUpdate( "CREATE TABLE TableA(ID VARCHAR(255) NOT NULL, NAME VARCHAR(255), AMOUNT INTEGER, PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TableA VALUES ('Ab', 'Name1', 10000.00)" );
                statement.executeUpdate( "INSERT INTO TableA VALUES ('Bc', 'Name2',  5000.00)" );
                statement.executeUpdate( "INSERT INTO TableA VALUES ('Cd', 'Name3',  7000.00)" );

                connection.commit();
            }
        }
    }


    @AfterClass
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE TableA" );
            }
            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    public void naturalJoinTests() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Name1", "Ab", 10000 },
                        new Object[]{ "Name2", "Bc", 5000 },
                        new Object[]{ "Name3", "Cd", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S NATURAL JOIN (SELECT name, Amount  FROM TableA) AS T" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void innerJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", "Name1", 10000 },
                        new Object[]{ "Bc", "Name2", "Name2", 5000 },
                        new Object[]{ "Cd", "Name3", "Name3", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S INNER JOIN (SELECT name, Amount  FROM TableA) AS T ON S.name = T.name" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void leftJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", "Name1", 10000 },
                        new Object[]{ "Bc", "Name2", "Name2", 5000 },
                        new Object[]{ "Cd", "Name3", "Name3", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S LEFT JOIN (SELECT name, Amount  FROM TableA) AS T ON S.name = T.name" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void rightJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", "Name1", 10000 },
                        new Object[]{ "Bc", "Name2", "Name2", 5000 },
                        new Object[]{ "Cd", "Name3", "Name3", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S RIGHT JOIN (SELECT name, Amount  FROM TableA) AS T ON S.name = T.name" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void fullJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", "Name1", 10000 },
                        new Object[]{ "Bc", "Name2", "Name2", 5000 },
                        new Object[]{ "Cd", "Name3", "Name3", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S FULL JOIN (SELECT name, Amount  FROM TableA) AS T ON S.name = T.name" ),
                        expectedResult,
                        true );
            }
        }
    }

}
