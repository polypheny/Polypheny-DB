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

/**
 * SELECT col1, col2 FROM table ORDER BY col3
 * results in SELECT col1, col2, col3 FROM table ORDER BY col3
 * which is often not desired
 */
@Tag("adapter")
public class SqlSortExcludeTest {

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
                statement.executeUpdate( "CREATE TABLE sortBugTest( id INTEGER NOT NULL, name VARCHAR(39), foo INTEGER, PRIMARY KEY (id))" );
                statement.executeUpdate( "INSERT INTO sortBugTest VALUES (1, 'Hans', 5)" );
                statement.executeUpdate( "INSERT INTO sortBugTest VALUES (2, 'Alice', 7)" );
                statement.executeUpdate( "INSERT INTO sortBugTest VALUES (3, 'Bob', 4)" );
                statement.executeUpdate( "INSERT INTO sortBugTest VALUES (4, 'Saskia', 6)" );
                statement.executeUpdate( "INSERT INTO sortBugTest VALUES (5, 'Rebecca', 3)" );
                statement.executeUpdate( "INSERT INTO sortBugTest VALUES (6, 'Georg', 9)" );
                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE sortBugTest" );
            }
        }
    }


    @Test
    public void sortExcludeTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Hans", 5 },
                        new Object[]{ "Alice", 7 },
                        new Object[]{ "Bob", 4 },
                        new Object[]{ "Saskia", 6 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT name, foo FROM sortBugTest ORDER BY id LIMIT 4" ),
                        expectedResult
                );


            }
        }
    }


}
