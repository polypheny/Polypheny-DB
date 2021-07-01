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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@Slf4j
public class DayTimeFunctionsTest {


    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestData();
    }


    //Inserting into table not working
    private static void addTestData() throws SQLException {
        try (JdbcConnection jdbcConnection = new JdbcConnection(false)) {
            Connection connection = jdbcConnection.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE TableA(ID INTEGER NOT NULL, StartDate DATE, PRIMARY KEY (ID))");
                statement.executeUpdate("INSERT INTO TableA VALUES (1, '2000-01-01')");
                statement.executeUpdate("INSERT INTO TableA VALUES (2, '2001-02-02')");
                statement.executeUpdate("INSERT INTO TableA VALUES (3, '2002-03-03')");

                connection.commit();
            }
        }
    }


    @AfterClass
    public static void stop() throws SQLException {
        try (JdbcConnection jdbcConnection = new JdbcConnection(true)) {
            Connection connection = jdbcConnection.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP TABLE TableA");
            }
        }
    }

    // --------------- Tests ---------------

    @Ignore
    @Test
    public void AssertDateTest() throws SQLException {
        try (TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection(true)) {
            Connection connection = polyphenyDbConnection.getConnection();

            try (Statement statement = connection.createStatement()) {


                List<Object[]> expectedResult1 = ImmutableList.of(
                        new Object[]{1, "2000-01-01"},
                        new Object[]{2, "2001-02-02"},
                        new Object[]{3, "2002-03-03"}
                );


                ResultSet rs;
                rs = statement.executeQuery("SELECT * FROM TableA");

                while (rs.next()) {

                    System.out.println(rs.getInt(2));
                }

            }

        }

    }


}

