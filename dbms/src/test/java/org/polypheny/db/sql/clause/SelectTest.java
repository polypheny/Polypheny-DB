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
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@Slf4j
public class SelectTest {


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
                statement.executeUpdate("CREATE TABLE TableA(ID INTEGER NOT NULL, NAME VARCHAR(20), AGE INTEGER, PRIMARY KEY (ID))");
                statement.executeUpdate("INSERT INTO TableA VALUES (12, 'Name1', 60)");
                statement.executeUpdate("INSERT INTO TableA VALUES (15, 'Name2', 24)");
                statement.executeUpdate("INSERT INTO TableA VALUES (99, 'Name3', 11)");
                statement.executeUpdate("CREATE TABLE TableB(  ID INTEGER NOT NULL, NAME VARCHAR(20), AGE INTEGER, PRIMARY KEY (ID) )");
                statement.executeUpdate("INSERT INTO TableB VALUES (15, 'Name2', 24)");
                statement.executeUpdate("INSERT INTO TableB VALUES (25, 'Name4', 40)");
                statement.executeUpdate("INSERT INTO TableB VALUES (98, 'Name3', 20)");
                statement.executeUpdate("CREATE TABLE TableC(  ID INTEGER NOT NULL, category VARCHAR(2), Tid BIGINT, PRIMARY KEY (ID) )");
                statement.executeUpdate("INSERT INTO TableC VALUES (15, 'AB', 8200)");
                statement.executeUpdate("INSERT INTO TableC VALUES (25, 'AB', 8201)");
                statement.executeUpdate("INSERT INTO TableC VALUES (98, 'AC', 8203)");
                statement.executeUpdate("INSERT INTO TableC VALUES (98, 'SC', 8204)");
                statement.executeUpdate("INSERT INTO TableC VALUES (98, 'AC', 8202)");


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
                statement.executeUpdate("DROP TABLE TableB");
                statement.executeUpdate("DROP TABLE TableC");
                //   statement.executeUpdate( "DROP TABLE trigotestinteger" );
            }
        }
    }

    // --------------- Tests ---------------

    @Ignore
    @Test
    public void nestedSelect() throws SQLException {
        try (TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection(true)) {
            Connection connection = polyphenyDbConnection.getConnection();

            try (Statement statement = connection.createStatement()) {


                List<Object[]> expectedResult1 = ImmutableList.of(
                        new Object[]{12, "Name1"},
                        new Object[]{15, "Name2"}
                );

//'All' key word has to be used After > sign

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT TableA.id, TableA.name FROM  TableA WHERE  TableA.age >  SELECT age FROM TableB WHERE TableB.name = 'Name3' "),
                        expectedResult1
                );


            }

        }

    }

    @Test
    public void existsSelect() throws SQLException {
        try (TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection(true)) {
            Connection connection = polyphenyDbConnection.getConnection();

            try (Statement statement = connection.createStatement()) {


                List<Object[]> expectedResult1 = ImmutableList.of(
                        new Object[]{15},
                        new Object[]{20}


                );


                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT id FROM TABLEC WHERE category='AC' AND EXISTS (SELECT *  FROM TABLEB WHERE age > 19 AND TABLEB.id = TABLEC.id)"),
                        expectedResult1
                );


            }

        }

    }


}

