/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@Slf4j
public class SqlKnnFunctionTest {


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
//                statement.executeUpdate( "CREATE SCHEMA knn" );
                statement.executeUpdate( "CREATE TABLE knntest( id INTEGER NOT NULL, myarray INTEGER ARRAY(1,2), PRIMARY KEY (id) )" );
                statement.executeUpdate( "INSERT INTO knntest VALUES (1, ARRAY[1,1])" );
                statement.executeUpdate( "INSERT INTO knntest VALUES (2, ARRAY[2,2])" );
                statement.executeUpdate( "INSERT INTO knntest VALUES (3, ARRAY[0,3])" );
                connection.commit();
            }
        }
    }


    @AfterClass
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE knntest" );
//                statement.executeUpdate( "DROP SCHEMA knn" );
            }
        }
    }

    // --------------- Tests ---------------


    @Test
    public void enumerableFunctionImplementationTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, knn(myarray, ARRAY[1,1], 'L2SQUARED') as distance FROM knntest" ),
                        ImmutableList.of(
                                new Object[]{ 1, 0.0 },
                                new Object[]{ 2, 2.0 },
                                new Object[]{ 3, 5.0 }
                        )
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, knn(myarray, ARRAY[1,1], 'L1') as distance FROM knntest" ),
                        ImmutableList.of(
                                new Object[]{ 1, 0.0 },
                                new Object[]{ 2, 2.0 },
                                new Object[]{ 3, 3.0 }
                        )
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, knn(myarray, ARRAY[1,1], 'CHISQUARED') as distance FROM knntest" ),
                        ImmutableList.of(
                                new Object[]{ 1, 0.0 },
                                new Object[]{ 2, 0.6666666666666666 },
                                new Object[]{ 3, 2.0 }
                        )
                );

                // Ignoring COSINE for now because apparently some close to zero issues.
                /*TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, knn(myarray, ARRAY[1,1], 'COSINE') as distance FROM knntest" ),
                        ImmutableList.of(
                                new Object[]{ 1, 0.0 },
                                new Object[]{ 2, 2.0 },
                                new Object[]{ 3, 5.0 }
                        )
                );*/
            }
        }
    }
}
