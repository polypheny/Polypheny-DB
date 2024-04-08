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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
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
public class SystemVariablesTest {


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
                statement.executeUpdate( "CREATE TABLE PiTestTable (id INT not null, piValue DOUBLE, primary key(id))" );
                // Insert Pi values with various degrees of precision
                statement.executeUpdate( "INSERT INTO PiTestTable (id, piValue) VALUES (1, 3.1415927)" );

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
                statement.executeUpdate( "DROP TABLE PiTestTable" );
            }

            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    public void testRowConstructor() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "pa", "pa", System.getProperty( "user.name" ) }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT USER, CURRENT_USER, SYSTEM_USER" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    // The problem is, that Polyphenys PI constant is not rewritten for the data store.
    @Test
    public void testPiConstant() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Considering a tolerance level for being 'close enough'
                double tolerance = 0.0001;

                // Query to select the Pi value that is within the defined tolerance
                String query = String.format(
                        "SELECT piValue FROM PiTestTable WHERE ABS(piValue - PI) < %f", tolerance );

                try ( ResultSet resultSet = statement.executeQuery( query ) ) {
                    assertTrue( resultSet.next(), "No Pi value found within the tolerance." );
                    double selectedPi = resultSet.getDouble( 1 );
                    // Check if the selected Pi value is close enough to the actual Pi value
                    assertEquals( Math.round( selectedPi * 10000000 ), Math.round( Math.PI * 10000000 ), "The selected Pi value is not close enough to the actual Pi value." );
                }
            }
        }
    }

}
