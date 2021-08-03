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
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.excluded.CassandraExcluded;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
@Slf4j
@Category({AdapterTestSuite.class, CassandraExcluded.class})
public class LogExponentialFunctionsTest {


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
                statement.executeUpdate("CREATE TABLE TableDecimal( ID INTEGER NOT NULL,Data DECIMAL(2,1), PRIMARY KEY (ID) )");
                statement.executeUpdate("INSERT INTO TableDecimal VALUES (0, -2.0)");
                statement.executeUpdate("INSERT INTO TableDecimal VALUES (1, 3.0)");
                statement.executeUpdate("INSERT INTO TableDecimal VALUES (2, 4.0)");

                statement.executeUpdate("CREATE TABLE TableDouble( ID INTEGER NOT NULL, Data DOUBLE , PRIMARY KEY (ID) )");
                statement.executeUpdate("INSERT INTO TableDouble VALUES (0, 2.0)");
                statement.executeUpdate("INSERT INTO TableDouble VALUES (1, -3.0)");
                statement.executeUpdate("INSERT INTO TableDouble VALUES (2, 4.0)");

                statement.executeUpdate("CREATE TABLE TableInteger( ID INTEGER NOT NULL, Data INTEGER, PRIMARY KEY (ID) )");
                statement.executeUpdate("INSERT INTO TableInteger VALUES (0, 2)");
                statement.executeUpdate("INSERT INTO TableInteger VALUES (1, 3)");
                statement.executeUpdate("INSERT INTO TableInteger VALUES (2, -4)");


                connection.commit();
            }
        }
    }


    @AfterClass
    public static void stop() throws SQLException {
        try (JdbcConnection jdbcConnection = new JdbcConnection(true)) {
            Connection connection = jdbcConnection.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP TABLE TableDecimal");
                statement.executeUpdate("DROP TABLE TableDouble");
                statement.executeUpdate("DROP TABLE TableInteger");
            }
            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Ignore
    @Test
    public void logTest() throws SQLException {
        try (TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection(true)) {
            Connection connection = polyphenyDbConnection.getConnection();

            try (Statement statement = connection.createStatement()) {

                //For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{1, 0.477121254719662435395122201953199692070484161376953125},
                        new Object[]{2, 0.60205999132796239603493404501932673156261444091796875}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, LN(Data) FROM TableDecimal where data > 0"),
                        expectedResult
                );

                //For Double
                expectedResult = ImmutableList.of(
                        new Object[]{0, 0.3010299956639812},
                        new Object[]{2, 0.6020599913279624}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, LN(Data) FROM TableDouble where data > 0"),
                        expectedResult
                );

//
//                //For Integer
//                expectedResult = ImmutableList.of(
//                        new Object[]{0, 0.3010299956639812},
//                        new Object[]{1, 0.47712125471966244}
//
//
//                );
//
//                TestHelper.checkResultSet(
//                        statement.executeQuery("SELECT ID, LN(Data) FROM TableInteger where Data > 0"),
//                        expectedResult
//                );


            }
        }

    }

    @Test
    public void exponentialTest() throws SQLException {
        try (TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection(true)) {
            Connection connection = polyphenyDbConnection.getConnection();

            try (Statement statement = connection.createStatement()) {

                //For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{0, 0.13533528323661270231781372785917483270168304443359375},
                        new Object[]{1, 20.08553692318766792368478490971028804779052734375},
                        new Object[]{2, 54.59815003314423620395245961844921112060546875}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT ID, EXP(Data) FROM TableDecimal"),
                        expectedResult
                );

//                //For Double
//                expectedResult = ImmutableList.of(
//                        new Object[]{0, 7.389056},
//                        new Object[]{1, 0.049787},
//                        new Object[]{2, 54.59815}
//                );
//
//                TestHelper.checkResultSet(
//                        statement.executeQuery("SELECT ID, ROUND(EXP(Data),6) FROM TableDouble"),
//                        expectedResult
//                );
//
//
//                //For Integer
//                expectedResult = ImmutableList.of(
//                        new Object[]{0, 7.38905609893065},
//                        new Object[]{1, 20.085536923187668},
//                        new Object[]{2, 0.01831563888873418}
//
//                );
//
//                TestHelper.checkResultSet(
//                        statement.executeQuery("SELECT ID, ROUND(EXP(Data),6) FROM TableInteger"),
//                        expectedResult
//                );


            }
        }

    }


}

