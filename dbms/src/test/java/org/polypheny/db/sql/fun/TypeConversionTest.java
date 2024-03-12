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
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class TypeConversionTest {


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
                statement.executeUpdate( "CREATE TABLE ConversionTestTable( ID INTEGER NOT NULL,DecimalData DECIMAL(6,4),TextData VARCHAR(255),DoubleData Double,BooleanData BOOLEAN,DateData Date,TimeData TIME, PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO ConversionTestTable VALUES (1, 10.1324,'DataA',0.434,true,DATE '2021-07-17', TIME '02:37:55')" );
                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "Drop Table ConversionTestTable" );
            }
            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    public void castTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Cast Double to Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, BigDecimal.valueOf( 10.1324 ), "DataA", BigDecimal.valueOf( 0.434 ), true, Date.valueOf( "2021-07-17" ), Time.valueOf( "02:37:55" ) }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ID, DecimalData, TextData, CAST(DoubleData AS DECIMAL(4,3)), BooleanData, DateData, TimeData FROM ConversionTestTable" ),
                        expectedResult
                );

                // Cast Decimal to Double
                expectedResult = ImmutableList.of(
                        new Object[]{ 10.1324 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT  CAST(DecimalData AS Double) FROM ConversionTestTable" ),
                        expectedResult
                );

                // Cast Double to Decimal
                expectedResult = ImmutableList.of(
                        new Object[]{ BigDecimal.valueOf( 0.434 ) }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT  CAST(DoubleData AS DECIMAL(4,3)) FROM ConversionTestTable" ),
                        expectedResult
                );

                // Cast Date to Varchar
                expectedResult = ImmutableList.of(
                        new Object[]{ "2021-07-17" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT  CAST(DateData AS VARCHAR(20) ) FROM ConversionTestTable" ),
                        expectedResult
                );

            }
        }
    }

}
