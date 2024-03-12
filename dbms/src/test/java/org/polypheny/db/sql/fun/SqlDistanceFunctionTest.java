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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.util.ArrayFactoryImpl;
import org.apache.calcite.avatica.util.Unsafe;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class SqlDistanceFunctionTest {


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
                statement.executeUpdate( "CREATE TABLE knninttest( id INTEGER NOT NULL, myarray INTEGER ARRAY(1,2), PRIMARY KEY (id) )" );
                statement.executeUpdate( "INSERT INTO knninttest VALUES (1, ARRAY[1,1])" );
                statement.executeUpdate( "INSERT INTO knninttest VALUES (2, ARRAY[2,2])" );
                statement.executeUpdate( "INSERT INTO knninttest VALUES (3, ARRAY[0,3])" );

                statement.executeUpdate( "CREATE TABLE knndoubletest( id INTEGER NOT NULL, myarray DOUBLE ARRAY(1,2), PRIMARY KEY (id) )" );
                statement.executeUpdate( "INSERT INTO knndoubletest VALUES (1, ARRAY[1.0,1.0])" );
                statement.executeUpdate( "INSERT INTO knndoubletest VALUES (2, ARRAY[2.0,2.0])" );
                statement.executeUpdate( "INSERT INTO knndoubletest VALUES (3, ARRAY[0.0,3.0])" );

                statement.executeUpdate( "CREATE TABLE knnbigtest( id INTEGER NOT NULL, myarray BIGINT ARRAY(1,2), PRIMARY KEY (id) )" );
                statement.executeUpdate( "INSERT INTO knnbigtest VALUES (1, ARRAY[1,1])" );
                statement.executeUpdate( "INSERT INTO knnbigtest VALUES (2, ARRAY[2,2])" );
                statement.executeUpdate( "INSERT INTO knnbigtest VALUES (3, ARRAY[0,3])" );

                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE knninttest" );
                statement.executeUpdate( "DROP TABLE knndoubletest" );
                statement.executeUpdate( "DROP TABLE knnbigtest" );
            }
        }
    }

    // --------------- Tests ---------------


    @Test
    public void l2SquaredTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 0.0 },
                        new Object[]{ 2, 2.0 },
                        new Object[]{ 3, 5.0 }
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'L2SQUARED') as dist FROM knninttest ORDER BY id" ),
                        expectedResult
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'L2SQUARED') as dist FROM knndoubletest ORDER BY id" ),
                        expectedResult
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'L2SQUARED') as dist FROM knnbigtest ORDER BY id" ),
                        expectedResult
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'L2SQUARED') as dist FROM knninttest ORDER BY dist LIMIT 5" ),
                        expectedResult
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'L2SQUARED') as dist FROM knndoubletest ORDER BY dist LIMIT 5" ),
                        expectedResult
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'L2SQUARED') as dist FROM knnbigtest ORDER BY dist LIMIT 5" ),
                        expectedResult
                );
            }
        }
    }


    @Test
    public void l2Test() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 0.0 },
                        new Object[]{ 2, 1.4142135623730951 },
                        new Object[]{ 3, 2.23606797749979 }
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'L2') as dist FROM knninttest ORDER BY id" ),
                        expectedResult
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'L2') as dist FROM knndoubletest ORDER BY id" ),
                        expectedResult
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'L2') as dist FROM knnbigtest ORDER BY id" ),
                        expectedResult
                );
            }
        }
    }


    @Test
    public void l1Test() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {
                ImmutableList<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 0.0 },
                        new Object[]{ 2, 2.0 },
                        new Object[]{ 3, 3.0 }
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'L1') as dist FROM knninttest ORDER BY id" ),
                        expectedResult
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'L1') as dist FROM knndoubletest ORDER BY id" ),
                        expectedResult
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'L1') as dist FROM knndoubletest ORDER BY id" ),
                        expectedResult
                );
            }
        }
    }


    @Test
    public void chiSquaredTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {
                ImmutableList<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 0.0 },
                        new Object[]{ 2, 0.6666666666666666 },
                        new Object[]{ 3, 2.0 }
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'CHISQUARED') as dist FROM knninttest ORDER BY id" ),
                        expectedResult
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'CHISQUARED') as dist FROM knndoubletest ORDER BY id" ),
                        expectedResult
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1,1], 'CHISQUARED') as dist FROM knndoubletest ORDER BY id" ),
                        expectedResult
                );
            }
        }
    }


    @Test
    public void knnFilterTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {
                ImmutableList<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 2L }
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(id) FROM knninttest WHERE distance(myarray, ARRAY[1,1], 'L2') < 2.0" ),
                        expectedResult
                );
            }
        }
    }


    @Test
    public void preparedStatementTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                PreparedStatement preparedStatement = connection.prepareStatement( "SELECT id, distance(myarray, cast(? as INTEGER ARRAY), cast( ? as VARCHAR)) as dist FROM knninttest ORDER BY id" );

                final ArrayFactoryImpl arrayFactory = new ArrayFactoryImpl( Unsafe.localCalendar().getTimeZone() );
                preparedStatement.setArray( 1, arrayFactory.createArray(
                        ColumnMetaData.scalar( Types.INTEGER, "INTEGER", Rep.STRING ),
                        ImmutableList.of( 1, 1 ) ) );
                preparedStatement.setString( 2, "L2SQUARED" );

                TestHelper.checkResultSet(
                        preparedStatement.executeQuery(),
                        ImmutableList.of(
                                new Object[]{ 1, 0.0 },
                                new Object[]{ 2, 2.0 },
                                new Object[]{ 3, 5.0 }
                        )
                );
            }
        }
    }

}
