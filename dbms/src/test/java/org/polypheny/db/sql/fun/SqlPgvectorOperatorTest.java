/*
 * Copyright 2019-2026 The Polypheny Project
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
import java.sql.SQLException;
import java.sql.Statement;
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
public class SqlPgvectorOperatorTest {

    @BeforeAll
    public static void start() throws SQLException {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestData();
    }


    private static void addTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE pgvecrealtest( id INTEGER NOT NULL, myarray REAL ARRAY(1,2), PRIMARY KEY (id) )" );
                statement.executeUpdate( "INSERT INTO pgvecrealtest VALUES (1, ARRAY[1.0, 1.0])" );
                statement.executeUpdate( "INSERT INTO pgvecrealtest VALUES (2, ARRAY[2.0, 2.0])" );
                statement.executeUpdate( "INSERT INTO pgvecrealtest VALUES (3, ARRAY[0.0, 3.0])" );

                statement.executeUpdate( "CREATE TABLE pgvecbooltest( id INTEGER NOT NULL, myarray BOOLEAN ARRAY(1,3), PRIMARY KEY (id) )" );
                statement.executeUpdate( "INSERT INTO pgvecbooltest VALUES (1, ARRAY[true, true, true])" );
                statement.executeUpdate( "INSERT INTO pgvecbooltest VALUES (2, ARRAY[true, false, true])" );
                statement.executeUpdate( "INSERT INTO pgvecbooltest VALUES (3, ARRAY[false, false, false])" );
                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE pgvecrealtest" );
                statement.executeUpdate( "DROP TABLE pgvecbooltest" );

            }
        }
    }


    // --------------- L2 operator (<->) ---------------
    @Test
    public void l2OperatorTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expected = ImmutableList.of(
                        new Object[]{ 1, 0.0 },
                        new Object[]{ 2, 1.4142135623730951 },
                        new Object[]{ 3, 2.23606797749979 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, myarray <-> ARRAY[1.0, 1.0] AS dist FROM pgvecrealtest ORDER BY id" ),
                        expected
                );
            }
        }
    }


    @Test
    public void l2OperatorReversedTest() throws SQLException {
        // Operator is symmetric: literal on the left must give the same result.
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expected = ImmutableList.of(
                        new Object[]{ 1, 0.0 },
                        new Object[]{ 2, 1.4142135623730951 },
                        new Object[]{ 3, 2.23606797749979 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, ARRAY[1.0, 1.0] <-> myarray AS dist FROM pgvecrealtest ORDER BY id" ),
                        expected
                );
            }
        }
    }


    @Test
    public void l2EquivalenceTest() throws SQLException {
        // <-> must produce identical results to distance(..., 'L2').
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expected = ImmutableList.of(
                        new Object[]{ 1, 0.0 },
                        new Object[]{ 2, 1.4142135623730951 },
                        new Object[]{ 3, 2.23606797749979 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1.0, 1.0], 'L2') AS dist FROM pgvecrealtest ORDER BY id" ),
                        expected
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, myarray <-> ARRAY[1.0, 1.0] AS dist FROM pgvecrealtest ORDER BY id" ),
                        expected
                );
            }
        }
    }


    @Test
    public void knnTopKL2Test() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expected = ImmutableList.of(
                        new Object[]{ 1, 0.0 },
                        new Object[]{ 2, 1.4142135623730951 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, myarray <-> ARRAY[1.0, 1.0] AS dist FROM pgvecrealtest ORDER BY dist LIMIT 2" ),
                        expected
                );
            }
        }
    }


    @Test
    public void filterL2Test() throws SQLException {
        // Rows 1 (dist 0.0) and 2 (dist 1.414) are within L2 distance 2.0 of [1,1].
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(id) FROM pgvecrealtest WHERE myarray <-> ARRAY[1.0, 1.0] < 2.0" ),
                        ImmutableList.of( new Object[]{ 2L } )
                );
            }
        }
    }


    @Test
    public void crossJoinKnnTest() throws SQLException {
        // Find the 2 rows in the table nearest to the vector of row id=1 via cross join.
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expected = ImmutableList.of(
                        new Object[]{ 1, 0.0 },
                        new Object[]{ 2, 1.4142135623730951 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery(
                                "SELECT a.id, a.myarray <-> b.myarray AS dist "
                                        + "FROM pgvecrealtest a, (SELECT myarray FROM pgvecrealtest WHERE id = 1) b "
                                        + "ORDER BY dist LIMIT 2" ),
                        expected
                );
            }
        }
    }


    // --------------- L1 operator (<+>) ---------------
    @Test
    public void l1OperatorTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expected = ImmutableList.of(
                        new Object[]{ 1, 0.0 },
                        new Object[]{ 2, 2.0 },
                        new Object[]{ 3, 3.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, myarray <+> ARRAY[1.0, 1.0] AS dist FROM pgvecrealtest ORDER BY id" ),
                        expected
                );
            }
        }
    }


    @Test
    public void knnTopKL1Test() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expected = ImmutableList.of(
                        new Object[]{ 1, 0.0 },
                        new Object[]{ 2, 2.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, myarray <+> ARRAY[1.0, 1.0] AS dist FROM pgvecrealtest ORDER BY dist LIMIT 2" ),
                        expected
                );
            }
        }
    }


    @Test
    public void filterL1Test() throws SQLException {
        // Rows 1 (dist 0.0) and 2 (dist 2.0) are within L1 distance 2.5 of [1,1].
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(id) FROM pgvecrealtest WHERE myarray <+> ARRAY[1.0, 1.0] < 2.5" ),
                        ImmutableList.of( new Object[]{ 2L } )
                );
            }
        }
    }


    @Test
    public void l1EquivalenceTest() throws SQLException {
        // <+> must produce identical results to distance(..., 'L1').
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expected = ImmutableList.of(
                        new Object[]{ 1, 0.0 },
                        new Object[]{ 2, 2.0 },
                        new Object[]{ 3, 3.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, distance(myarray, ARRAY[1.0, 1.0], 'L1') AS dist FROM pgvecrealtest ORDER BY id" ),
                        expected
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, myarray <+> ARRAY[1.0, 1.0] AS dist FROM pgvecrealtest ORDER BY id" ),
                        expected
                );
            }
        }
    }


    // --------------- Cosine operator (<=>) ---------------
    @Test
    public void cosOperatorTest() throws SQLException {
        // cosDistance([2,2],[1,1]) = 0 (same direction); cosDistance([0,3],[1,1]) = 1 - 1/sqrt(2)
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expected = ImmutableList.of(
                        new Object[]{ 1, 0.0 },
                        new Object[]{ 2, 0.0 },
                        new Object[]{ 3, 1.0 - 1.0 / Math.sqrt( 2 ) }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, myarray <=> ARRAY[1.0, 1.0] AS dist FROM pgvecrealtest ORDER BY id" ),
                        expected
                );
            }
        }
    }


    // --------------- Hamming operator (<~>) ---------------
    @Test
    public void hammingOperatorTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expected = ImmutableList.of(
                        new Object[]{ 1, 1.0 },
                        new Object[]{ 2, 2.0 },
                        new Object[]{ 3, 2.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, myarray <~> ARRAY[true, true, false] AS dist FROM pgvecbooltest ORDER BY id" ),
                        expected
                );
            }
        }
    }

    @Test
    public void hammingEquivalenceTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expected = ImmutableList.of(
                        new Object[]{ 1, 1.0 },
                        new Object[]{ 2, 2.0 },
                        new Object[]{ 3, 2.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, hamming_distance(myarray, ARRAY[true, true, false]) AS dist FROM pgvecbooltest ORDER BY id" ),
                        expected
                );
            }
        }
    }

    // --------------- Jaccard operator (<%>) ---------------
    @Test
    public void jaccardOperatorTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expected = ImmutableList.of(
                        new Object[]{ 1, 1.0 - (2.0 / 3.0) },
                        new Object[]{ 2, 1.0 - (1.0 / 3.0) },
                        new Object[]{ 3, 1.0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id, myarray <%> ARRAY[true, true, false] AS dist FROM pgvecbooltest ORDER BY id" ),
                        expected
                );
            }
        }
    }

}
