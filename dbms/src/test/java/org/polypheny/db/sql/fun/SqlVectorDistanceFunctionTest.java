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
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class SqlVectorDistanceFunctionTest {

        @BeforeAll
        public static void start() throws SQLException {
            TestHelper.getInstance();
            addTestData();
        }

        private static void addTestData() throws SQLException {
            try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
                Connection connection = jdbcConnection.getConnection();
                try ( Statement statement = connection.createStatement() ) {
                    // REAL ARRAY(1,2) -> VectorType in Polypheny -> PolyFloatList at runtime
                    statement.executeUpdate( "CREATE TABLE knnrealtest( id INTEGER NOT NULL, feature REAL ARRAY(1,2), PRIMARY KEY (id) )" );
                    statement.executeUpdate( "INSERT INTO knnrealtest VALUES (1, ARRAY[1.0, 0.0])" );
                    statement.executeUpdate( "INSERT INTO knnrealtest VALUES (2, ARRAY[2.0, 2.0])" );
                    statement.executeUpdate( "INSERT INTO knnrealtest VALUES (3, ARRAY[0.0, 3.0])" );
                    connection.commit();
                }
            }
        }


        @AfterAll
        public static void stop() throws SQLException {
            try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
                Connection connection = jdbcConnection.getConnection();
                try ( Statement statement = connection.createStatement() ) {
                    statement.executeUpdate( "DROP TABLE knnrealtest" );
                }
            }
        }


    /**
     * Verify that REAL values survive INSERT -> SELECT without precision loss
     * or type corruption (e.g. FLOAT_LIST JSON vs LIST JSON deserialization).
     */
    @Test
    public void roundTripTest() throws SQLException {
        try ( JdbcConnection conn = new JdbcConnection( true ) ) {
            Connection connection = conn.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // exact cast - serializes/deserializes REAL ARRAY
                TestHelper.checkResultSet( statement.executeQuery(
                        "SELECT id, feature FROM knnrealtest ORDER BY id" ),
                            ImmutableList.of(
                                    new Object[]{ 1, new Float[]{ 1.0f, 0.0f } },
                                    new Object[]{ 2, new Float[]{ 2.0f, 2.0f } },
                                    new Object[]{ 3, new Float[]{ 0.0f, 3.0f } }
                            )
                        );
            }
        }
    }


    // target = [1.0, 0.0]
    // L2: row1=0, row2=sqrt(1+4)=sqrt(5) \approx 2.2361, row3=sqrt(1+9)=sqrt(10) \approx 3.1623
    @Test
    public void realArrayL2Test() throws SQLException {
        try ( JdbcConnection conn = new JdbcConnection( true ) ) {
            Connection connection = conn.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet( statement.executeQuery(
                        "SELECT id, distance(feature, ARRAY[1.0, 0.0], 'L2') as dist FROM knnrealtest ORDER BY id" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 0.0 },
                                    new Object[]{ 2, Math.sqrt( 5.0 ) },
                                    new Object[]{ 3, Math.sqrt( 10.0 ) }
                            )
                        );
            }
        }
    }


    // L1: row1=0, row2=|2-1|+|2-0|=3, row3=|0-1|+|3-0|=4
    @Test
    public void realArrayL1Test() throws SQLException {
        try ( JdbcConnection conn = new JdbcConnection( true ) ) {
            Connection connection = conn.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet( statement.executeQuery(
                        "SELECT id, distance(feature, ARRAY[1.0, 0.0], 'L1') as dist FROM knnrealtest ORDER BY id" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 0.0 },
                                    new Object[]{ 2, 3.0 },
                                    new Object[]{ 3, 4.0 }
                            )
                        );
            }
        }
    }


    // COSINE([a,b], [1,0]) = 1 - a||[a,b]||  (since ||[1,0]||=1)
    // row1=[1,0]: 1 - 1 = 0.0
    // row2=[2,2]: 1 - 2/sqrt(8) = 1 - 1/sqrt(2) \approx 0.2929
    // row3=[0,3]: 1 - 0 = 1.0
    @Test
    public void realArrayCosineTest() throws SQLException {
        try ( JdbcConnection conn = new JdbcConnection( true ) ) {
            Connection connection = conn.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet( statement.executeQuery(
                        "SELECT id, distance(feature, ARRAY[1.0, 0.0], 'COSINE') as dist FROM knnrealtest ORDER BY id" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 0.0 },
                                    new Object[]{ 2, 1.0 - (2.0 / Math.sqrt( 8.0 )) },
                                    new Object[]{ 3, 1.0 }
                            )
                        );
            }
        }
    }


    @Test
    public void realArrayTopKTest() throws SQLException {
        try ( JdbcConnection conn = new JdbcConnection( true ) ) {
            Connection connection = conn.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // top-2 nearest to [1,0] by L2 -> ids 1, 2
                TestHelper.checkResultSet( statement.executeQuery(
                        "SELECT id FROM knnrealtest ORDER BY distance(feature, ARRAY[1.0, 0.0], 'L2') ASC LIMIT 2" ),
                            ImmutableList.of(
                                    new Object[]{ 1 },
                                    new Object[]{ 2 }
                            )
                  );
            }
        }
    }


    /**
     * Both sides of DISTANCE are VectorType columns -> PolyFloatList at runtime
     * -> triggers float[] fast path in DistanceFunctions.
     */
    @Test
    public void realArrayCrossJoinL2Test() throws SQLException {
        try ( JdbcConnection conn = new JdbcConnection( true ) ) {
            Connection connection = conn.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // self-distance of row 1 ([1,0]) to itself must be 0
                TestHelper.checkResultSet( statement.executeQuery(
                        "SELECT t1.id, t2.id, distance(t1.feature, t2.feature, 'L2') as dist"
                                + " FROM knnrealtest t1, knnrealtest t2"
                                + " WHERE t1.id = 1 AND t2.id = 1" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, 0.0 }
                        )
                );
            }
        }
    }


    @Test
    public void realArrayPreparedStatementTest() throws SQLException {
        try ( JdbcConnection conn = new JdbcConnection( true ) ) {
            Connection connection = conn.getConnection();
            PreparedStatement ps = connection.prepareStatement(
                    "SELECT id, distance(feature, cast(? as REAL ARRAY), cast(? as VARCHAR)) as dist"
                    + " FROM knnrealtest ORDER BY id" );
            ps.setArray( 1, connection.createArrayOf( "REAL", new Object[]{ 1.0f, 0.0f
            } ) );
            ps.setString( 2, "L2" );

            TestHelper.checkResultSet( ps.executeQuery(),
                    ImmutableList.of(
                            new Object[]{ 1, 0.0 },
                            new Object[]{ 2, Math.sqrt( 5.0 ) },
                            new Object[]{ 3, Math.sqrt( 10.0 ) }
                    )
            );
        }
    }
}
