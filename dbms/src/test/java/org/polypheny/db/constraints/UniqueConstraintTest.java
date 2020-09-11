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

package org.polypheny.db.constraints;


import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@Slf4j
public class UniqueConstraintTest {


    private static final String CREATE_TABLE_CONSTRAINT_TEST =
            "CREATE TABLE IF NOT EXISTS constraint_test (" +
            "ctid INTEGER NOT NULL, " +
            "a INTEGER NOT NULL, " +
            "b INTEGER NOT NULL, " +
            "c INTEGER NOT NULL, " +
            "PRIMARY KEY (ctid), " +
            "CONSTRAINT u_bc UNIQUE (a, b)" +
            ")";


    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }

    @Test
    public void testInsertNoConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1)" );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (2, 2, 2, 2)" );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (3, 3, 3, 2)" );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (4, 4, 2, 2), (5, 4, 3, 2)" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(ctid) FROM constraint_test" ),
                        ImmutableList.of( new Object[]{ 5L } )
                );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }

        }
    }

    @Test
    public void testInsertExternalConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1)" );
                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 2, 3, 4)" );
                    Assert.fail( "Expected ConstraintViolationException was not thrown" );
                } catch ( Exception ignored ) {}
                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (2, 1, 1, 4)" );
                    Assert.fail( "Expected ConstraintViolationException was not thrown" );
                } catch ( Exception ignored ) {}
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of( new Object[]{ 1, 1, 1, 1 } )
                );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }

        }
    }

    @Test
    public void testInsertInternalConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1)" );
                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (2, 2, 3, 4), (4, 2, 3, 1)" );
                    Assert.fail( "Expected ConstraintViolationException was not thrown" );
                } catch ( Exception ignored ) {}
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of( new Object[]{ 1, 1, 1, 1 } )
                );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }

        }
    }

    @Test
    public void testInsertSelectNoConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2)" );
                statement.executeUpdate( "INSERT INTO constraint_test SELECT ctid + 2 AS ctid, a, b + 2 AS b, c FROM constraint_test" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, 1, 1 },
                                new Object[]{ 2, 2, 2, 2 },
                                new Object[]{ 3, 1, 3, 1 },
                                new Object[]{ 4, 2, 4, 2 }
                        )
                );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }

        }
    }

    @Test
    public void testInsertSelectExternalConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2)" );
                try {
                    statement.executeUpdate( "INSERT INTO constraint_test SELECT * FROM constraint_test" );
                    Assert.fail( "Expected ConstraintViolationException was not thrown" );
                } catch ( Exception ignored ) {}
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, 1, 1 },
                                new Object[]{ 2, 2, 2, 2 }
                        )
                );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }

        }
    }

    @Test
    public void testInsertSelectInternalConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 5), (2, 2, 2, 5)" );
                try {
                    statement.executeUpdate( "INSERT INTO constraint_test SELECT d AS ctid, a + 2 AS a, b, d FROM constraint_test" );
                    Assert.fail( "Expected ConstraintViolationException was not thrown" );
                } catch ( Exception ignored ) {}
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, 1, 1 },
                                new Object[]{ 2, 2, 2, 2 }
                        )
                );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }

        }
    }

    @Test
    public void testUpdateNoConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3), (4, 4, 4, 4)" );
                statement.executeUpdate( "UPDATE constraint_test SET a = ctid" );
                statement.executeUpdate( "UPDATE constraint_test SET a = 2 * ctid, b = 2 * ctid" );
                statement.executeUpdate( "UPDATE constraint_test SET c = 1" );
                statement.executeUpdate( "UPDATE constraint_test SET c = 2 WHERE ctid = 3" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of(
                                new Object[]{ 1, 2, 2, 1 },
                                new Object[]{ 2, 4, 4, 1 },
                                new Object[]{ 3, 6, 6, 2 },
                                new Object[]{ 4, 8, 8, 1 }
                        )
                );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }

        }
    }

    @Test
    public void testUpdateConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3), (4, 4, 4, 4)" );
                try {
                    statement.executeUpdate( "UPDATE constraint_test SET ctid = 1" );
                    Assert.fail( "Expected ConstraintViolationException was not thrown" );
                } catch ( Exception ignored ) {}
                try {
                    statement.executeUpdate( "UPDATE constraint_test SET a = 42, b = 73" );
                    Assert.fail( "Expected ConstraintViolationException was not thrown" );
                } catch ( Exception ignored ) {}
                try {
                    statement.executeUpdate( "UPDATE constraint_test SET ctid = 4 WHERE a = 3" );
                    Assert.fail( "Expected ConstraintViolationException was not thrown" );
                } catch ( Exception ignored ) {}
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, 1, 1 },
                                new Object[]{ 2, 2, 2, 2 },
                                new Object[]{ 3, 3, 3, 3 },
                                new Object[]{ 4, 4, 4, 4 }
                        )
                );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }

        }
    }

}
