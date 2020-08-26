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
public class ForeignKeyConstraintTest {


    private static final String CREATE_TABLE_CONSTRAINT_TEST =
            "CREATE TABLE IF NOT EXISTS constraint_test (" +
                    "ctid INTEGER NOT NULL, " +
                    "a INTEGER NOT NULL, " +
                    "b INTEGER NOT NULL, " +
                    "c INTEGER NOT NULL, " +
                    "PRIMARY KEY (ctid), " +
                    "CONSTRAINT u_bc UNIQUE (a, b)" +
                    ")";
    private static final String CREATE_TABLE_CONSTRAINT_TEST2 =
            "CREATE TABLE IF NOT EXISTS constraint_test2 (" +
                    "ct2id INTEGER NOT NULL, " +
                    "ctid INTEGER NOT NULL, " +
                    "PRIMARY KEY (ct2id) " +
                    ")";


    private static final String ALTER_TABLE_ADD_FK =
            "ALTER TABLE constraint_test2 ADD CONSTRAINT fk FOREIGN KEY (ctid) REFERENCES constraint_test(ctid) ON UPDATE RESTRICT ON DELETE RESTRICT";

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
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2)" );
                statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (3, 1), (4, 2)" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(ctid) FROM constraint_test" ),
                        ImmutableList.of( new Object[]{ 2 } )
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(ct2id) FROM constraint_test2" ),
                        ImmutableList.of( new Object[]{ 2 } )
                );
                statement.executeUpdate( "DROP TABLE constraint_test2" );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }

        }
    }

    @Test
    public void testInsertConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2)" );
                try {
                    statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (3, 1), (4, 3)" );
                    Assert.fail( "Expected ConstraintViolationException was not thrown" );
                } catch ( Exception ignored ) {}
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(ctid) FROM constraint_test" ),
                        ImmutableList.of( new Object[]{ 2 } )
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(ct2id) FROM constraint_test2" ),
                        ImmutableList.of( new Object[]{ 0 } )
                );
                statement.executeUpdate( "DROP TABLE constraint_test2" );
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
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2)" );
                statement.executeUpdate( "INSERT INTO constraint_test2 SELECT ctid + 10 AS ct2id, ctid FROM constraint_test" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, 1, 1 },
                                new Object[]{ 2, 2, 2, 2 }
                        )
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test2" ),
                        ImmutableList.of(
                                new Object[]{ 11, 1 },
                                new Object[]{ 12, 2 }
                        )
                );
                statement.executeUpdate( "DROP TABLE constraint_test2" );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }

        }
    }

    @Test
    public void testInsertSelectConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2)" );
                try {
                    statement.executeUpdate( "INSERT INTO constraint_test2 SELECT ctid + 10 AS ct2id, ctid * 2 AS ctid FROM constraint_test" );
                    Assert.fail( "Expected ConstraintViolationException was not thrown" );
                } catch ( Exception ignored ) {}
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, 1, 1 },
                                new Object[]{ 2, 2, 2, 2 }
                        )
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(ct2id) FROM constraint_test2" ),
                        ImmutableList.of( new Object[]{ 0 } )
                );
                statement.executeUpdate( "DROP TABLE constraint_test2" );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }

        }
    }

    @Test
    public void testUpdateOutNoConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)" );
                statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (1, 1), (3, 3)" );
                statement.executeUpdate( "UPDATE constraint_test2 SET ctid = 2 WHERE ct2id = 3" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, 1, 1 },
                                new Object[]{ 2, 2, 2, 2 },
                                new Object[]{ 3, 3, 3, 3 }
                        )
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test2" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1 },
                                new Object[]{ 3, 2 }
                        )
                );
                statement.executeUpdate( "DROP TABLE constraint_test2" );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }

        }
    }

    @Test
    public void testUpdateOutConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)" );
                statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (1, 1), (3, 3)" );
                try {
                    statement.executeUpdate( "UPDATE constraint_test2 SET ctid = ctid + 2" );
                    Assert.fail( "Expected ConstraintViolationException was not thrown" );
                } catch ( Exception ignored ) {}
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, 1, 1 },
                                new Object[]{ 2, 2, 2, 2 },
                                new Object[]{ 3, 3, 3, 3 }
                        )
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test2" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1 },
                                new Object[]{ 3, 3 }
                        )
                );
                statement.executeUpdate( "DROP TABLE constraint_test2" );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }

        }
    }

    @Test
    public void testUpdateInNoConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)" );
                statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (1, 1), (3, 3)" );
                statement.executeUpdate( "UPDATE constraint_test SET ctid = 4 WHERE ctid = 2" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, 1, 1 },
                                new Object[]{ 4, 2, 2, 2 },
                                new Object[]{ 3, 3, 3, 3 }
                        )
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test2" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1 },
                                new Object[]{ 3, 3 }
                        )
                );
                statement.executeUpdate( "DROP TABLE constraint_test2" );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }
        }
    }

    @Test
    public void testUpdateInConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)" );
                statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (1, 1), (3, 3)" );
                try {
                    statement.executeUpdate( "UPDATE constraint_test SET ctid = 4 WHERE ctid = 1" );
                    Assert.fail( "Expected ConstraintViolationException was not thrown" );
                } catch ( Exception ignored ) {}
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, 1, 1 },
                                new Object[]{ 2, 2, 2, 2 },
                                new Object[]{ 3, 3, 3, 3 }
                        )
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test2" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1 },
                                new Object[]{ 3, 3 }
                        )
                );
                statement.executeUpdate( "DROP TABLE constraint_test2" );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }
        }
    }

    @Test
    public void testDeleteNoConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)" );
                statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (1, 1), (3, 3)" );
                statement.executeUpdate( "DELETE FROM constraint_test WHERE ctid = 2" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, 1, 1 },
                                new Object[]{ 3, 3, 3, 3 }
                        )
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test2" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1 },
                                new Object[]{ 3, 3 }
                        )
                );
                statement.executeUpdate( "DROP TABLE constraint_test2" );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }
        }
    }

    @Test
    public void testDeleteConflict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)" );
                statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (1, 1), (3, 3)" );
                try {
                    statement.executeUpdate( "DELETE FROM constraint_test WHERE ctid = 1" );
                    Assert.fail( "Expected ConstraintViolationException was not thrown" );
                } catch ( Exception ignored ) {}
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, 1, 1 },
                                new Object[]{ 2, 2, 2, 2 },
                                new Object[]{ 3, 3, 3, 3 }
                        )
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM constraint_test2" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1 },
                                new Object[]{ 3, 3 }
                        )
                );
                statement.executeUpdate( "DROP TABLE constraint_test2" );
                statement.executeUpdate( "DROP TABLE constraint_test" );
            }
        }
    }

}
