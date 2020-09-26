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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@RunWith(Parameterized.class)
public class UniqueConstraintTest {

    @Parameters(name = "Create Indexes: {0}")
    public static Object[] data() {
        return new Object[]{ false, true };
    }


    private static final String CREATE_TABLE_CONSTRAINT_STATEMENTS =
            "CREATE TABLE IF NOT EXISTS constraint_test (" +
                    "ctid INTEGER NOT NULL, " +
                    "a INTEGER NOT NULL, " +
                    "b INTEGER NOT NULL, " +
                    "c INTEGER NOT NULL, " +
                    "PRIMARY KEY (ctid), " +
                    "CONSTRAINT u_ab UNIQUE (a, b)" +
                    ")";

    private static final String[] ALTER_TABLE_ADD_INDEX_STATEMENTS = {
            "ALTER TABLE constraint_test ADD UNIQUE INDEX idx_ctid ON ctid",
            "ALTER TABLE constraint_test ADD UNIQUE INDEX idx_ab ON (a, b)",
            "ALTER TABLE constraint_test ADD INDEX idx_a ON (a)",
            "ALTER TABLE constraint_test ADD INDEX idx_b ON (b)"
    };


    private final boolean createIndexes;


    public UniqueConstraintTest( boolean createIndexes ) {
        this.createIndexes = createIndexes;
    }


    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void insertNoConflictTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_STATEMENTS );
                if ( createIndexes ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1)" );
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (2, 2, 2, 2)" );
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (3, 3, 3, 2)" );
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (4, 4, 2, 2), (5, 4, 3, 2)" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT COUNT(ctid) FROM constraint_test" ),
                            ImmutableList.of( new Object[]{ 5L } )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @Test
    public void insertExternalConflictTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_STATEMENTS );
                if ( createIndexes ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1)" );
                    try {
                        statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 2, 3, 4)" );
                        Assert.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( Exception ignored ) {
                    }
                    try {
                        statement.executeUpdate( "INSERT INTO constraint_test VALUES (2, 1, 1, 4)" );
                        Assert.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( Exception ignored ) {
                    }
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test" ),
                            ImmutableList.of( new Object[]{ 1, 1, 1, 1 } )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @Test
    public void insertInternalConflictTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_STATEMENTS );
                if ( createIndexes ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1)" );
                    try {
                        statement.executeUpdate( "INSERT INTO constraint_test VALUES (2, 2, 3, 4), (4, 2, 3, 1)" );
                        Assert.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( Exception ignored ) {
                    }
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test" ),
                            ImmutableList.of( new Object[]{ 1, 1, 1, 1 } )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @Test
    public void insertSelectNoConflictTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_STATEMENTS );
                if ( createIndexes ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2)" );
                    statement.executeUpdate( "INSERT INTO constraint_test SELECT ctid + 2 AS ctid, a, b + 2 AS b, c FROM constraint_test" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 2, 2, 2, 2 },
                                    new Object[]{ 3, 1, 3, 1 },
                                    new Object[]{ 4, 2, 4, 2 }
                            )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @Test
    public void insertSelectExternalConflictTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_STATEMENTS );
                if ( createIndexes ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2)" );
                    try {
                        statement.executeUpdate( "INSERT INTO constraint_test SELECT * FROM constraint_test" );
                        Assert.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( Exception ignored ) {
                    }
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 2, 2, 2, 2 }
                            )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @Test
    public void insertSelectInternalConflictTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_STATEMENTS );
                if ( createIndexes ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 5), (2, 2, 2, 5)" );
                    try {
                        statement.executeUpdate( "INSERT INTO constraint_test SELECT c AS ctid, a + 2 AS a, b, c FROM constraint_test" );
                        Assert.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( Exception ignored ) {
                    }
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 5 },
                                    new Object[]{ 2, 2, 2, 5 }
                            )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @Test
    public void batchInsertTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_STATEMENTS );
                if ( createIndexes ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    PreparedStatement preparedStatement = connection.prepareStatement( "INSERT INTO constraint_test VALUES (?,?,?,?)" );

                    // This should not work
                    preparedStatement.setInt( 1, 1 );
                    preparedStatement.setInt( 2, 1 );
                    preparedStatement.setInt( 3, 1 );
                    preparedStatement.setInt( 4, 1 );
                    preparedStatement.addBatch();
                    preparedStatement.setInt( 1, 1 );
                    preparedStatement.setInt( 2, 1 );
                    preparedStatement.setInt( 3, 1 );
                    preparedStatement.setInt( 4, 1 );
                    preparedStatement.addBatch();
                    try {
                        preparedStatement.executeBatch();
                        Assert.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( Exception ignored ) {
                    }

                    // This should work
                    for ( int i = 1; i < 5; i++ ) {
                        preparedStatement.setInt( 1, i );
                        preparedStatement.setInt( 2, i );
                        preparedStatement.setInt( 3, i );
                        preparedStatement.setInt( 4, i );
                        preparedStatement.addBatch();
                    }
                    preparedStatement.executeBatch();

                    // This should not work
                    for ( int i = 8; i > 3; i-- ) {
                        preparedStatement.setInt( 1, i );
                        preparedStatement.setInt( 2, i );
                        preparedStatement.setInt( 3, i );
                        preparedStatement.setInt( 4, i );
                        preparedStatement.addBatch();
                    }
                    try {
                        preparedStatement.executeBatch();
                        Assert.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( Exception ignored ) {
                    }
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 2, 2, 2, 2 },
                                    new Object[]{ 3, 3, 3, 3 },
                                    new Object[]{ 4, 4, 4, 4 }
                            )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @Test
    public void updateNoConflictTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_STATEMENTS );
                if ( createIndexes ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3), (4, 4, 4, 4)" );
                    statement.executeUpdate( "UPDATE constraint_test SET a = ctid" );
                    statement.executeUpdate( "UPDATE constraint_test SET a = 2 * ctid, b = 2 * ctid" );
                    statement.executeUpdate( "UPDATE constraint_test SET c = 1" );
                    statement.executeUpdate( "UPDATE constraint_test SET c = 2 WHERE ctid = 3" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 2, 2, 1 },
                                    new Object[]{ 2, 4, 4, 1 },
                                    new Object[]{ 3, 6, 6, 2 },
                                    new Object[]{ 4, 8, 8, 1 }
                            )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @Test
    public void updateConflictTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_STATEMENTS );
                if ( createIndexes ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3), (4, 4, 4, 4)" );
                    try {
                        statement.executeUpdate( "UPDATE constraint_test SET ctid = 1" );
                        Assert.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( Exception ignored ) {
                    }
                    try {
                        statement.executeUpdate( "UPDATE constraint_test SET a = 42, b = 73" );
                        Assert.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( Exception ignored ) {
                    }
                    try {
                        statement.executeUpdate( "UPDATE constraint_test SET ctid = 4 WHERE a = 3" );
                        Assert.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( Exception ignored ) {
                    }
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 2, 2, 2, 2 },
                                    new Object[]{ 3, 3, 3, 3 },
                                    new Object[]{ 4, 4, 4, 4 }
                            )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }

}
