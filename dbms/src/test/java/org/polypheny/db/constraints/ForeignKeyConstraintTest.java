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

package org.polypheny.db.constraints;


import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaClientRuntimeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class ForeignKeyConstraintTest {


    private static final String CREATE_TABLE_CONSTRAINT_TEST = """
            CREATE TABLE IF NOT EXISTS constraint_test (
                ctid INTEGER NOT NULL,\s
                a INTEGER NOT NULL,\s
                b INTEGER NOT NULL,\s
                c INTEGER NOT NULL,\s
                PRIMARY KEY (ctid),\s
                CONSTRAINT u_ab UNIQUE (a, b)
            )""";

    private static final String CREATE_TABLE_CONSTRAINT_TEST2 =
            "CREATE TABLE IF NOT EXISTS constraint_test2 (" +
                    "ct2id INTEGER NOT NULL, " +
                    "ctid INTEGER NOT NULL, " +
                    "PRIMARY KEY (ct2id) " +
                    ")";

    private static final String ALTER_TABLE_ADD_FK =
            "ALTER TABLE constraint_test2 "
                    + "ADD CONSTRAINT fk FOREIGN KEY (ctid) "
                    + "REFERENCES constraint_test(ctid) ON UPDATE RESTRICT ON DELETE RESTRICT";

    private static final String[] ALTER_TABLE_ADD_INDEX_STATEMENTS = {
            "ALTER TABLE constraint_test ADD UNIQUE INDEX idx_ctid ON ctid",
            "ALTER TABLE constraint_test ADD UNIQUE INDEX idx_ab ON (a, b)",
            "ALTER TABLE constraint_test ADD INDEX idx_a ON (a)",
            "ALTER TABLE constraint_test ADD INDEX idx_b ON (b)",
            "ALTER TABLE constraint_test ADD INDEX idx_c ON (c)",
            "ALTER TABLE constraint_test2 ADD UNIQUE INDEX idx2_ct2id ON ctid",
            "ALTER TABLE constraint_test2 ADD INDEX idx2_ctid ON ctid",
    };
    private static TestHelper helper;


    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        helper = TestHelper.getInstance();
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER CONFIG 'runtime/uniqueConstraintEnforcement' SET true" );
                statement.executeUpdate( "ALTER CONFIG 'runtime/foreignKeyEnforcement' SET true" );
                statement.executeUpdate( "ALTER CONFIG 'runtime/polystoreIndexesSimplify' SET true" );
            }
        }
    }


    @AfterAll
    public static void shutdown() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER CONFIG 'runtime/uniqueConstraintEnforcement' SET false" );
                statement.executeUpdate( "ALTER CONFIG 'runtime/foreignKeyEnforcement' SET false" );
                statement.executeUpdate( "ALTER CONFIG 'runtime/polystoreIndexesSimplify' SET false" );
            }
        }
    }


    @ParameterizedTest(name = "{index}. Create Index: {0}")
    @ValueSource(booleans = { false, true })
    public void testInsertNoConflict( boolean useIndex ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                if ( useIndex && helper.storeSupportsIndex() ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2)" );
                    statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (3, 1), (4, 2)" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT COUNT(ctid) FROM constraint_test" ),
                            ImmutableList.of( new Object[]{ 2L } )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT COUNT(ct2id) FROM constraint_test2" ),
                            ImmutableList.of( new Object[]{ 2L } )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test2" );
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @ParameterizedTest(name = "{index}. Create Index: {0}")
    @ValueSource(booleans = { false, true })
    public void testInsertConflict( boolean useIndex ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                if ( useIndex && helper.storeSupportsIndex() ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2)" );
                    connection.commit();
                    try {
                        statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (3, 1), (4, 3)" );
                        connection.commit();
                        Assertions.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( AvaticaClientRuntimeException e ) {
                        if ( !(e.getMessage().contains( "Remote driver error:" )
                                && e.getMessage().contains( "Transaction violates foreign key constraint" )) ) {
                            throw new RuntimeException( "Unexpected exception", e );
                        }
                    }
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT COUNT(ctid) FROM constraint_test" ),
                            ImmutableList.of( new Object[]{ 2L } )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT COUNT(ct2id) FROM constraint_test2" ),
                            ImmutableList.of( new Object[]{ 0L } )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test2" );
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @ParameterizedTest(name = "{index}. Create Index: {0}")
    @ValueSource(booleans = { false, true })
    //@Disabled // todo dl enable as soon as such inserts work correctly
    public void testInsertSelectNoConflict( boolean useIndex ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                if ( useIndex && helper.storeSupportsIndex() ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2)" );
                    statement.executeUpdate( "INSERT INTO constraint_test2 SELECT ctid + 10 AS ct2id, ctid FROM constraint_test" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 2, 2, 2, 2 }
                            )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test2 ORDER BY ct2id" ),
                            ImmutableList.of(
                                    new Object[]{ 11, 1 },
                                    new Object[]{ 12, 2 }
                            )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test2" );
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @ParameterizedTest(name = "{index}. Create Index: {0}")
    @ValueSource(booleans = { false, true })
    public void testInsertSelectConflict( boolean useIndex ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                if ( useIndex && helper.storeSupportsIndex() ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2)" );

                    try {
                        statement.executeUpdate( "INSERT INTO constraint_test2 SELECT ctid + 10 AS ct2id, ctid * 2 AS ctid FROM constraint_test" );
                        Assertions.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( Throwable e ) {
                        if ( !(e.getMessage().contains( "Remote driver error:" ) && e.getMessage().contains( "Transaction violates foreign key constraint" )) ) {
                            throw new RuntimeException( "Unexpected exception", e );
                        }
                        connection.rollback();
                    }
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 2, 2, 2, 2 }
                            )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT COUNT(ct2id) FROM constraint_test2" ),
                            ImmutableList.of( new Object[]{ 0L } )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test2" );
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @ParameterizedTest(name = "{index}. Create Index: {0}")
    @ValueSource(booleans = { false, true })
    public void testUpdateOutNoConflict( boolean useIndex ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                if ( useIndex && helper.storeSupportsIndex() ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)" );
                    statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (1, 1), (3, 3)" );
                    statement.executeUpdate( "UPDATE constraint_test2 SET ctid = 2 WHERE ct2id = 3" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 2, 2, 2, 2 },
                                    new Object[]{ 3, 3, 3, 3 }
                            )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test2 ORDER BY ct2id" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1 },
                                    new Object[]{ 3, 2 }
                            )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test2" );
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @ParameterizedTest(name = "{index}. Create Index: {0}")
    @ValueSource(booleans = { false, true })
    public void testUpdateOutConflict( boolean useIndex ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                if ( useIndex && helper.storeSupportsIndex() ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)" );
                    statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (3, 3), (1, 1)" );
                    connection.commit();

                    try {
                        statement.executeUpdate( "UPDATE constraint_test2 SET ctid = ctid + 2" );

                        TestHelper.checkResultSet(
                                statement.executeQuery( "SELECT * FROM constraint_test2 ORDER BY ct2id" ),
                                ImmutableList.of(
                                        new Object[]{ 1, 3 },
                                        new Object[]{ 3, 5 }
                                ),
                                false
                        );

                        connection.commit();
                        Assertions.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( RuntimeException e ) {
                        if ( !(e.getMessage().contains( "Remote driver error:" )
                                && e.getMessage().contains( "Transaction violates foreign key constraint" )) ) {
                            throw new RuntimeException( "Unexpected exception", e );
                        }
                    }
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 2, 2, 2, 2 },
                                    new Object[]{ 3, 3, 3, 3 }
                            )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test2 ORDER BY ct2id" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1 },
                                    new Object[]{ 3, 3 }
                            )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test2" );
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @ParameterizedTest(name = "{index}. Create Index: {0}")
    @ValueSource(booleans = { false, true })
    @Tag("cottontailExcluded") // only with indexes, cannot find column during update
    public void testUpdateInNoConflict( boolean useIndex ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                if ( useIndex && helper.storeSupportsIndex() ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)" );
                    statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (1, 1), (3, 3)" );
                    statement.executeUpdate( "UPDATE constraint_test SET ctid = 4 WHERE ctid = 2" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY c" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 4, 2, 2, 2 },
                                    new Object[]{ 3, 3, 3, 3 }
                            )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test2 ORDER BY ct2id" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1 },
                                    new Object[]{ 3, 3 }
                            )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test2" );
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @ParameterizedTest(name = "{index}. Create Index: {0}")
    @ValueSource(booleans = { false, true })
    @Tag("cottontailExcluded") // only with indexes, cannot find column during update
    public void testUpdateInConflict( boolean useIndex ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                if ( useIndex && helper.storeSupportsIndex() ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)" );
                    statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (1, 1), (3, 3)" );
                    connection.commit();
                    try {
                        statement.executeUpdate( "UPDATE constraint_test SET ctid = 4 WHERE ctid = 1" );
                        connection.commit();
                        Assertions.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( AvaticaClientRuntimeException e ) {
                        if ( !(e.getMessage().contains( "Remote driver error:" )
                                && e.getMessage().contains( "Transaction violates foreign key constraint" )) ) {
                            throw new RuntimeException( "Unexpected exception", e );
                        }
                    }
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 2, 2, 2, 2 },
                                    new Object[]{ 3, 3, 3, 3 }
                            )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test2 ORDER BY ct2id" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1 },
                                    new Object[]{ 3, 3 }
                            )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test2" );
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @ParameterizedTest(name = "{index}. Create Index: {0}")
    @ValueSource(booleans = { false, true })
    public void testDeleteNoConflict( boolean useIndex ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                if ( useIndex && helper.storeSupportsIndex() ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)" );
                    statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (1, 1), (3, 3)" );
                    statement.executeUpdate( "DELETE FROM constraint_test WHERE ctid = 2" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 3, 3, 3, 3 }
                            )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test2 ORDER BY ct2id" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1 },
                                    new Object[]{ 3, 3 }
                            )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test2" );
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }


    @ParameterizedTest(name = "{index}. Create Index: {0}")
    @ValueSource(booleans = { false, true })
    public void testDeleteConflict( boolean useIndex ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create schema
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST );
                statement.executeUpdate( CREATE_TABLE_CONSTRAINT_TEST2 );
                statement.executeUpdate( ALTER_TABLE_ADD_FK );
                if ( useIndex && helper.storeSupportsIndex() ) {
                    // Add indexes
                    for ( String s : ALTER_TABLE_ADD_INDEX_STATEMENTS ) {
                        statement.executeUpdate( s );
                    }
                }

                try {
                    statement.executeUpdate( "INSERT INTO constraint_test VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)" );
                    statement.executeUpdate( "INSERT INTO constraint_test2 VALUES (1, 1), (3, 3)" );
                    connection.commit();
                    try {
                        statement.executeUpdate( "DELETE FROM constraint_test WHERE ctid = 1" );
                        connection.commit();
                        Assertions.fail( "Expected ConstraintViolationException was not thrown" );
                    } catch ( AvaticaClientRuntimeException e ) {
                        if ( !(e.getMessage().contains( "Remote driver error:" )
                                && e.getMessage().contains( "Transaction violates foreign key constraint" )) ) {
                            throw new RuntimeException( "Unexpected exception", e );
                        }
                    }
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 2, 2, 2, 2 },
                                    new Object[]{ 3, 3, 3, 3 }
                            )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM constraint_test2 ORDER BY ct2id" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1 },
                                    new Object[]{ 3, 3 }
                            )
                    );
                } finally {
                    statement.executeUpdate( "DROP TABLE constraint_test2" );
                    statement.executeUpdate( "DROP TABLE constraint_test" );
                }
            }
        }
    }

}
