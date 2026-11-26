/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.sql;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.ConstraintStrategy;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Transaction;


@Tag("adapter")
class IdentityDmlTest {

    private static final String RELATIONAL_FIXTURE = "identity_dml_relational_store";
    private static String relationalStore;


    @BeforeAll
    static void start() throws SQLException {
        TestHelper.getInstance();
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            relationalStore = TestHelper.useJdbcTestStore( RELATIONAL_FIXTURE, sql );
        }
    }


    @AfterAll
    static void dropRelationalFixture() throws SQLException {
        if ( RELATIONAL_FIXTURE.equals( relationalStore ) ) {
            try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
                TestHelper.dropAdapter( RELATIONAL_FIXTURE, sql );
            }
        }
    }


    @Test
    void valuesSelectAndPreparedBatchUseOneValuePerDefault() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Connection connection = jdbc.getConnection();
                Statement statement = connection.createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_dml (id BIGSERIAL PRIMARY KEY, label VARCHAR(40) NOT NULL DEFAULT 'default row')" );
                assertEquals( 3, statement.executeUpdate( "INSERT INTO identity_dml VALUES (DEFAULT, 'first'), (50, 'explicit'), (DEFAULT, 'second')" ) );
                assertEquals( 1, statement.executeUpdate( "INSERT INTO identity_dml DEFAULT VALUES" ) );
                assertEquals( 2, statement.executeUpdate( "INSERT INTO identity_dml (label)"
                        + " SELECT CASE id WHEN 1 THEN 'first copy' ELSE 'second copy' END FROM identity_dml WHERE id < 3" ) );
                try ( PreparedStatement insert = connection.prepareStatement( "INSERT INTO identity_dml (label) VALUES (?)" ) ) {
                    insert.setString( 1, "batch first" );
                    insert.addBatch();
                    insert.setString( 1, "batch second" );
                    insert.addBatch();
                    assertArrayEquals( new int[]{ Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO }, insert.executeBatch() );
                    insert.setString( 1, "after batch" );
                    assertEquals( 1, insert.executeUpdate() );
                }
                assertThrows( SQLException.class, () -> statement.executeUpdate( "INSERT INTO identity_dml VALUES (NULL, 'invalid')" ) );
                Map<String, Long> rows = readRows( statement, "identity_dml" );
                assertEquals( 9, rows.size() );
                assertEquals( 1L, rows.get( "first" ) );
                assertEquals( 2L, rows.get( "second" ) );
                assertEquals( 3L, rows.get( "default row" ) );
                assertEquals( 8L, rows.get( "after batch" ) );
                assertEquals( 6L, rows.get( "batch first" ) );
                assertEquals( 7L, rows.get( "batch second" ) );
                assertEquals( 50L, rows.get( "explicit" ) );
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_dml" );
            }
        }
    }


    @Test
    void alwaysIdentityRequiresExplicitOverride() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement statement = jdbc.getConnection().createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_always (id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, label VARCHAR(40))" );
                assertThrows( SQLException.class, () -> statement.executeUpdate( "INSERT INTO identity_always VALUES (40, 'rejected')" ) );
                assertEquals( 1, statement.executeUpdate( "INSERT INTO identity_always OVERRIDING SYSTEM VALUE VALUES (40, 'explicit')" ) );
                assertEquals( 1, statement.executeUpdate( "INSERT INTO identity_always OVERRIDING USER VALUE VALUES (NULL, 'generated')" ) );
                assertEquals( 1, statement.executeUpdate( "INSERT INTO identity_always VALUES (DEFAULT, 'another')" ) );
                assertEquals( Map.of( "explicit", 40L, "generated", 1L, "another", 2L ), readRows( statement, "identity_always" ) );
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_always" );
            }
        }
    }


    @Test
    void overridingUserValuePreservesDistinctSourceRows() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement statement = jdbc.getConnection().createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_union (id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, label VARCHAR(40))" );
                try ( ResultSet rows = statement.executeQuery( "INSERT INTO identity_union (id, label) OVERRIDING USER VALUE "
                        + "SELECT 100, 'same' UNION SELECT 200, 'same' RETURNING id, label" ) ) {
                    assertTrue( rows.next() );
                    assertEquals( 1, rows.getInt( 1 ) );
                    assertEquals( "same", rows.getString( 2 ) );
                    assertTrue( rows.next() );
                    assertEquals( 2, rows.getInt( 1 ) );
                    assertEquals( "same", rows.getString( 2 ) );
                    assertFalse( rows.next() );
                }
                assertEquals( 1, statement.executeUpdate( "INSERT INTO identity_union (id, label) OVERRIDING USER VALUE SELECT NULL, 'null supplied'" ) );
                assertEquals( 3L, readRows( statement, "identity_union" ).get( "null supplied" ) );
                try ( ResultSet rows = statement.executeQuery( "INSERT INTO identity_union (id, label) OVERRIDING USER VALUE "
                        + "SELECT s.id, s.label FROM (SELECT 300 AS id, 'nested' AS label, 1 AS ordinal "
                        + "UNION SELECT 300, 'nested', 2) AS s RETURNING id" ) ) {
                    assertTrue( rows.next() );
                    assertEquals( 4, rows.getInt( 1 ) );
                    assertTrue( rows.next() );
                    assertEquals( 5, rows.getInt( 1 ) );
                    assertFalse( rows.next() );
                }
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_union" );
            }
        }
    }


    @Test
    void writesSameIdentityValuesToEveryPlacementAndRoutesByGeneratedPrimaryKey() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement statement = jdbc.getConnection().createStatement() ) {
            boolean addedReplica = false;
            try {
                statement.executeUpdate( "CREATE TABLE identity_partitioned (id SERIAL PRIMARY KEY, label VARCHAR(40)) PARTITION BY HASH (id) PARTITIONS 3" );
                TestHelper.addHsqldb( "identity_replica", statement );
                addedReplica = true;
                statement.executeUpdate( "ALTER TABLE identity_partitioned ADD PLACEMENT ON STORE identity_replica" );
                assertEquals( 4, statement.executeUpdate( "INSERT INTO identity_partitioned (label) VALUES ('a'), ('b'), ('c'), ('d')" ) );
                Map<String, Long> expected = Map.of( "a", 1L, "b", 2L, "c", 3L, "d", 4L );
                assertEquals( expected, readRows( statement, "identity_partitioned" ) );
                statement.executeUpdate( "ALTER TABLE identity_partitioned DROP PLACEMENT ON STORE hsqldb" );
                assertEquals( expected, readRows( statement, "identity_partitioned" ) );
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_partitioned" );
                if ( addedReplica ) {
                    statement.executeUpdate( "ALTER ADAPTERS DROP identity_replica" );
                }
            }
        }
    }


    @Test
    void updateDefaultAllocatesPerMatchedRow() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement statement = jdbc.getConnection().createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_update (pk INTEGER PRIMARY KEY, id BIGSERIAL, label VARCHAR(40))" );
                statement.executeUpdate( "INSERT INTO identity_update (pk, label) VALUES (11, 'a'), (12, 'b')" );
                assertEquals( 0, statement.executeUpdate( "UPDATE identity_update SET id = DEFAULT WHERE pk = 13" ) );
                assertEquals( 2, statement.executeUpdate( "UPDATE identity_update SET id = DEFAULT WHERE pk IN (11, 12)" ) );
                Map<String, Long> rows = readRows( statement, "identity_update" );
                assertEquals( 2, rows.size() );
                assertEquals( java.util.Set.of( 3L, 4L ), new java.util.HashSet<>( rows.values() ) );
                statement.executeUpdate( "INSERT INTO identity_update (pk, label) VALUES (13, 'c')" );
                assertEquals( 5L, readRows( statement, "identity_update" ).get( "c" ) );
                try ( PreparedStatement update = jdbc.getConnection().prepareStatement( "UPDATE identity_update SET id = DEFAULT, label = ? WHERE pk = ?" ) ) {
                    update.setString( 1, "updated label" );
                    update.setInt( 2, 13 );
                    assertEquals( 1, update.executeUpdate() );
                }
                assertEquals( 6L, readRows( statement, "identity_update" ).get( "updated label" ) );
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_update" );
            }
        }
    }


    @Test
    void multirowIdentityUpdateChecksUniquenessAtStatementBoundary() throws SQLException {
        boolean uniqueConstraints = RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean();
        Enum<?> strategy = RuntimeConfig.CONSTRAINT_ENFORCEMENT_STRATEGY.getEnum();
        RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.setBoolean( true );
        RuntimeConfig.CONSTRAINT_ENFORCEMENT_STRATEGY.setEnum( ConstraintStrategy.AFTER_QUERY_EXECUTION );
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement statement = jdbc.getConnection().createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_unique_update (pk INTEGER PRIMARY KEY, seq SERIAL, u INTEGER, CONSTRAINT identity_u UNIQUE (u))" );
                assertEquals( 2, statement.executeUpdate( "INSERT INTO identity_unique_update (pk, u) VALUES (1, 1), (2, 2)" ) );
                assertEquals( 2, statement.executeUpdate( "UPDATE identity_unique_update SET seq = DEFAULT" ) );
                assertEquals( 2, statement.executeUpdate( "UPDATE identity_unique_update SET seq = DEFAULT, u = CASE u WHEN 1 THEN 2 ELSE 1 END" ) );
                SQLException violation = assertThrows( SQLException.class, () -> statement.executeUpdate( "UPDATE identity_unique_update SET seq = DEFAULT, u = 1" ) );
                assertTrue( violation.getMessage().contains( "identity_u" ), violation.getMessage() );
                try ( ResultSet rows = statement.executeQuery( "SELECT pk, seq, u FROM identity_unique_update ORDER BY pk" ) ) {
                    assertTrue( rows.next() );
                    assertEquals( 1, rows.getInt( 1 ) );
                    int firstSequence = rows.getInt( 2 );
                    assertEquals( 2, rows.getInt( 3 ) );
                    assertTrue( rows.next() );
                    assertEquals( 2, rows.getInt( 1 ) );
                    assertEquals( Set.of( 5, 6 ), Set.of( firstSequence, rows.getInt( 2 ) ) );
                    assertEquals( 1, rows.getInt( 3 ) );
                    assertFalse( rows.next() );
                }
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_unique_update" );
            }
        } finally {
            RuntimeConfig.CONSTRAINT_ENFORCEMENT_STRATEGY.setEnum( strategy );
            RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.setBoolean( uniqueConstraints );
        }
    }


    @Test
    void beforeExecutionChecksValidateAllMaterializedAssignments() throws SQLException {
        boolean uniqueConstraints = RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean();
        Enum<?> strategy = RuntimeConfig.CONSTRAINT_ENFORCEMENT_STRATEGY.getEnum();
        RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.setBoolean( true );
        RuntimeConfig.CONSTRAINT_ENFORCEMENT_STRATEGY.setEnum( ConstraintStrategy.BEFORE_QUERY_EXECUTION );
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement statement = jdbc.getConnection().createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_before_update (pk INTEGER PRIMARY KEY, seq SERIAL, u INTEGER, CONSTRAINT identity_before_u UNIQUE (u))"
                        + " ON STORE " + relationalStore );
                assertEquals( 2, statement.executeUpdate( "INSERT INTO identity_before_update (pk, u) VALUES (1, 1), (2, 2)" ) );
                assertEquals( 2, statement.executeUpdate( "UPDATE identity_before_update SET seq = DEFAULT, u = CASE u WHEN 1 THEN 11 ELSE 12 END WHERE pk > 0" ) );
                SQLException violation = assertThrows( SQLException.class, () -> statement.executeUpdate( "UPDATE identity_before_update SET seq = DEFAULT, u = 99" ) );
                assertTrue( violation.getMessage().contains( "identity_before_u" ), violation.getMessage() );
                try ( ResultSet rows = statement.executeQuery( "SELECT pk, seq, u FROM identity_before_update ORDER BY pk" ) ) {
                    assertTrue( rows.next() );
                    assertEquals( 1, rows.getInt( 1 ) );
                    int firstSequence = rows.getInt( 2 );
                    assertEquals( 11, rows.getInt( 3 ) );
                    assertTrue( rows.next() );
                    assertEquals( 2, rows.getInt( 1 ) );
                    assertEquals( Set.of( 3, 4 ), Set.of( firstSequence, rows.getInt( 2 ) ) );
                    assertEquals( 12, rows.getInt( 3 ) );
                    assertFalse( rows.next() );
                }
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_before_update" );
            }
        } finally {
            RuntimeConfig.CONSTRAINT_ENFORCEMENT_STRATEGY.setEnum( strategy );
            RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.setBoolean( uniqueConstraints );
        }
    }


    @Test
    void preparedSelectUsesCurrentSequenceWithDmlCachesEnabled() throws SQLException {
        boolean addDefaults = RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean();
        boolean cachePlans = RuntimeConfig.QUERY_PLAN_CACHING_DML.getBoolean();
        boolean cacheImplementations = RuntimeConfig.IMPLEMENTATION_CACHING_DML.getBoolean();
        RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.setBoolean( false );
        RuntimeConfig.QUERY_PLAN_CACHING_DML.setBoolean( true );
        RuntimeConfig.IMPLEMENTATION_CACHING_DML.setBoolean( true );
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Connection connection = jdbc.getConnection();
                Statement statement = connection.createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_cached (id BIGSERIAL PRIMARY KEY, label VARCHAR(40) NOT NULL)" );
                try ( PreparedStatement insert = connection.prepareStatement( "INSERT INTO identity_cached (label) SELECT CAST(? AS VARCHAR(40))" ) ) {
                    insert.setString( 1, "first" );
                    assertEquals( 1, insert.executeUpdate() );
                    insert.setString( 1, "second" );
                    assertEquals( 1, insert.executeUpdate() );
                    statement.executeUpdate( "ALTER TABLE identity_cached ALTER COLUMN id RESTART WITH 50" );
                    insert.setString( 1, "restarted" );
                    assertEquals( 1, insert.executeUpdate() );
                }
                assertEquals( Map.of( "first", 1L, "second", 2L, "restarted", 50L ), readRows( statement, "identity_cached" ) );
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_cached" );
            }
        } finally {
            RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.setBoolean( addDefaults );
            RuntimeConfig.QUERY_PLAN_CACHING_DML.setBoolean( cachePlans );
            RuntimeConfig.IMPLEMENTATION_CACHING_DML.setBoolean( cacheImplementations );
        }
    }


    @Test
    void identityModeChangeInvalidatesAlreadyValidatedDml() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement statement = jdbc.getConnection().createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_lock_check (id SERIAL PRIMARY KEY, label VARCHAR(40))" );
                Transaction transaction = TestHelper.getInstance().getTransaction();
                try {
                    String sql = "INSERT INTO identity_lock_check VALUES (42, 'explicit')";
                    QueryLanguage language = QueryLanguage.from( "sql" );
                    Processor processor = transaction.getProcessor( language );
                    Node validated = processor.validate( transaction, processor.parse( sql ).get( 0 ), false ).left;
                    var internal = transaction.createStatement();
                    QueryContext context = QueryContext.builder()
                            .query( sql )
                            .language( language )
                            .namespaceId( transaction.getDefaultNamespace().id )
                            .transactions( List.of( transaction ) )
                            .transactionManager( transaction.getTransactionManager() )
                            .origin( "IdentityDmlTest" )
                            .build();
                    statement.executeUpdate( "ALTER TABLE identity_lock_check ALTER COLUMN id SET GENERATED ALWAYS" );
                    AlgRoot root = processor.translate( internal, ParsedQueryContext.fromQuery( sql, validated, context ) );
                    GenericRuntimeException failure = assertThrows( GenericRuntimeException.class, () -> internal.getQueryProcessor().prepareQuery( root, false ) );
                    assertTrue( failure.getMessage().contains( "changed during planning" ) );
                } finally {
                    transaction.rollback( null );
                }
                assertEquals( Map.of(), readRows( statement, "identity_lock_check" ) );
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_lock_check" );
            }
        }
    }


    @Test
    void preparedInsertWithoutParametersPreservesEveryBatchEntry() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Connection connection = jdbc.getConnection();
                Statement statement = connection.createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_zero_parameter_batch (id SERIAL PRIMARY KEY, n INTEGER NOT NULL DEFAULT 7)" );
                try ( PreparedStatement insert = connection.prepareStatement( "INSERT INTO identity_zero_parameter_batch DEFAULT VALUES" ) ) {
                    insert.addBatch();
                    insert.addBatch();
                    assertArrayEquals( new int[]{ 1, 1 }, insert.executeBatch() );
                }
                try ( PreparedStatement insert = connection.prepareStatement( "INSERT INTO identity_zero_parameter_batch VALUES (DEFAULT, 9)" ) ) {
                    insert.addBatch();
                    insert.addBatch();
                    assertArrayEquals( new int[]{ 1, 1 }, insert.executeBatch() );
                }
                assertEquals( 4, singleLong( statement, "SELECT COUNT(*) FROM identity_zero_parameter_batch" ) );
                assertEquals( 2, singleLong( statement, "SELECT COUNT(*) FROM identity_zero_parameter_batch WHERE n = 7" ) );
                assertEquals( 4, singleLong( statement, "SELECT MAX(id) FROM identity_zero_parameter_batch" ) );
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_zero_parameter_batch" );
            }
        }
    }


    @Test
    void preparedInsertSelectReadsEarlierBatchWrites() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Connection connection = jdbc.getConnection();
                Statement statement = connection.createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_select_batch (id SERIAL PRIMARY KEY, n INTEGER NOT NULL)" );
                statement.executeUpdate( "INSERT INTO identity_select_batch (n) VALUES (0)" );
                try ( PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO identity_select_batch (n) SELECT MAX(n) + CAST(? AS INTEGER) FROM identity_select_batch" ) ) {
                    for ( int i = 0; i < 2; i++ ) {
                        insert.setInt( 1, 1 );
                        insert.addBatch();
                    }
                    assertArrayEquals( new int[]{ 1, 1 }, insert.executeBatch() );
                }
                try ( ResultSet rows = statement.executeQuery( "SELECT id, n FROM identity_select_batch ORDER BY id" ) ) {
                    for ( int id = 1; id <= 3; id++ ) {
                        assertTrue( rows.next() );
                        assertEquals( id, rows.getInt( 1 ) );
                        assertEquals( id - 1, rows.getInt( 2 ) );
                    }
                    assertFalse( rows.next() );
                }
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_select_batch" );
            }
        }
    }


    @Test
    void preparedDefaultUpdateSeesEarlierBatchWrites() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Connection connection = jdbc.getConnection();
                Statement statement = connection.createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_update_batch (pk INTEGER PRIMARY KEY, seq SERIAL, n INTEGER NOT NULL)" );
                statement.executeUpdate( "INSERT INTO identity_update_batch VALUES (1, 100, 0)" );
                try ( PreparedStatement update = connection.prepareStatement( "UPDATE identity_update_batch SET seq = DEFAULT, n = n + ? WHERE pk = ?" ) ) {
                    for ( int i = 0; i < 2; i++ ) {
                        update.setInt( 1, 1 );
                        update.setInt( 2, 1 );
                        update.addBatch();
                    }
                    update.setInt( 1, 50 );
                    update.setInt( 2, 99 );
                    update.addBatch();
                    assertArrayEquals( new int[]{ 1, 1, 0 }, update.executeBatch() );
                }
                try ( ResultSet result = statement.executeQuery( "SELECT seq, n FROM identity_update_batch WHERE pk = 1" ) ) {
                    assertTrue( result.next() );
                    assertEquals( 2, result.getInt( 1 ) );
                    assertEquals( 2, result.getInt( 2 ) );
                }
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_update_batch" );
            }
        }
    }


    @Test
    void partitionedUpdateDoesNotPruneDisjunctivePredicate() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement statement = jdbc.getConnection().createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_partitioned_or (id INTEGER PRIMARY KEY, n INTEGER NOT NULL) PARTITION BY HASH (id) PARTITIONS 3" );
                statement.executeUpdate( "INSERT INTO identity_partitioned_or VALUES (1,7), (2,7), (3,7), (4,7), (5,7), (6,7), (7,7), (8,7), (9,7)" );
                assertEquals( 9, statement.executeUpdate( "UPDATE identity_partitioned_or SET n = 1 WHERE id = 1 OR n = 7" ) );
                assertEquals( 9, singleLong( statement, "SELECT COUNT(*) FROM identity_partitioned_or WHERE n = 1" ) );
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_partitioned_or" );
            }
        }
    }


    @Test
    void generatedPrimaryKeyUpdateRejectsRetargetingAndCanRekeySafely() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement statement = jdbc.getConnection().createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_update_key (id SERIAL PRIMARY KEY, label VARCHAR(40))" );
                statement.executeUpdate( "INSERT INTO identity_update_key (label) VALUES ('first'), ('second')" );
                statement.executeUpdate( "ALTER TABLE identity_update_key ALTER COLUMN id RESTART WITH 2" );
                assertThrows( SQLException.class, () -> statement.executeUpdate( "UPDATE identity_update_key SET id = DEFAULT WHERE id = 1" ) );
                Map<String, Long> original = Map.of( "first", 1L, "second", 2L );
                assertEquals( original, readRows( statement, "identity_update_key" ) );

                // Depending on scan order, restarting at 2 can make one row's new key
                // overlap another row's old key. Either outcome must preserve both rows.
                statement.executeUpdate( "ALTER TABLE identity_update_key ALTER COLUMN id RESTART WITH 2" );
                boolean rejected = false;
                try {
                    assertEquals( 2, statement.executeUpdate( "UPDATE identity_update_key SET id = DEFAULT" ) );
                } catch ( SQLException overlap ) {
                    rejected = true;
                }
                assertEquals( rejected ? original : Map.of( "first", 3L, "second", 2L ), readRows( statement, "identity_update_key" ) );

                statement.executeUpdate( "ALTER TABLE identity_update_key ALTER COLUMN id RESTART WITH 10" );
                assertEquals( 2, statement.executeUpdate( "UPDATE identity_update_key SET id = DEFAULT" ) );
                Map<String, Long> rows = readRows( statement, "identity_update_key" );
                assertEquals( 2, rows.size() );
                assertEquals( java.util.Set.of( 10L, 11L ), new java.util.HashSet<>( rows.values() ) );
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_update_key" );
            }
        }
    }


    @Test
    void aPlannedIdentityInsertRejectsAReorderedTargetLayout() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement statement = jdbc.getConnection().createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_reordered (id SERIAL PRIMARY KEY, payload INTEGER) ON STORE " + relationalStore );
                Transaction transaction = TestHelper.getInstance().getTransaction();
                try {
                    String sql = "INSERT INTO identity_reordered (payload) VALUES (500)";
                    QueryLanguage language = QueryLanguage.from( "sql" );
                    Processor processor = transaction.getProcessor( language );
                    Node validated = processor.validate( transaction, processor.parse( sql ).get( 0 ), false ).left;
                    var internal = transaction.createStatement();
                    QueryContext context = QueryContext.builder()
                            .query( sql )
                            .language( language )
                            .namespaceId( transaction.getDefaultNamespace().id )
                            .transactions( List.of( transaction ) )
                            .transactionManager( transaction.getTransactionManager() )
                            .origin( "IdentityDmlTest" )
                            .build();
                    AlgRoot plan = processor.translate( internal, ParsedQueryContext.fromQuery( sql, validated, context ) );
                    statement.executeUpdate( "ALTER TABLE identity_reordered MODIFY COLUMN payload SET POSITION BEFORE id" );
                    GenericRuntimeException failure = assertThrows( GenericRuntimeException.class, () -> internal.getQueryProcessor().prepareQuery( plan, false ) );
                    assertTrue( failure.getMessage().contains( "changed during planning" ) );
                } finally {
                    transaction.rollback( null );
                }
                assertEquals( 0, singleLong( statement, "SELECT COUNT(*) FROM identity_reordered" ) );
                assertEquals( 1, statement.executeUpdate( "INSERT INTO identity_reordered (payload) VALUES (500)" ) );
                assertEquals( 500, singleLong( statement, "SELECT payload FROM identity_reordered WHERE id = 1" ) );
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_reordered" );
            }
        }
    }


    @Test
    void emptyPartitionedInsertSelectReturnsZeroWithoutConsumingIdentity() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement statement = jdbc.getConnection().createStatement() ) {
            try {
                statement.executeUpdate( "CREATE TABLE identity_empty_partition (id SERIAL PRIMARY KEY, n INTEGER NOT NULL) PARTITION BY HASH (id) PARTITIONS 3" );
                assertEquals( 0, statement.executeUpdate( "INSERT INTO identity_empty_partition (n) SELECT n FROM identity_empty_partition" ) );
                assertEquals( 1, statement.executeUpdate( "INSERT INTO identity_empty_partition (n) VALUES (10)" ) );
                assertEquals( 1, singleLong( statement, "SELECT id FROM identity_empty_partition" ) );
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_empty_partition" );
            }
        }
    }


    @Test
    void generatedKeysRouteAcrossSplitPlacements() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement statement = jdbc.getConnection().createStatement() ) {
            boolean addedStore = false;
            try {
                statement.executeUpdate( "CREATE TABLE identity_split (id SERIAL PRIMARY KEY, n INTEGER) PARTITION BY HASH (id) WITH (p0, p1, p2)" );
                TestHelper.addHsqldb( "identity_split_store", statement );
                addedStore = true;
                statement.executeUpdate( "ALTER TABLE identity_split ADD PLACEMENT ON STORE identity_split_store" );
                statement.executeUpdate( "ALTER TABLE identity_split MODIFY PARTITIONS (p0) ON STORE hsqldb" );
                statement.executeUpdate( "ALTER TABLE identity_split MODIFY PARTITIONS (p1, p2) ON STORE identity_split_store" );
                assertEquals( 9, statement.executeUpdate( "INSERT INTO identity_split (n) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9)" ) );
                assertEquals( 9, singleLong( statement, "SELECT COUNT(*) FROM identity_split WHERE id = n" ) );
            } finally {
                statement.executeUpdate( "DROP TABLE IF EXISTS identity_split" );
                if ( addedStore ) {
                    statement.executeUpdate( "ALTER ADAPTERS DROP identity_split_store" );
                }
            }
        }
    }


    private static long singleLong( Statement statement, String sql ) throws SQLException {
        try ( ResultSet result = statement.executeQuery( sql ) ) {
            assertTrue( result.next() );
            return result.getLong( 1 );
        }
    }


    private static Map<String, Long> readRows( Statement statement, String table ) throws SQLException {
        Map<String, Long> rows = new HashMap<>();
        try ( ResultSet result = statement.executeQuery( "SELECT id, label FROM " + table ) ) {
            while ( result.next() ) {
                rows.put( result.getString( 2 ), result.getLong( 1 ) );
            }
        }
        return rows;
    }

}
