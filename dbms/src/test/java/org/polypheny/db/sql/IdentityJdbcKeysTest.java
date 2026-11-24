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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;

@Tag("adapter")
class IdentityJdbcKeysTest {

    @BeforeAll
    static void start() {
        TestHelper.getInstance();
    }


    @Test
    void nativeJdbcKeysAndMetadataUseThePrismCapability() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Connection connection = jdbc.getConnection();
                Statement sql = connection.createStatement() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsGetGeneratedKeys() );

            sql.executeUpdate( "CREATE TABLE identity_jdbc_keys (id BIGSERIAL PRIMARY KEY, label VARCHAR(50))" );
            try {
                assertEquals( 2, sql.executeLargeUpdate(
                        "INSERT INTO identity_jdbc_keys (label) VALUES ('a'), ('b')", Statement.RETURN_GENERATED_KEYS ) );
                assertEquals( List.of( 1L, 2L ), readKeys( sql ) );
                assertEquals( 1, sql.executeUpdate( "INSERT INTO identity_jdbc_keys VALUES (500, 'explicit')", Statement.RETURN_GENERATED_KEYS ) );
                assertEquals( List.of(), readKeys( sql ) );
                assertEquals( 1, sql.executeUpdate( "INSERT INTO identity_jdbc_keys VALUES (DEFAULT, 'c')", new int[]{ 1 } ) );
                assertEquals( List.of( 3L ), readKeys( sql ) );

                assertFalse( sql.execute( "INSERT INTO identity_jdbc_keys VALUES (DEFAULT, 'named')", new String[]{ "label", "id" } ) );
                assertEquals( 1, sql.getUpdateCount() );
                try ( ResultSet keys = sql.getGeneratedKeys() ) {
                    assertTrue( keys.next() );
                    assertEquals( "named", keys.getString( 1 ) );
                    assertEquals( 4, keys.getLong( 2 ) );
                    assertTrue( keys.getMetaData().isAutoIncrement( 2 ) );
                    assertFalse( keys.getMetaData().isAutoIncrement( 1 ) );
                    assertFalse( keys.next() );
                }

                try ( PreparedStatement insert = connection.prepareStatement( "INSERT INTO identity_jdbc_keys (label) VALUES (?)", Statement.RETURN_GENERATED_KEYS ) ) {
                    insert.setString( 1, "batch a" );
                    insert.addBatch();
                    insert.setString( 1, "batch b" );
                    insert.addBatch();
                    assertArrayEquals( new int[]{ 1, 1 }, insert.executeBatch() );
                    assertEquals( List.of( 5L, 6L ), readKeys( insert ) );
                    insert.setString( 1, "after batch" );
                    assertEquals( 1, insert.executeUpdate() );
                    assertEquals( List.of( 7L ), readKeys( insert ) );
                }

                sql.addBatch( "INSERT INTO identity_jdbc_keys DEFAULT VALUES" );
                sql.addBatch( "INSERT INTO identity_jdbc_keys DEFAULT VALUES" );
                assertArrayEquals( new int[]{ 1, 1 }, sql.executeBatch() );
                assertEquals( List.of( 8L, 9L ), readKeys( sql ) );

                assertThrows( SQLException.class, () -> sql.executeUpdate(
                        "INSERT INTO identity_jdbc_keys DEFAULT VALUES", new String[]{ "unknown" } ) );
                assertEquals( List.of(), readKeys( sql ) );
                sql.executeUpdate( "ALTER TABLE identity_jdbc_keys ALTER COLUMN id RESTART WITH 9223372036854775807" );
                assertEquals( 1, sql.executeUpdate( "INSERT INTO identity_jdbc_keys DEFAULT VALUES", Statement.RETURN_GENERATED_KEYS ) );
                assertEquals( List.of( Long.MAX_VALUE ), readKeys( sql ) );

                Map<String, String> autoIncrement = new HashMap<>();
                try ( ResultSet fields = metadata.getColumns( null, Catalog.DEFAULT_NAMESPACE_NAME, "identity_jdbc_keys", "%" ) ) {
                    while ( fields.next() ) {
                        autoIncrement.put( fields.getString( "COLUMN_NAME" ), fields.getString( "IS_AUTOINCREMENT" ) );
                    }
                }
                assertEquals( Map.of( "id", "YES", "label", "NO" ), autoIncrement );
                try ( ResultSet rows = sql.executeQuery( "SELECT id AS direct_id, id + 1 AS derived_id, label FROM identity_jdbc_keys WHERE id = 1" ) ) {
                    assertTrue( rows.next() );
                    assertTrue( rows.getMetaData().isAutoIncrement( 1 ) );
                    assertFalse( rows.getMetaData().isAutoIncrement( 2 ) );
                    assertFalse( rows.getMetaData().isAutoIncrement( 3 ) );
                }
                try ( ResultSet rows = sql.executeQuery( "SELECT q.key_id FROM (SELECT id AS key_id FROM identity_jdbc_keys WHERE id = 1 "
                        + "UNION ALL SELECT CAST(99 AS BIGINT)) AS q ORDER BY q.key_id" ) ) {
                    assertFalse( rows.getMetaData().isAutoIncrement( 1 ) );
                    assertTrue( rows.next() );
                    assertEquals( 1, rows.getLong( 1 ) );
                    assertTrue( rows.next() );
                    assertEquals( 99, rows.getLong( 1 ) );
                }
                try ( ResultSet rows = sql.executeQuery( "SELECT id FROM identity_jdbc_keys WHERE id = 1 "
                        + "UNION ALL SELECT id FROM identity_jdbc_keys WHERE id = 2" ) ) {
                    assertTrue( rows.getMetaData().isAutoIncrement( 1 ) );
                    assertTrue( rows.next() );
                }
                assertEquals( List.of(), readKeys( sql ) );
            } finally {
                sql.executeUpdate( "DROP TABLE IF EXISTS identity_jdbc_keys" );
            }
        }
    }


    @Test
    void returningAndGeneratedKeysUseTheQualifiedInsertTarget() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE SCHEMA qualified_identity_keys" );
            try {
                sql.executeUpdate( "CREATE TABLE qualified_identity_keys.identity_scoped_keys (id BIGSERIAL PRIMARY KEY, marker BIGINT)" );
                try ( ResultSet rows = sql.executeQuery( "INSERT INTO qualified_identity_keys.identity_scoped_keys (marker) VALUES (101) RETURNING id" ) ) {
                    TestHelper.checkResultSet( rows, List.<Object[]>of( new Object[]{ 1L } ) );
                }
                assertEquals( 1, sql.executeUpdate( "INSERT INTO qualified_identity_keys.identity_scoped_keys (marker) VALUES (102)", Statement.RETURN_GENERATED_KEYS ) );
                assertEquals( List.of( 2L ), readKeys( sql ) );

                sql.executeUpdate( "CREATE TABLE identity_scoped_keys (marker BIGINT, id BIGSERIAL PRIMARY KEY)" );
                try ( ResultSet rows = sql.executeQuery( "INSERT INTO qualified_identity_keys.identity_scoped_keys (marker) VALUES (103) RETURNING id" ) ) {
                    TestHelper.checkResultSet( rows, List.<Object[]>of( new Object[]{ 3L } ) );
                }
                assertEquals( 1, sql.executeUpdate( "INSERT INTO qualified_identity_keys.identity_scoped_keys (marker) VALUES (104)", Statement.RETURN_GENERATED_KEYS ) );
                assertEquals( List.of( 4L ), readKeys( sql ) );
                try ( PreparedStatement insert = jdbc.getConnection().prepareStatement(
                        "INSERT INTO qualified_identity_keys.identity_scoped_keys (marker) VALUES (?)", new String[]{ "id" } ) ) {
                    insert.setLong( 1, 105 );
                    assertEquals( 1, insert.executeUpdate() );
                    try ( ResultSet keys = insert.getGeneratedKeys() ) {
                        assertTrue( keys.next() );
                        assertEquals( 5, keys.getLong( 1 ) );
                        assertEquals( "qualified_identity_keys", keys.getMetaData().getSchemaName( 1 ) );
                        assertFalse( keys.next() );
                    }
                }

                try ( ResultSet rows = sql.executeQuery( "SELECT id, marker FROM qualified_identity_keys.identity_scoped_keys ORDER BY id" ) ) {
                    TestHelper.checkResultSet( rows, List.of(
                            new Object[]{ 1L, 101L }, new Object[]{ 2L, 102L }, new Object[]{ 3L, 103L }, new Object[]{ 4L, 104L }, new Object[]{ 5L, 105L } ) );
                }
                try ( ResultSet rows = sql.executeQuery( "SELECT id FROM identity_scoped_keys" ) ) {
                    assertFalse( rows.next() );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE IF EXISTS identity_scoped_keys" );
                sql.executeUpdate( "DROP TABLE IF EXISTS qualified_identity_keys.identity_scoped_keys" );
                sql.executeUpdate( "DROP SCHEMA qualified_identity_keys" );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void generatedKeysInsertHonorsNetworkTimeoutBehindWriteLock( boolean prepared ) throws Exception {
        try ( JdbcConnection admin = new JdbcConnection( true ); Statement sql = admin.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE identity_jdbc_deadline (id BIGSERIAL PRIMARY KEY, label INTEGER NOT NULL)" );
            try {
                sql.executeUpdate( "INSERT INTO identity_jdbc_deadline (label) VALUES (0)" );
                assertBlockedInsertTimesOut( prepared );
            } finally {
                sql.executeUpdate( "DROP TABLE identity_jdbc_deadline" );
            }
        }
    }


    private static void assertBlockedInsertTimesOut( boolean prepared ) throws Exception {
        String insert = "INSERT INTO identity_jdbc_deadline (label) VALUES (" + (prepared ? "?" : "1") + ")";
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try ( JdbcConnection blocking = new JdbcConnection( false ); JdbcConnection jdbc = new JdbcConnection( true );
                Statement writer = blocking.getConnection().createStatement();
                Statement statement = prepared ? jdbc.getConnection().prepareStatement( insert, Statement.RETURN_GENERATED_KEYS )
                        : jdbc.getConnection().createStatement() ) {
            if ( prepared ) {
                ((PreparedStatement) statement).setInt( 1, 2 );
            }
            Connection connection = jdbc.getConnection();
            connection.setNetworkTimeout( Runnable::run, 250 );
            writer.executeUpdate( "UPDATE identity_jdbc_deadline SET label = 3 WHERE id = 1" );
            Future<?> release = executor.schedule( () -> {
                blocking.getConnection().rollback();
                return null;
            }, 2, TimeUnit.SECONDS );
            try {
                SQLException failure = assertThrows( SQLException.class, () -> {
                    if ( prepared ) {
                        ((PreparedStatement) statement).executeUpdate();
                    } else {
                        statement.executeUpdate( insert, Statement.RETURN_GENERATED_KEYS );
                    }
                } );
                assertEquals( "08003", failure.getSQLState() );
                assertInstanceOf( TimeoutException.class, failure.getCause() );
                assertTrue( connection.isClosed() );
                assertTrue( statement.isClosed() );
            } finally {
                release.get( 10, TimeUnit.SECONDS );
            }
        } finally {
            executor.shutdownNow();
        }
    }


    private static List<Long> readKeys( Statement statement ) throws SQLException {
        List<Long> values = new ArrayList<>();
        try ( ResultSet keys = statement.getGeneratedKeys() ) {
            while ( keys.next() ) {
                values.add( keys.getLong( 1 ) );
            }
        }
        return values;
    }

}
