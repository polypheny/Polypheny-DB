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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@Tag("adapter")
class IdentityReplicaInsertVolatilityTest {

    @BeforeAll
    static void start() {
        TestHelper.getInstance();
    }


    @Test
    void volatileKeyPreservesUnionRangeChecksAndSourceOrder() throws SQLException {
        assumeTrue( List.of( "hsqldb", "postgresql" ).contains( PolyphenyDb.defaultStoreName ) );
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            String store = "volatile_key_replica";
            String table = "volatile_keys";
            TestHelper.addHsqldb( store, sql );
            try {
                sql.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, qty BIGINT) ON STORE " + store );
                try {
                    String initialStore = PolyphenyDb.defaultStoreName;
                    sql.executeUpdate( "ALTER TABLE " + table + " ADD PLACEMENT ON STORE " + initialStore );
                    String insert = "INSERT INTO " + table
                            + " VALUES (? + CASE WHEN CURRENT_DATE > DATE '2000-01-01' THEN 0 ELSE 10 END, CAST(? AS DOUBLE)), "
                            + "(?, CAST(? AS DECIMAL(19,0)))";
                    Connection connection = jdbc.getConnection();
                    try ( PreparedStatement keys = connection.prepareStatement( insert, new String[]{ "id", "qty" } );
                            PreparedStatement returning = connection.prepareStatement( insert + " RETURNING id, qty" ) ) {
                        bindRange( keys, 20, Long.MAX_VALUE );
                        bindRange( returning, 20, Long.MAX_VALUE );
                        assertEquals( "22003", assertThrows( SQLException.class, keys::executeUpdate ).getSQLState() );
                        assertEquals( "22003", assertThrows( SQLException.class, returning::executeQuery ).getSQLState() );
                        try ( ResultSet stored = sql.executeQuery( "SELECT id FROM " + table ) ) {
                            assertFalse( stored.next() );
                        }

                        bindRange( keys, 20, 42 );
                        assertEquals( 2, keys.executeUpdate() );
                        try ( ResultSet rows = keys.getGeneratedKeys() ) {
                            assertEquals( List.of( List.of( 22L, 1L ), List.of( 21L, 42L ) ), readPairs( rows ) );
                        }

                        bindRange( returning, 30, 42 );
                        try ( ResultSet rows = returning.executeQuery() ) {
                            assertEquals( List.of( List.of( 32L, 1L ), List.of( 31L, 42L ) ), readPairs( rows ) );
                        }
                        try ( ResultSet stored = sql.executeQuery( "SELECT id, qty FROM " + table + " ORDER BY id" ) ) {
                            assertEquals( List.of( List.of( 21L, 42L ), List.of( 22L, 1L ),
                                    List.of( 31L, 42L ), List.of( 32L, 1L ) ), readPairs( stored ) );
                        }
                    }
                } finally {
                    sql.executeUpdate( "DROP TABLE " + table );
                }
            } finally {
                TestHelper.dropAdapter( store, sql );
            }
        }
    }


    @Test
    void volatileValueDoesNotMaskStableAssignmentInAnotherBranch() throws SQLException {
        assumeTrue( "postgresql".equalsIgnoreCase( PolyphenyDb.defaultStoreName ) );
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            String store = "volatile_value_replica";
            String table = "volatile_values";
            TestHelper.addHsqldb( store, sql );
            try {
                sql.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, qty DECIMAL(5,2)) ON STORE " + store );
                try {
                    sql.executeUpdate( "ALTER TABLE " + table + " ADD PLACEMENT ON STORE postgresql" );
                    String insert = "INSERT INTO " + table + " VALUES (?, CASE WHEN CURRENT_DATE > DATE '2000-01-01' "
                            + "THEN CAST(NULL AS DECIMAL(7,3)) ELSE CAST(2 AS DECIMAL(7,3)) END), (?, CAST(? AS DECIMAL(7,3)))";
                    Connection connection = jdbc.getConnection();
                    try ( PreparedStatement keys = connection.prepareStatement( insert, new String[]{ "id", "qty" } );
                            PreparedStatement returning = connection.prepareStatement( insert + " RETURNING id, qty" ) ) {
                        bindAssignment( keys, "42.495" );
                        bindAssignment( returning, "42.495" );
                        assertEquals( "22000", assertThrows( SQLException.class, keys::executeUpdate ).getSQLState() );
                        assertEquals( "22000", assertThrows( SQLException.class, returning::executeQuery ).getSQLState() );
                        try ( ResultSet stored = sql.executeQuery( "SELECT id FROM " + table ) ) {
                            assertFalse( stored.next() );
                        }

                        bindAssignment( returning, "42.490" );
                        try ( ResultSet rows = returning.executeQuery() ) {
                            assertTrue( rows.next() );
                            assertEquals( 1, rows.getInt( 1 ) );
                            rows.getBigDecimal( 2 );
                            assertTrue( rows.wasNull() );
                            assertTrue( rows.next() );
                            assertEquals( 2, rows.getInt( 1 ) );
                            assertEquals( new BigDecimal( "42.49" ), rows.getBigDecimal( 2 ) );
                            assertFalse( rows.next() );
                        }
                    }
                } finally {
                    sql.executeUpdate( "DROP TABLE " + table );
                }
            } finally {
                TestHelper.dropAdapter( store, sql );
            }
        }
    }


    @Test
    void volatileFilterPreservesNativeUnionRangeAndPrecision() throws SQLException {
        String initialStore = PolyphenyDb.defaultStoreName;
        assumeTrue( List.of( "hsqldb", "postgresql" ).contains( initialStore ) );
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            String store = "volatile_filter_replica";
            String source = "volatile_filter_source";
            String table = "volatile_filter_target";
            TestHelper.addHsqldb( store, sql );
            try {
                sql.executeUpdate( "CREATE TABLE " + source + " (id INTEGER PRIMARY KEY) ON STORE " + initialStore );
                try {
                    sql.executeUpdate( "INSERT INTO " + source + " VALUES (1), (2)" );
                    sql.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, qty BIGINT) ON STORE " + store );
                    try {
                        sql.executeUpdate( "ALTER TABLE " + table + " ADD PLACEMENT ON STORE " + initialStore );
                        String insert = "INSERT INTO " + table + " SELECT id AS id, CAST(? AS DOUBLE) AS qty FROM " + source
                                + " WHERE id=1 AND CURRENT_DATE > DATE '2000-01-01' UNION ALL "
                                + "SELECT id AS id, CAST(? AS DECIMAL(19,0)) AS qty FROM " + source + " WHERE id=2";
                        Connection connection = jdbc.getConnection();
                        try ( PreparedStatement plain = connection.prepareStatement( insert );
                                PreparedStatement keys = connection.prepareStatement( insert, new String[]{ "id", "qty" } );
                                PreparedStatement returning = connection.prepareStatement( insert + " RETURNING id, qty" ) ) {
                            for ( PreparedStatement statement : List.of( plain, keys, returning ) ) {
                                bindFilterRange( statement, Long.MAX_VALUE );
                            }
                            assertEquals( "22003", assertThrows( SQLException.class, plain::executeUpdate ).getSQLState() );
                            assertEquals( "22003", assertThrows( SQLException.class, keys::executeUpdate ).getSQLState() );
                            assertEquals( "22003", assertThrows( SQLException.class, returning::executeQuery ).getSQLState() );
                            try ( ResultSet stored = sql.executeQuery( "SELECT id FROM " + table ) ) {
                                assertFalse( stored.next() );
                            }

                            bindFilterRange( keys, 9007199254740993L );
                            assertEquals( 2, keys.executeUpdate() );
                            List<List<Long>> expected = List.of( List.of( 1L, 1L ), List.of( 2L, 9007199254740992L ) );
                            try ( ResultSet projected = keys.getGeneratedKeys() ) {
                                assertEquals( expected, readPairs( projected ) );
                            }
                            try ( ResultSet stored = sql.executeQuery( "SELECT id, qty FROM " + table + " ORDER BY id" ) ) {
                                assertEquals( expected, readPairs( stored ) );
                            }

                            if ( initialStore.equals( "postgresql" ) ) {
                                sql.executeUpdate( "DELETE FROM " + table );
                                for ( PreparedStatement statement : List.of( keys, returning ) ) {
                                    // Both previews select source id=1. HSQLDB truncates
                                    // this DOUBLE assignment to 42 and PostgreSQL rounds to 43.
                                    statement.setDouble( 1, 42.75 );
                                    statement.setBigDecimal( 2, BigDecimal.valueOf( 42 ) );
                                }
                                assertEquals( "22000", assertThrows( SQLException.class, keys::executeUpdate ).getSQLState() );
                                assertEquals( "22000", assertThrows( SQLException.class, returning::executeQuery ).getSQLState() );
                                try ( ResultSet stored = sql.executeQuery( "SELECT id FROM " + table ) ) {
                                    assertFalse( stored.next() );
                                }
                            }
                        }
                    } finally {
                        sql.executeUpdate( "DROP TABLE " + table );
                    }
                } finally {
                    sql.executeUpdate( "DROP TABLE " + source );
                }
            } finally {
                TestHelper.dropAdapter( store, sql );
            }
        }
    }


    @Test
    void replicaPreviewsMaySelectDifferentRowsForRandomFilters() throws SQLException {
        assumeTrue( "hsqldb".equalsIgnoreCase( PolyphenyDb.defaultStoreName ) );
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            String store = "random_filter_replica";
            String source = "random_filter_source";
            String table = "random_filter_target";
            TestHelper.addHsqldb( store, sql );
            try {
                sql.executeUpdate( "CREATE TABLE " + source + " (id INTEGER PRIMARY KEY) ON STORE hsqldb" );
                try {
                    String values = IntStream.rangeClosed( 1, 24 ).mapToObj( value -> "(" + value + ")" ).collect( Collectors.joining( ", " ) );
                    sql.executeUpdate( "INSERT INTO " + source + " VALUES " + values );
                    sql.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, qty BIGINT) ON STORE hsqldb" );
                    try {
                        sql.executeUpdate( "ALTER TABLE " + table + " ADD PLACEMENT ON STORE " + store );
                        String insert = "INSERT INTO " + table + " SELECT id AS id, CAST(id AS BIGINT) AS qty FROM " + source
                                + " WHERE RAND() > 0.5 UNION ALL SELECT id + 100 AS id, CAST(42 AS BIGINT) AS qty FROM " + source + " WHERE id=1";
                        int count = sql.executeUpdate( insert, new String[]{ "id", "qty" } );
                        List<List<Long>> keys;
                        try ( ResultSet projected = sql.getGeneratedKeys() ) {
                            keys = readPairs( projected );
                        }
                        assertEquals( count, keys.size() );
                        assertRandomFilterRows( sql, table, keys );

                        sql.executeUpdate( "DELETE FROM " + table );
                        List<List<Long>> returning;
                        try ( ResultSet projected = sql.executeQuery( insert + " RETURNING id, qty" ) ) {
                            returning = readPairs( projected );
                        }
                        assertRandomFilterRows( sql, table, returning );
                    } finally {
                        sql.executeUpdate( "DROP TABLE " + table );
                    }
                } finally {
                    sql.executeUpdate( "DROP TABLE " + source );
                }
            } finally {
                TestHelper.dropAdapter( store, sql );
            }
        }
    }


    private static void assertRandomFilterRows( Statement sql, String table, List<List<Long>> rows ) throws SQLException {
        assertTrue( rows.contains( List.of( 101L, 42L ) ) );
        List<List<Long>> sorted = rows.stream().sorted( Comparator.comparingLong( row -> row.get( 0 ) ) ).toList();
        try ( ResultSet stored = sql.executeQuery( "SELECT id, qty FROM " + table + " ORDER BY id" ) ) {
            assertEquals( sorted, readPairs( stored ) );
        }
    }


    private static void bindFilterRange( PreparedStatement statement, long value ) throws SQLException {
        statement.setDouble( 1, 1 );
        statement.setBigDecimal( 2, BigDecimal.valueOf( value ) );
    }


    private static void bindRange( PreparedStatement statement, int offset, long value ) throws SQLException {
        statement.setInt( 1, offset + 2 );
        statement.setDouble( 2, 1 );
        statement.setInt( 3, offset + 1 );
        statement.setBigDecimal( 4, BigDecimal.valueOf( value ) );
    }


    private static void bindAssignment( PreparedStatement statement, String value ) throws SQLException {
        statement.setInt( 1, 1 );
        statement.setInt( 2, 2 );
        statement.setBigDecimal( 3, new BigDecimal( value ) );
    }


    private static List<List<Long>> readPairs( ResultSet rows ) throws SQLException {
        List<List<Long>> values = new ArrayList<>();
        while ( rows.next() ) {
            values.add( List.of( rows.getLong( 1 ), rows.getLong( 2 ) ) );
        }
        return values;
    }

}
