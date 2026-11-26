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
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.util.temporal.DateTimeUtils;

@Tag("adapter")
class IdentityTemporalAssignmentTest {

    // DefaultInserter uses this name for every configured default store type.
    private static final String DEFAULT_STORE = "hsqldb";


    @BeforeAll
    static void start() {
        TestHelper.getInstance();
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void postgresTimeAtTheEndOfDayRemainsReadable( boolean parameterize ) throws SQLException {
        assumeTrue( "postgresql".equals( PolyphenyDb.defaultStoreName ) );
        boolean previousParameterization = RuntimeConfig.PARAMETERIZE_DML.getBoolean();
        String table = "temporal_end_of_day";
        String input = "TIME '23:59:59.999'";
        Calendar utc = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, stamp TIME(0))" );
            try {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + parameterize );
                String insert = "INSERT INTO " + table + " VALUES ";
                assertEquals( 1, sql.executeUpdate( insert + "(1, " + input + ")" ) );
                assertEquals( 1, sql.executeUpdate( insert + "(2, " + input + ")", new String[]{ "id" } ) );
                try ( ResultSet keys = sql.getGeneratedKeys() ) {
                    assertTrue( keys.next() );
                    assertEquals( 2, keys.getInt( 1 ) );
                    assertFalse( keys.next() );
                }
                try ( ResultSet returning = sql.executeQuery( insert + "(3, " + input + ") RETURNING stamp" ) ) {
                    assertTrue( returning.next() );
                    assertEquals( DateTimeUtils.MILLIS_PER_DAY, returning.getTime( 1, utc ).getTime() );
                    assertFalse( returning.next() );
                }
                try ( ResultSet stored = sql.executeQuery( "SELECT stamp, CAST(stamp AS VARCHAR(24)) FROM " + table + " ORDER BY id" ) ) {
                    for ( int i = 0; i < 3; i++ ) {
                        assertTrue( stored.next() );
                        assertEquals( DateTimeUtils.MILLIS_PER_DAY, stored.getTime( 1, utc ).getTime() );
                        assertEquals( "24:00:00", stored.getString( 2 ) );
                    }
                    assertFalse( stored.next() );
                }
            } finally {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + previousParameterization );
                sql.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void barePostgresTimeKeepsTheNativePrimaryKey( boolean parameterize ) throws SQLException {
        assumeTrue( "postgresql".equals( PolyphenyDb.defaultStoreName ) );
        boolean previousParameterization = RuntimeConfig.PARAMETERIZE_DML.getBoolean();
        String table = "temporal_bare_time_key";
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE " + table + " (bucket INTEGER NOT NULL, stamp TIME NOT NULL, PRIMARY KEY (bucket, stamp))" );
            try {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + parameterize );
                String insert = "INSERT INTO " + table + " VALUES ";
                sql.executeUpdate( insert + "(1, TIME '12:34:57'), (2, TIME '12:34:57'), (3, TIME '12:34:57')" );
                assertEquals( 1, sql.executeUpdate( insert + "(1, TIME '12:34:56.789')" ) );
                assertEquals( 1, sql.executeUpdate( insert + "(2, TIME '12:34:56.789')", new String[]{ "stamp" } ) );
                Calendar utc = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
                long expected = LocalTime.parse( "12:34:56.789" ).toNanoOfDay() / 1_000_000;
                try ( ResultSet keys = sql.getGeneratedKeys() ) {
                    assertTrue( keys.next() );
                    assertEquals( expected, keys.getTime( 1, utc ).getTime() );
                    assertFalse( keys.next() );
                }
                try ( ResultSet returning = sql.executeQuery( insert + "(3, TIME '12:34:56.789') RETURNING stamp" ) ) {
                    assertTrue( returning.next() );
                    assertEquals( expected, returning.getTime( 1, utc ).getTime() );
                    assertFalse( returning.next() );
                }
                try ( ResultSet stored = sql.executeQuery( "SELECT CAST(stamp AS VARCHAR(24)) FROM " + table + " ORDER BY bucket, stamp" ) ) {
                    for ( int i = 0; i < 3; i++ ) {
                        assertTrue( stored.next() );
                        assertEquals( "12:34:56.789", stored.getString( 1 ) );
                        assertTrue( stored.next() );
                        assertEquals( "12:34:57", stored.getString( 1 ) );
                    }
                    assertFalse( stored.next() );
                }
            } finally {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + previousParameterization );
                sql.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(strings = { "temporal_replica", DEFAULT_STORE })
    void disagreeingTemporalReplicasStayUnchanged( String placementToDrop ) throws SQLException {
        assumeTrue( "postgresql".equals( PolyphenyDb.defaultStoreName ) );
        String table = "temporal_replica_values";
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            TestHelper.addHsqldb( "temporal_replica", sql );
            try {
                sql.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, stamp TIMESTAMP(2)) ON STORE temporal_replica" );
                try {
                    sql.executeUpdate( "ALTER TABLE " + table + " ADD PLACEMENT ON STORE " + DEFAULT_STORE );
                    String insert = "INSERT INTO " + table + " VALUES (1, TIMESTAMP '2024-12-31 23:59:59.995')";
                    SQLException disagreement = assertThrows( SQLException.class, () -> sql.executeUpdate( insert, new String[]{ "stamp" } ) );
                    assertEquals( "22000", disagreement.getSQLState() );

                    // Query the remaining placement directly so either replica's partial write would be observable.
                    sql.executeUpdate( "ALTER TABLE " + table + " DROP PLACEMENT ON STORE " + placementToDrop );
                    try ( ResultSet rows = sql.executeQuery( "SELECT id FROM " + table ) ) {
                        assertFalse( rows.next() );
                    }
                } finally {
                    sql.executeUpdate( "DROP TABLE " + table );
                }
            } finally {
                TestHelper.dropAdapter( "temporal_replica", sql );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void canonicalDuplicateWithinOneInsertFailsBeforeManualCommit( boolean parameterize ) throws SQLException {
        requireJdbcStore();
        boolean previousParameterization = RuntimeConfig.PARAMETERIZE_DML.getBoolean();
        boolean previousUniqueness = RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean();
        String table = "temporal_duplicate_values";
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement();
                JdbcConnection manual = new JdbcConnection( false ); Statement writer = manual.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE " + table + " (bucket INTEGER NOT NULL, stamp TIMESTAMP(2) NOT NULL, "
                    + "amount INTEGER NOT NULL, PRIMARY KEY (bucket, stamp))" );
            try {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + parameterize );
                sql.executeUpdate( "ALTER CONFIG 'runtime/uniqueConstraintEnforcement' SET false" );
                String insert = "INSERT INTO " + table + " VALUES ";
                assertEquals( 1, writer.executeUpdate( insert + "(1, TIMESTAMP '2024-12-31 23:59:59.990', 10)" ) );
                manual.getConnection().commit();
                SQLException duplicate = assertThrows( SQLException.class, () -> writer.executeUpdate(
                        insert + "(2, TIMESTAMP '2024-12-31 23:59:59.990', 11), (2, TIMESTAMP '2024-12-31 23:59:59.991', 22)",
                        new String[]{ "stamp" } ) );

                // A failed projected write must not leave rows that a later COMMIT can persist.
                manual.getConnection().commit();
                try ( ResultSet stored = sql.executeQuery( "SELECT bucket, amount FROM " + table + " ORDER BY amount" ) ) {
                    assertTrue( stored.next() );
                    assertEquals( 1, stored.getInt( 1 ) );
                    assertEquals( 10, stored.getInt( 2 ) );
                    assertFalse( stored.next() );
                }
                assertEquals( "23505", duplicate.getSQLState() );
            } finally {
                manual.getConnection().rollback();
                sql.executeUpdate( "ALTER CONFIG 'runtime/uniqueConstraintEnforcement' SET " + previousUniqueness );
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + previousParameterization );
                sql.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @Test
    void decimalOnlyProjectionUsesTheInsertedBareTimestampKey() throws SQLException {
        requireJdbcStore();
        boolean previousParameterization = RuntimeConfig.PARAMETERIZE_DML.getBoolean();
        String table = "temporal_bare_key_values";
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE " + table + " (bucket INTEGER NOT NULL, stamp TIMESTAMP NOT NULL, "
                    + "amount DECIMAL(12,4) NOT NULL, PRIMARY KEY (bucket, stamp))" );
            try {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET false" );
                String insert = "INSERT INTO " + table + " VALUES ";
                assertEquals( 1, sql.executeUpdate( insert + "(1, TIMESTAMP '2025-01-01 00:00:00', 999)" ) );
                try ( PreparedStatement keys = jdbc.getConnection().prepareStatement( insert + "(1, ?, ?)", new String[]{ "amount" } ) ) {
                    keys.setTimestamp( 1, Timestamp.valueOf( "2024-12-31 23:59:59.995" ) );
                    keys.setBigDecimal( 2, new BigDecimal( "1.234567" ) );
                    assertEquals( 1, keys.executeUpdate() );
                    try ( ResultSet projected = keys.getGeneratedKeys() ) {
                        assertTrue( projected.next() );
                        assertEquals( new BigDecimal( "1.2346" ), projected.getBigDecimal( 1 ) );
                        assertFalse( projected.next() );
                    }
                }
                try ( ResultSet stored = sql.executeQuery( "SELECT amount FROM " + table + " ORDER BY amount" ) ) {
                    assertTrue( stored.next() );
                    assertEquals( new BigDecimal( "1.2346" ), stored.getBigDecimal( 1 ) );
                    assertTrue( stored.next() );
                    assertEquals( new BigDecimal( "999.0000" ), stored.getBigDecimal( 1 ) );
                    assertFalse( stored.next() );
                }
            } finally {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + previousParameterization );
                sql.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void timeProjectionsReportTheNativeStoredPrecision( boolean parameterize ) throws SQLException {
        requireJdbcStore();
        boolean previousParameterization = RuntimeConfig.PARAMETERIZE_DML.getBoolean();
        String table = "temporal_time_values";
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, whole_second TIME(0), hundredths TIME(2))" );
            try {
                String insert = "INSERT INTO " + table + " VALUES ";
                String values = ", TIME '12:34:56.789', TIME '12:34:56.789')";
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET false" );
                assertEquals( 1, sql.executeUpdate( insert + "(1" + values ) );
                List<LocalTime> expected = readNativeTimes( sql, table, 1 );
                assertEquals( 0, expected.get( 0 ).getNano() );
                assertTrue( expected.get( 1 ).getNano() > 0 );

                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + parameterize );
                assertEquals( 1, sql.executeUpdate( insert + "(2" + values, new String[]{ "whole_second", "hundredths" } ) );
                assertEquals( expected, readProjectedTimes( sql.getGeneratedKeys() ) );
                assertEquals( expected, readNativeTimes( sql, table, 2 ) );

                assertEquals( expected, readProjectedTimes( sql.executeQuery(
                        insert + "(3" + values + " RETURNING whole_second, hundredths" ) ) );
                assertEquals( expected, readNativeTimes( sql, table, 3 ) );
            } finally {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + previousParameterization );
                sql.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void projectedInsertPreservesExplicitTemporalSourcePrecision( boolean parameterize ) throws SQLException {
        requireJdbcStore();
        boolean previousParameterization = RuntimeConfig.PARAMETERIZE_DML.getBoolean();
        String table = "temporal_source_casts";
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, stamp TIMESTAMP(3), clock_val TIME(3))" );
            try {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + parameterize );
                String insert = "INSERT INTO " + table + " VALUES (";
                String literals = ", CAST(TIMESTAMP '2024-12-31 23:59:59.995' AS TIMESTAMP(2)), CAST(TIME '12:34:56.789' AS TIME(2)))";
                assertEquals( 1, sql.executeUpdate( insert + "1" + literals ) );
                TemporalValues expected = readStoredTemporalValues( sql, table, 1 );
                assertEquals( 0, expected.stamp().getNanos() % 10_000_000 );
                assertEquals( 0, expected.time().getNano() % 10_000_000 );
                assertEquals( 1, sql.executeUpdate( insert + "2" + literals, new String[]{ "stamp", "clock_val" } ) );
                assertEquals( expected, readProjectedTemporalValues( sql.getGeneratedKeys() ) );
                assertEquals( expected, readStoredTemporalValues( sql, table, 2 ) );

                String bindings = ", CAST(? AS TIMESTAMP(2)), CAST(? AS TIME(2)))";
                try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( insert + "3" + bindings );
                        PreparedStatement returning = jdbc.getConnection().prepareStatement( insert + "4" + bindings + " RETURNING stamp, clock_val" ) ) {
                    bindTemporalSources( plain );
                    bindTemporalSources( returning );
                    assertEquals( 1, plain.executeUpdate() );
                    TemporalValues preparedExpected = readStoredTemporalValues( sql, table, 3 );
                    assertEquals( preparedExpected, readProjectedTemporalValues( returning.executeQuery() ) );
                    assertEquals( preparedExpected, readStoredTemporalValues( sql, table, 4 ) );
                }
            } finally {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + previousParameterization );
                sql.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void temporalSourceCastsApplyOnEveryReplica( boolean parameterize ) throws SQLException {
        assumeTrue( "postgresql".equals( PolyphenyDb.defaultStoreName ) );
        boolean previousParameterization = RuntimeConfig.PARAMETERIZE_DML.getBoolean();
        String table = "temporal_replica_source_casts";
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            TestHelper.addHsqldb( "temporal_source_replica", sql );
            try {
                sql.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, stamp TIMESTAMP(3), clock_val TIME(3)) ON STORE temporal_source_replica" );
                try {
                    sql.executeUpdate( "ALTER TABLE " + table + " ADD PLACEMENT ON STORE " + DEFAULT_STORE );
                    sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + parameterize );
                    String insert = "INSERT INTO " + table + " VALUES (";
                    String returning = " RETURNING stamp, clock_val";
                    String casts = "CAST(TIMESTAMP '2024-12-31 23:59:59.995' AS TIMESTAMP(2)), CAST(TIME '12:34:56.789' AS TIME(2))";
                    String raw = insert + "1, " + casts + ")" + returning;
                    if ( parameterize ) {
                        assertEquals( "22000", assertThrows( SQLException.class, () -> sql.executeQuery( raw ) ).getSQLState() );
                        String multiple = insert + "3, " + casts + "), (4, " + casts + ")" + returning;
                        assertEquals( "22000", assertThrows( SQLException.class, () -> sql.executeQuery( multiple ) ).getSQLState() );
                    } else {
                        TemporalValues expected = new TemporalValues( Timestamp.valueOf( "2024-12-31 23:59:59.990" ), LocalTime.parse( "12:34:56.780" ) );
                        assertEquals( expected, readProjectedTemporalValues( sql.executeQuery( raw ) ) );
                    }
                    try ( PreparedStatement write = jdbc.getConnection().prepareStatement(
                            insert + "2, CAST(? AS TIMESTAMP(2)), CAST(? AS TIME(2)))" + returning ) ) {
                        bindTemporalSources( write );
                        assertEquals( "22000", assertThrows( SQLException.class, write::executeQuery ).getSQLState() );
                    }
                    try ( ResultSet rows = sql.executeQuery( "SELECT id FROM " + table ) ) {
                        assertEquals( !parameterize, rows.next() );
                        if ( !parameterize ) {
                            assertEquals( 1, rows.getInt( 1 ) );
                            assertFalse( rows.next() );
                        }
                    }
                } finally {
                    sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + previousParameterization );
                    sql.executeUpdate( "DROP TABLE " + table );
                }
            } finally {
                TestHelper.dropAdapter( "temporal_source_replica", sql );
            }
        }
    }


    private static void bindTemporalSources( PreparedStatement insert ) throws SQLException {
        Calendar utc = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
        insert.setTimestamp( 1, Timestamp.valueOf( "2024-12-31 23:59:59.995" ), utc );
        insert.setTime( 2, new Time( LocalTime.parse( "12:34:56.789" ).toNanoOfDay() / 1_000_000L ), utc );
    }


    private static TemporalValues readStoredTemporalValues( Statement sql, String table, int id ) throws SQLException {
        try ( ResultSet stored = sql.executeQuery( "SELECT stamp, CAST(clock_val AS VARCHAR(24)) FROM " + table + " WHERE id = " + id ) ) {
            assertTrue( stored.next() );
            TemporalValues values = new TemporalValues( stored.getTimestamp( 1 ), LocalTime.parse( stored.getString( 2 ).trim() ) );
            assertFalse( stored.next() );
            return values;
        }
    }


    private static TemporalValues readProjectedTemporalValues( ResultSet projected ) throws SQLException {
        try ( projected ) {
            assertTrue( projected.next() );
            Calendar utc = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
            TemporalValues values = new TemporalValues( projected.getTimestamp( 1 ), fromSqlTime( projected.getTime( 2, utc ) ) );
            assertFalse( projected.next() );
            return values;
        }
    }


    private static void requireJdbcStore() {
        assumeTrue( List.of( "hsqldb", "postgresql" ).contains( PolyphenyDb.defaultStoreName ) );
    }


    private static List<LocalTime> readNativeTimes( Statement sql, String table, int id ) throws SQLException {
        // Read the native text to retain fractional seconds that a java.sql.Time string omits.
        try ( ResultSet stored = sql.executeQuery( "SELECT CAST(whole_second AS VARCHAR(24)), CAST(hundredths AS VARCHAR(24)) FROM "
                + table + " WHERE id = " + id ) ) {
            assertTrue( stored.next() );
            List<LocalTime> times = List.of( LocalTime.parse( stored.getString( 1 ).trim() ), LocalTime.parse( stored.getString( 2 ).trim() ) );
            assertFalse( stored.next() );
            return times;
        }
    }


    private static List<LocalTime> readProjectedTimes( ResultSet projected ) throws SQLException {
        try ( projected ) {
            assertTrue( projected.next() );
            Calendar utc = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
            List<LocalTime> values = List.of(
                    fromSqlTime( projected.getTime( 1, utc ) ),
                    fromSqlTime( projected.getTime( 2, utc ) ) );
            assertFalse( projected.next() );
            return values;
        }
    }


    private static LocalTime fromSqlTime( Time time ) {
        return LocalTime.ofNanoOfDay( Math.floorMod( time.getTime(), 86_400_000L ) * 1_000_000L );
    }


    private record TemporalValues( Timestamp stamp, LocalTime time ) {
    }

}
