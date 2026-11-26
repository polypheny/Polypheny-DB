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

@Tag("adapter")
class IdentityTemporalAssignmentTest {

    @BeforeAll
    static void start() {
        TestHelper.getInstance();
    }


    @ParameterizedTest
    @ValueSource(strings = { "temporal_replica", "postgresql" })
    void disagreeingTemporalReplicasStayUnchanged( String placementToDrop ) throws SQLException {
        assumeTrue( "postgresql".equals( PolyphenyDb.defaultStoreName ) );
        String table = "temporal_replica_values";
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            TestHelper.addHsqldb( "temporal_replica", sql );
            try {
                sql.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, stamp TIMESTAMP(2)) ON STORE temporal_replica" );
                try {
                    sql.executeUpdate( "ALTER TABLE " + table + " ADD PLACEMENT ON STORE postgresql" );
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
                    LocalTime.ofNanoOfDay( Math.floorMod( projected.getTime( 1, utc ).getTime(), 86_400_000L ) * 1_000_000L ),
                    LocalTime.ofNanoOfDay( Math.floorMod( projected.getTime( 2, utc ).getTime(), 86_400_000L ) * 1_000_000L ) );
            assertFalse( projected.next() );
            return values;
        }
    }

}
