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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@Tag("adapter")
class IdentityTimeHourParsingTest {

    private static final LocalTime STORED_TIME = LocalTime.of( 2, 34, 56, 780_000_000 );


    @BeforeAll
    static void start() {
        TestHelper.getInstance();
        assumeTrue( "hsqldb".equals( PolyphenyDb.defaultStoreName ) );
    }


    @Test
    void selectsFractionalTimeWithASingleDigitHour() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            String table = "time_hour_parsing_read";
            sql.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, clock_value TIME(2))" );
            try {
                assertEquals( 1, sql.executeUpdate( "INSERT INTO " + table + " VALUES (1, TIME '02:34:56.780')" ) );
                assertStoredTime( sql, table, 1 );
            } finally {
                sql.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @Test
    void projectedInsertsKeepNativeEarlyHourPrecision() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            Connection connection = jdbc.getConnection();
            String table = "time_hour_parsing_insert";
            sql.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, clock_value TIME(2))" );
            try {
                String insert = "INSERT INTO " + table + " VALUES ";
                String[] projection = { "clock_value" };
                assertEquals( 1, sql.executeUpdate( insert + "(1, TIME '02:34:56.789')", projection ) );
                assertStoredTimeAfterProjection( sql.getGeneratedKeys(), sql, table, 1 );

                assertStoredTimeAfterProjection( sql.executeQuery(
                        insert + "(2, TIME '02:34:56.789') RETURNING clock_value" ), sql, table, 2 );

                Time input = Time.valueOf( "02:34:56" );
                input.setTime( input.getTime() + 789 );
                try ( PreparedStatement keys = connection.prepareStatement( insert + "(3, ?)", projection ) ) {
                    keys.setTime( 1, input );
                    assertEquals( 1, keys.executeUpdate() );
                    assertStoredTimeAfterProjection( keys.getGeneratedKeys(), sql, table, 3 );
                }

                try ( PreparedStatement returning = connection.prepareStatement( insert + "(4, ?) RETURNING clock_value" ) ) {
                    returning.setTime( 1, input );
                    assertStoredTimeAfterProjection( returning.executeQuery(), sql, table, 4 );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    private static void assertStoredTimeAfterProjection( ResultSet projection, Statement sql, String table, int id ) throws SQLException {
        LocalTime projected;
        try ( projection ) {
            assertTrue( projection.next() );
            projected = timeOfDay( projection );
            assertFalse( projection.next() );
        }
        assertStoredTime( sql, table, id );
        assertEquals( STORED_TIME, projected );
    }


    private static void assertStoredTime( Statement sql, String table, int id ) throws SQLException {
        try ( ResultSet stored = sql.executeQuery( "SELECT clock_value, CAST(clock_value AS VARCHAR(24)) FROM " + table + " WHERE id = " + id ) ) {
            assertTrue( stored.next() );
            String nativeText = stored.getString( 2 ).trim();
            assertTrue( nativeText.endsWith( ".78" ), nativeText );
            assertEquals( STORED_TIME, timeOfDay( stored ) );
            assertFalse( stored.next() );
        }
    }


    private static LocalTime timeOfDay( ResultSet rows ) throws SQLException {
        Calendar utc = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
        long millis = Math.floorMod( rows.getTime( 1, utc ).getTime(), 86_400_000L );
        return LocalTime.ofNanoOfDay( millis * 1_000_000L );
    }

}
