/*
 * Copyright 2019-2026 The Polypheny Project
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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.ArrayList;
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
class IdentityValuesTemporalCastTest {

    private static final String PLAIN = "temporal_cast_plain";
    private static final String IDENTITY = "temporal_cast_identity";
    private static final String COLUMNS = "stamp TIMESTAMP(3), clock_val TIME(3)";
    private static final String SELECT_VALUES = "SELECT stamp, clock_val FROM ";
    private static final String RETURNING = " RETURNING stamp, clock_val";
    private static final String RAW_VALUES = "TIMESTAMP '2024-12-31 23:59:59.995', TIME '12:34:56.789'";
    private static final String RAW_CASTS = "CAST(TIMESTAMP '2024-12-31 23:59:59.995' AS TIMESTAMP(2)), CAST(TIME '12:34:56.789' AS TIME(2))";
    private static final String PREPARED_CASTS = "CAST(? AS TIMESTAMP(2)), CAST(? AS TIME(2))";


    @BeforeAll
    static void start() {
        TestHelper.getInstance();
        assumeTrue( List.of( "hsqldb", "postgresql" ).contains( PolyphenyDb.defaultStoreName ) );
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void rawValuesApplySourcePrecisionBeforeAssignment( boolean parameterize ) throws SQLException {
        withTables( parameterize, (connection, sql, utc) -> {
            assertEquals( 1, sql.executeUpdate( "INSERT INTO " + PLAIN + " VALUES (1, " + RAW_CASTS + ")" ) );
            assertProjectedAndStored( sql, sql.executeQuery( "INSERT INTO " + IDENTITY + "(stamp, clock_val) VALUES (" + RAW_CASTS + ")" + RETURNING ), utc, 1 );

            assertEquals( 2, sql.executeUpdate( "INSERT INTO " + PLAIN + " VALUES (2, " + RAW_CASTS + "), (3, " + RAW_CASTS + ")" ) );
            assertProjectedAndStored( sql, sql.executeQuery( "INSERT INTO " + IDENTITY + "(stamp, clock_val) VALUES (" + RAW_CASTS + "), (" + RAW_CASTS + ")" + RETURNING ), utc, 2 );
        } );
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void preparedValuesKeepDirectAndNestedSourceCasts( boolean parameterize ) throws SQLException {
        withTables( parameterize, (connection, sql, utc) -> {
            String nested = "CAST(CAST(? AS TIMESTAMP(2)) AS TIMESTAMP(3)), CAST(CAST(? AS TIME(2)) AS TIME(3))";
            assertPreparedInsert( connection, sql, utc, "VALUES (1, " + PREPARED_CASTS + ")", "VALUES (" + PREPARED_CASTS + ")" );
            assertPreparedInsert( connection, sql, utc, "VALUES (2, " + PREPARED_CASTS + ")", "VALUES (" + nested + ")" );
        } );
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void insertSelectKeepsExplicitSourceCasts( boolean parameterize ) throws SQLException {
        withTables( parameterize, (connection, sql, utc) -> {
            assertEquals( 1, sql.executeUpdate( "INSERT INTO " + PLAIN + " SELECT 1, " + RAW_CASTS ) );
            assertProjectedAndStored( sql, sql.executeQuery(
                    "INSERT INTO " + IDENTITY + "(stamp, clock_val) SELECT " + RAW_CASTS + RETURNING ), utc, 1 );
            assertPreparedInsert( connection, sql, utc, "SELECT 2, " + PREPARED_CASTS, "SELECT " + PREPARED_CASTS );

            assertEquals( 1, sql.executeUpdate( "INSERT INTO " + PLAIN + " SELECT 3, " + RAW_CASTS ) );
            assertEquals( 1, sql.executeUpdate( "INSERT INTO " + PLAIN + " SELECT 4, " + RAW_VALUES ) );
            String castBranch = "SELECT 2, " + RAW_CASTS;
            String literalBranch = "SELECT 1, " + RAW_VALUES;
            assertProjectedAndStored( sql, sql.executeQuery( "INSERT INTO " + IDENTITY + "(stamp, clock_val) SELECT "
                    + "stamp, clock_val FROM (" + literalBranch + " UNION ALL " + castBranch
                    + ") AS source_rows (source_order, stamp, clock_val) ORDER BY source_order DESC FETCH NEXT 2 ROWS ONLY" + RETURNING ), utc, 2 );

            String from = " FROM " + PLAIN + " WHERE id = 1";
            assertEquals( 1, sql.executeUpdate( "INSERT INTO " + PLAIN + " SELECT 5, " + RAW_CASTS + from ) );
            assertProjectedAndStored( sql, sql.executeQuery(
                    "INSERT INTO " + IDENTITY + "(stamp, clock_val) SELECT " + RAW_CASTS + from + RETURNING ), utc, 1 );
        } );
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void temporalLiteralsUseNativeDestinationPrecision( boolean parameterize ) throws SQLException {
        withTables( parameterize, "stamp TIMESTAMP(2), clock_val TIME(2)", (connection, sql, utc) -> {
            String explicit = "CAST(TIMESTAMP '2024-12-31 23:59:59.995' AS TIMESTAMP(3)), CAST(TIME '12:34:56.789' AS TIME(3))";
            assertEquals( 2, sql.executeUpdate( "INSERT INTO " + PLAIN + " VALUES (1, " + RAW_VALUES + "), (2, " + explicit + ")" ) );
            assertProjectedAndStored( sql, sql.executeQuery( "INSERT INTO " + IDENTITY + "(stamp, clock_val) VALUES ("
                    + RAW_VALUES + "), (" + explicit + ")" + RETURNING ), utc, 2 );
        } );
    }


    @Test
    void parameterizedSelectKeepsTheSourceAdaptersCast() throws SQLException {
        assumeTrue( "postgresql".equals( PolyphenyDb.defaultStoreName ) );
        withTables( true, (connection, sql, utc) -> {
            String store = "temporal_cast_source_store";
            String source = "temporal_cast_source";
            TestHelper.addHsqldb( store, sql );
            try {
                sql.executeUpdate( "CREATE TABLE " + source + " (id INTEGER PRIMARY KEY) ON STORE " + store );
                try {
                    sql.executeUpdate( "INSERT INTO " + source + " VALUES (1)" );
                    String from = " FROM " + source;
                    assertEquals( 1, sql.executeUpdate( "INSERT INTO " + PLAIN + " SELECT id, " + RAW_CASTS + from ) );
                    assertProjectedAndStored( sql, sql.executeQuery(
                            "INSERT INTO " + IDENTITY + "(stamp, clock_val) SELECT " + RAW_CASTS + from + RETURNING ), utc, 1 );
                } finally {
                    sql.executeUpdate( "DROP TABLE " + source );
                }
            } finally {
                TestHelper.dropAdapter( store, sql );
            }
        } );
    }


    private static void assertPreparedInsert( Connection connection, Statement sql, Calendar utc, String plainSource, String identitySource ) throws SQLException {
        try ( PreparedStatement plain = connection.prepareStatement( "INSERT INTO " + PLAIN + " " + plainSource );
                PreparedStatement projected = connection.prepareStatement( "INSERT INTO " + IDENTITY + "(stamp, clock_val) "
                        + identitySource + RETURNING ) ) {
            for ( PreparedStatement insert : List.of( plain, projected ) ) {
                insert.setTimestamp( 1, Timestamp.valueOf( "2024-12-31 23:59:59.995" ), utc );
                insert.setTime( 2, new Time( LocalTime.parse( "12:34:56.789" ).toNanoOfDay() / 1_000_000L ), utc );
            }
            assertEquals( 1, plain.executeUpdate() );
            assertProjectedAndStored( sql, projected.executeQuery(), utc, 1 );
        }
    }


    private static void assertProjectedAndStored( Statement sql, ResultSet projection, Calendar utc, int count ) throws SQLException {
        List<CapturedTime> projected = readTimes( projection, utc );
        List<CapturedTime> ordinary = readTimes( sql.executeQuery( SELECT_VALUES + PLAIN + " ORDER BY id" ), utc );
        List<CapturedTime> stored = readTimes( sql.executeQuery( SELECT_VALUES + IDENTITY + " ORDER BY id" ), utc );
        assertEquals( ordinary, stored );
        assertEquals( count, projected.size() );
        assertEquals( ordinary.subList( ordinary.size() - count, ordinary.size() ), projected );
    }


    private static List<CapturedTime> readTimes( ResultSet rows, Calendar utc ) throws SQLException {
        List<CapturedTime> values = new ArrayList<>();
        try ( rows ) {
            while ( rows.next() ) {
                values.add( new CapturedTime( rows.getTimestamp( 1, utc ), rows.getTime( 2, utc ).getTime() ) );
            }
        }
        return values;
    }


    private static void withTables( boolean parameterize, TableCheck check ) throws SQLException {
        withTables( parameterize, COLUMNS, check );
    }


    private static void withTables( boolean parameterize, String columns, TableCheck check ) throws SQLException {
        boolean previousParameterization = RuntimeConfig.PARAMETERIZE_DML.getBoolean();
        Calendar utc = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE " + PLAIN + " (id INTEGER PRIMARY KEY, " + columns + ")" );
            try {
                sql.executeUpdate( "CREATE TABLE " + IDENTITY + " (id BIGSERIAL PRIMARY KEY, " + columns + ")" );
                try {
                    sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + parameterize );
                    check.run( jdbc.getConnection(), sql, utc );
                } finally {
                    sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + previousParameterization );
                    sql.executeUpdate( "DROP TABLE " + IDENTITY );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE " + PLAIN );
            }
        }
    }


    private record CapturedTime( Timestamp timestamp, long millisOfDay ) {
    }


    @FunctionalInterface
    private interface TableCheck {

        void run( Connection connection, Statement sql, Calendar utc ) throws SQLException;

    }

}
