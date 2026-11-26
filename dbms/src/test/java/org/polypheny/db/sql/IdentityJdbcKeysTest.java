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

import java.math.BigDecimal;
import java.math.RoundingMode;
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
    void preparedProjectionsReturnStoredDecimalAssignments( boolean identity ) throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE projected_decimal (id " + (identity ? "BIGSERIAL" : "BIGINT")
                    + " PRIMARY KEY, amount DECIMAL(12,4))" );
            try {
                String insert = "INSERT INTO projected_decimal (id, amount) VALUES (?, ?), (?, ?)";
                BigDecimal first = new BigDecimal( "1.234567" );
                BigDecimal second = new BigDecimal( "-1.23455" );
                try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( insert );
                        PreparedStatement keys = jdbc.getConnection().prepareStatement( insert, new String[]{ "amount" } );
                        PreparedStatement returning = jdbc.getConnection().prepareStatement( insert + " RETURNING amount" ) ) {
                    bindDecimalRows( plain, 10, first, second );
                    assertEquals( 2, plain.executeUpdate() );
                    List<BigDecimal> stored;
                    try ( ResultSet rows = sql.executeQuery( "SELECT amount FROM projected_decimal WHERE id < 20 ORDER BY id DESC" ) ) {
                        stored = readDecimals( rows );
                    }
                    assertEquals( new BigDecimal( "1.2346" ), stored.get( 0 ) );
                    assertEquals( 4, stored.get( 1 ).scale() );

                    bindDecimalRows( keys, 20, first, second );
                    assertEquals( 2, keys.executeUpdate() );
                    try ( ResultSet rows = keys.getGeneratedKeys() ) {
                        assertEquals( stored, readDecimals( rows ) );
                    }
                    bindDecimalRows( returning, 30, first, second );
                    try ( ResultSet rows = returning.executeQuery() ) {
                        assertEquals( stored, readDecimals( rows ) );
                    }
                    try ( ResultSet rows = sql.executeQuery( "SELECT amount FROM projected_decimal WHERE id >= 20 ORDER BY id" ) ) {
                        assertEquals( List.of( stored.get( 1 ), stored.get( 0 ), stored.get( 1 ), stored.get( 0 ) ), readDecimals( rows ) );
                    }
                }
            } finally {
                sql.executeUpdate( "DROP TABLE projected_decimal" );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void preparedProjectionsReturnStoredIntegralAssignments( boolean floatingAndScaledInputs ) throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE projected_integral (id INTEGER PRIMARY KEY, small_num SMALLINT, ordinary_num INTEGER, wide_num BIGINT)" );
            try {
                String insert = "INSERT INTO projected_integral (id, small_num, ordinary_num, wide_num) VALUES (?, ?, ?, ?)";
                try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( insert );
                        PreparedStatement keys = jdbc.getConnection().prepareStatement( insert, new String[]{ "small_num", "ordinary_num", "wide_num" } );
                        PreparedStatement returning = jdbc.getConnection().prepareStatement( insert + " RETURNING small_num, ordinary_num, wide_num" ) ) {
                    bindIntegralProjection( plain, 1, floatingAndScaledInputs );
                    assertEquals( 1, plain.executeUpdate() );
                    List<Number> expected;
                    try ( ResultSet rows = sql.executeQuery( "SELECT small_num, ordinary_num, wide_num FROM projected_integral WHERE id = 1" ) ) {
                        expected = readIntegralProjection( rows );
                    }
                    assertEquals( 9_007_199_254_740_993L, expected.get( 2 ) );

                    bindIntegralProjection( keys, 2, floatingAndScaledInputs );
                    assertEquals( 1, keys.executeUpdate() );
                    try ( ResultSet rows = keys.getGeneratedKeys() ) {
                        assertEquals( expected, readIntegralProjection( rows ) );
                    }
                    bindIntegralProjection( returning, 3, floatingAndScaledInputs );
                    try ( ResultSet rows = returning.executeQuery() ) {
                        assertEquals( expected, readIntegralProjection( rows ) );
                    }
                    for ( int id : new int[]{ 2, 3 } ) {
                        try ( ResultSet rows = sql.executeQuery( "SELECT small_num, ordinary_num, wide_num FROM projected_integral WHERE id = " + id ) ) {
                            assertEquals( expected, readIntegralProjection( rows ) );
                        }
                    }
                }
            } finally {
                sql.executeUpdate( "DROP TABLE projected_integral" );
            }
        }
    }


    @Test
    void preparedProjectionsReturnTheStoredValueAndTypeForZeroScaleDecimalAssignments() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE projected_zero_scale (id INTEGER PRIMARY KEY, amount INTEGER)" );
            try {
                String insert = "INSERT INTO projected_zero_scale VALUES (?, ?), (?, ?)";
                try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( insert );
                        PreparedStatement keys = jdbc.getConnection().prepareStatement( insert, new String[]{ "amount" } );
                        PreparedStatement returning = jdbc.getConnection().prepareStatement( insert + " RETURNING amount" ) ) {
                    bindZeroScaleProjection( plain, 1 );
                    assertEquals( 2, plain.executeUpdate() );
                    List<Object> expected = new ArrayList<>();
                    try ( ResultSet stored = sql.executeQuery( "SELECT amount FROM projected_zero_scale ORDER BY id" ) ) {
                        while ( stored.next() ) {
                            expected.add( stored.getObject( 1 ) );
                        }
                    }
                    assertEquals( 2, expected.size() );

                    bindZeroScaleProjection( keys, 3 );
                    assertEquals( 2, keys.executeUpdate() );
                    try ( ResultSet rows = keys.getGeneratedKeys() ) {
                        assertIntegralProjectionMatchesStored( rows, expected );
                    }

                    bindZeroScaleProjection( returning, 5 );
                    try ( ResultSet rows = returning.executeQuery() ) {
                        assertIntegralProjectionMatchesStored( rows, expected );
                    }
                }
            } finally {
                sql.executeUpdate( "DROP TABLE projected_zero_scale" );
            }
        }
    }


    private static void bindZeroScaleProjection( PreparedStatement insert, int firstId ) throws SQLException {
        insert.setInt( 1, firstId );
        insert.setBigDecimal( 2, new BigDecimal( "42" ) );
        insert.setInt( 3, firstId + 1 );
        insert.setBigDecimal( 4, new BigDecimal( "2147483648" ) );
    }


    private static void assertIntegralProjectionMatchesStored( ResultSet rows, List<Object> expected ) throws SQLException {
        for ( Object stored : expected ) {
            assertTrue( rows.next() );
            assertEquals( stored, rows.getObject( 1 ) );
            assertEquals( stored, rows.getObject( 1, Integer.class ) );
        }
        assertFalse( rows.next() );
    }


    @ParameterizedTest
    @ValueSource(longs = { 2147483648L, -2147483649L })
    void rejectsAProjectedInsertWithAnOutOfRangeLongAssignment( long overflow ) throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE projected_overflow (id INTEGER PRIMARY KEY, qty INTEGER)" );
            try {
                String insert = "INSERT INTO projected_overflow VALUES (1, 42), (2, ?)";
                try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( insert );
                        PreparedStatement keys = jdbc.getConnection().prepareStatement( insert, new String[]{ "id" } );
                        PreparedStatement returning = jdbc.getConnection().prepareStatement( insert + " RETURNING qty" ) ) {
                    plain.setLong( 1, overflow );
                    assertThrows( SQLException.class, plain::executeUpdate );
                    keys.setLong( 1, overflow );
                    assertEquals( "22003", assertThrows( SQLException.class, keys::executeUpdate ).getSQLState() );
                    returning.setLong( 1, overflow );
                    assertEquals( "22003", assertThrows( SQLException.class, returning::executeQuery ).getSQLState() );
                }
                try ( ResultSet stored = sql.executeQuery( "SELECT id FROM projected_overflow" ) ) {
                    assertFalse( stored.next() );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE projected_overflow" );
            }
        }
    }


    @Test
    void preparedProjectionsRespectDeclaredIntegerWidthsAndExactDecimalValues() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE projected_numeric_types (id INTEGER PRIMARY KEY, narrow_num INTEGER, small_num SMALLINT, "
                    + "wide_num BIGINT, scaled_num DECIMAL(12,4), exact_num DECIMAL(20,0))" );
            try {
                String insert = "INSERT INTO projected_numeric_types VALUES (?, ?, ?, ?, ?, ?)";
                String fields = "narrow_num, small_num, wide_num, scaled_num, exact_num";
                List<Object> expected = List.of( Integer.MIN_VALUE, 42, 42L, new BigDecimal( "42.0000" ), new BigDecimal( "9007199254740993" ) );
                try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( insert );
                        PreparedStatement keys = jdbc.getConnection().prepareStatement( insert, fields.split( ", " ) );
                        PreparedStatement returning = jdbc.getConnection().prepareStatement( insert + " RETURNING " + fields ) ) {
                    bindNumericWidths( plain, 1 );
                    assertEquals( 1, plain.executeUpdate() );
                    bindNumericWidths( keys, 2 );
                    assertEquals( 1, keys.executeUpdate() );
                    try ( ResultSet rows = keys.getGeneratedKeys() ) {
                        assertNumericWidths( rows, expected );
                    }
                    bindNumericWidths( returning, 3 );
                    try ( ResultSet rows = returning.executeQuery() ) {
                        assertNumericWidths( rows, expected );
                    }
                }
                for ( int id = 1; id <= 3; id++ ) {
                    try ( ResultSet stored = sql.executeQuery( "SELECT " + fields + " FROM projected_numeric_types WHERE id = " + id ) ) {
                        assertNumericWidths( stored, expected );
                    }
                }
            } finally {
                sql.executeUpdate( "DROP TABLE projected_numeric_types" );
            }
        }
    }


    private static void bindNumericWidths( PreparedStatement insert, int id ) throws SQLException {
        insert.setInt( 1, id );
        insert.setLong( 2, Integer.MIN_VALUE );
        insert.setLong( 3, 42L );
        insert.setInt( 4, 42 );
        insert.setInt( 5, 42 );
        insert.setLong( 6, 9_007_199_254_740_993L );
    }


    private static void assertNumericWidths( ResultSet rows, List<Object> expected ) throws SQLException {
        assertTrue( rows.next() );
        for ( int i = 0; i < expected.size(); i++ ) {
            Object value = expected.get( i );
            assertEquals( value, rows.getObject( i + 1 ) );
            assertEquals( value, rows.getObject( i + 1, value.getClass() ) );
        }
        assertFalse( rows.next() );
    }


    @Test
    void projectedBigintAssignmentsUseTheStoresCastBeforeRouting() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE projected_bigint_cast_control (id INTEGER PRIMARY KEY, qty BIGINT)" );
            try {
                sql.executeUpdate( "CREATE TABLE projected_bigint_cast_keys (id INTEGER NOT NULL, qty BIGINT NOT NULL, PRIMARY KEY (id, qty)) "
                        + "PARTITION BY HASH (qty) PARTITIONS 3" );
                try {
                    String insert = "INSERT INTO projected_bigint_cast_keys VALUES (?, ?)";
                    try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( "INSERT INTO projected_bigint_cast_control VALUES (?, ?)" );
                            PreparedStatement keys = jdbc.getConnection().prepareStatement( insert, new String[]{ "qty" } );
                            PreparedStatement returning = jdbc.getConnection().prepareStatement( insert + " RETURNING qty" ) ) {
                        for ( int sign : new int[]{ 1, -1 } ) {
                            BigDecimal value = BigDecimal.valueOf( sign * 42.75 );
                            plain.setInt( 1, sign );
                            plain.setBigDecimal( 2, value );
                            assertEquals( 1, plain.executeUpdate() );
                            long expected;
                            try ( ResultSet stored = sql.executeQuery( "SELECT qty FROM projected_bigint_cast_control WHERE id = " + sign ) ) {
                                assertTrue( stored.next() );
                                expected = stored.getLong( 1 );
                            }
                            keys.setInt( 1, sign );
                            keys.setBigDecimal( 2, value );
                            assertEquals( 1, keys.executeUpdate() );
                            try ( ResultSet projected = keys.getGeneratedKeys() ) {
                                assertNumericWidths( projected, List.of( expected ) );
                            }
                            returning.setInt( 1, sign * 2 );
                            returning.setDouble( 2, value.doubleValue() );
                            try ( ResultSet projected = returning.executeQuery() ) {
                                assertNumericWidths( projected, List.of( expected ) );
                            }
                            try ( ResultSet stored = sql.executeQuery( "SELECT count(*) FROM projected_bigint_cast_keys WHERE qty = " + expected ) ) {
                                assertTrue( stored.next() );
                                assertEquals( 2, stored.getInt( 1 ) );
                            }
                        }
                    }
                } finally {
                    sql.executeUpdate( "DROP TABLE projected_bigint_cast_keys" );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE projected_bigint_cast_control" );
            }
        }
    }


    @Test
    void rejectsOutOfRangeBigintBeforeWritingAnyProjectedRow() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE projected_bigint_range (id INTEGER PRIMARY KEY, qty BIGINT)" );
            try {
                String insert = "INSERT INTO projected_bigint_range VALUES (1, 42), (2, ?)";
                try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( insert );
                        PreparedStatement keys = jdbc.getConnection().prepareStatement( insert, new String[]{ "id" } );
                        PreparedStatement returning = jdbc.getConnection().prepareStatement( insert + " RETURNING qty" ) ) {
                    plain.setBigDecimal( 1, new BigDecimal( "9223372036854775808" ) );
                    assertThrows( SQLException.class, plain::executeUpdate );
                    keys.setBigDecimal( 1, new BigDecimal( "-9223372036854775809" ) );
                    assertEquals( "22003", assertThrows( SQLException.class, keys::executeUpdate ).getSQLState() );
                    returning.setDouble( 1, 0x1.0p63 );
                    assertEquals( "22003", assertThrows( SQLException.class, returning::executeQuery ).getSQLState() );
                    returning.setDouble( 1, Double.NaN );
                    assertEquals( "22003", assertThrows( SQLException.class, returning::executeQuery ).getSQLState() );
                }
                try ( ResultSet stored = sql.executeQuery( "SELECT id FROM projected_bigint_range" ) ) {
                    assertFalse( stored.next() );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE projected_bigint_range" );
            }
        }
    }


    @Test
    void floatingIntegerAssignmentsMatchOrdinaryParameterConversion() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE projected_integer_cast (id INTEGER PRIMARY KEY, qty INTEGER)" );
            try {
                String insert = "INSERT INTO projected_integer_cast VALUES (?, ?)";
                try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( insert );
                        PreparedStatement keys = jdbc.getConnection().prepareStatement( insert, new String[]{ "qty" } );
                        PreparedStatement returning = jdbc.getConnection().prepareStatement( insert + " RETURNING qty" ) ) {
                    List<Double> values = List.of( 2147483648d, -2147483649d, Double.NaN );
                    for ( int i = 0; i < values.size(); i++ ) {
                        int id = i * 3;
                        double value = values.get( i );
                        plain.setInt( 1, id );
                        plain.setDouble( 2, value );
                        assertEquals( 1, plain.executeUpdate() );
                        int expected;
                        try ( ResultSet stored = sql.executeQuery( "SELECT qty FROM projected_integer_cast WHERE id = " + id ) ) {
                            assertTrue( stored.next() );
                            expected = stored.getInt( 1 );
                        }
                        keys.setInt( 1, id + 1 );
                        keys.setDouble( 2, value );
                        assertEquals( 1, keys.executeUpdate() );
                        try ( ResultSet projected = keys.getGeneratedKeys() ) {
                            assertNumericWidths( projected, List.of( expected ) );
                        }
                        returning.setInt( 1, id + 2 );
                        returning.setDouble( 2, value );
                        try ( ResultSet projected = returning.executeQuery() ) {
                            assertNumericWidths( projected, List.of( expected ) );
                        }
                    }
                }
            } finally {
                sql.executeUpdate( "DROP TABLE projected_integer_cast" );
            }
        }
    }


    @Test
    void rawComputedNumericAssignmentsUseNativeCastsBeforeProjectingKeys() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE native_projected_expr (id INTEGER PRIMARY KEY, narrow_num SMALLINT, ordinary_num INTEGER)" );
            try {
                for ( boolean parameterize : new boolean[]{ true, false } ) {
                    sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + parameterize );
                    String prefix = "INSERT INTO native_projected_expr VALUES ";
                    String expressions = ", CAST(42.75 AS DOUBLE), CAST(42.75 AS DECIMAL(12,4)))";
                    assertEquals( 1, sql.executeUpdate( prefix + "(1" + expressions ) );
                    List<Object> expected;
                    try ( ResultSet stored = sql.executeQuery( "SELECT narrow_num, ordinary_num FROM native_projected_expr WHERE id = 1" ) ) {
                        assertTrue( stored.next() );
                        expected = List.of( stored.getObject( 1 ), stored.getObject( 2 ) );
                    }
                    assertEquals( 1, sql.executeUpdate( prefix + "(2" + expressions, new String[]{ "narrow_num", "ordinary_num" } ) );
                    try ( ResultSet keys = sql.getGeneratedKeys() ) {
                        assertNumericWidths( keys, expected );
                    }
                    try ( ResultSet returning = sql.executeQuery( prefix + "(3" + expressions + " RETURNING narrow_num, ordinary_num" ) ) {
                        assertNumericWidths( returning, expected );
                    }
                    String overflow = prefix + "(4, 1, CAST(2147483648.75 AS DOUBLE))";
                    assertThrows( SQLException.class, () -> sql.executeUpdate( overflow ) );
                    assertEquals( "22003", assertThrows( SQLException.class, () -> sql.executeUpdate( overflow, new String[]{ "id" } ) ).getSQLState() );
                    assertEquals( "22003", assertThrows( SQLException.class, () -> sql.executeQuery( overflow + " RETURNING ordinary_num" ) ).getSQLState() );
                    try ( ResultSet stored = sql.executeQuery( "SELECT id FROM native_projected_expr WHERE id = 4" ) ) {
                        assertFalse( stored.next() );
                    }
                    sql.executeUpdate( "DELETE FROM native_projected_expr" );
                }
            } finally {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET true" );
                sql.executeUpdate( "DROP TABLE native_projected_expr" );
            }
        }
    }


    @Test
    void foldedDoubleAssignmentsKeepOrdinaryLiteralRounding() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE folded_projected_rounding (id INTEGER PRIMARY KEY, narrow INTEGER, wide BIGINT)" );
            try {
                String prefix = "INSERT INTO folded_projected_rounding VALUES ";
                String folded = ", CAST(42.5 AS DOUBLE), CAST(-42.5 AS DOUBLE))";
                String fields = "narrow, wide";
                for ( boolean parameterize : new boolean[]{ true, false } ) {
                    sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + parameterize );
                    for ( boolean boundId : new boolean[]{ false, true } ) {
                        List<Object> expected;
                        if ( boundId ) {
                            try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( prefix + "(?" + folded );
                                    PreparedStatement keys = jdbc.getConnection().prepareStatement( prefix + "(?" + folded, new String[]{ "narrow", "wide" } );
                                    PreparedStatement returning = jdbc.getConnection().prepareStatement( prefix + "(?" + folded + " RETURNING " + fields ) ) {
                                plain.setInt( 1, 1 );
                                assertEquals( 1, plain.executeUpdate() );
                                expected = readFoldedProjection( sql, fields );
                                keys.setInt( 1, 2 );
                                assertEquals( 1, keys.executeUpdate() );
                                try ( ResultSet projected = keys.getGeneratedKeys() ) {
                                    assertNumericWidths( projected, expected );
                                }
                                returning.setInt( 1, 3 );
                                try ( ResultSet projected = returning.executeQuery() ) {
                                    assertNumericWidths( projected, expected );
                                }
                            }
                        } else {
                            assertEquals( 1, sql.executeUpdate( prefix + "(1" + folded ) );
                            expected = readFoldedProjection( sql, fields );
                            assertEquals( 1, sql.executeUpdate( prefix + "(2" + folded, new String[]{ "narrow", "wide" } ) );
                            try ( ResultSet projected = sql.getGeneratedKeys() ) {
                                assertNumericWidths( projected, expected );
                            }
                            try ( ResultSet projected = sql.executeQuery( prefix + "(3" + folded + " RETURNING " + fields ) ) {
                                assertNumericWidths( projected, expected );
                            }
                        }
                        for ( int id : new int[]{ 2, 3 } ) {
                            try ( ResultSet stored = sql.executeQuery( "SELECT " + fields + " FROM folded_projected_rounding WHERE id = " + id ) ) {
                                assertNumericWidths( stored, expected );
                            }
                        }
                        sql.executeUpdate( "DELETE FROM folded_projected_rounding" );
                    }
                }
            } finally {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET true" );
                sql.executeUpdate( "DROP TABLE folded_projected_rounding" );
            }
        }
    }


    private static List<Object> readFoldedProjection( Statement sql, String fields ) throws SQLException {
        try ( ResultSet stored = sql.executeQuery( "SELECT " + fields + " FROM folded_projected_rounding WHERE id = 1" ) ) {
            assertTrue( stored.next() );
            return List.of( stored.getObject( 1 ), stored.getObject( 2 ) );
        }
    }


    @Test
    void projectedNumericExpressionsUseTheOrdinaryWriteSemantics() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE projected_numeric_expr (id INTEGER PRIMARY KEY, narrow INTEGER, wide BIGINT, "
                    + "real_num REAL, double_num DOUBLE, amount DECIMAL(12,4), nested_amount DECIMAL(12,4))" );
            try {
                for ( boolean parameterize : new boolean[]{ true, false } ) {
                    sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + parameterize );
                    String prefix = "INSERT INTO projected_numeric_expr (id,narrow,wide) VALUES ";
                    String expressions = ", CAST(42.5 AS DOUBLE) + CAST(0.0 AS DOUBLE), -CAST(42.5 AS DOUBLE))";
                    String foldedFields = "narrow, wide";
                    assertEquals( 1, sql.executeUpdate( prefix + "(1" + expressions ) );
                    List<Object> folded = readProjectedExpression( sql, 1, foldedFields );
                    assertEquals( 1, sql.executeUpdate( prefix + "(2" + expressions, new String[]{ "narrow", "wide" } ) );
                    try ( ResultSet keys = sql.getGeneratedKeys() ) {
                        assertNumericWidths( keys, folded );
                    }
                    try ( ResultSet projected = sql.executeQuery( prefix + "(3" + expressions + " RETURNING " + foldedFields ) ) {
                        assertNumericWidths( projected, folded );
                    }
                    for ( int id : new int[]{ 2, 3 } ) {
                        assertEquals( folded, readProjectedExpression( sql, id, foldedFields ) );
                    }

                    String prepared = "INSERT INTO projected_numeric_expr (id,real_num,double_num,amount,nested_amount) VALUES "
                            + "(?, CAST(? AS DOUBLE) * CAST(1.0 AS DOUBLE), CAST(? AS REAL) * CAST(1.0 AS REAL), "
                            + "CAST(? AS DECIMAL(5,2)), CAST(CAST(? AS DECIMAL(6,3)) AS DECIMAL(5,2)))";
                    String preparedFields = "real_num, double_num, amount, nested_amount";
                    try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( prepared );
                            PreparedStatement keys = jdbc.getConnection().prepareStatement( prepared, new String[]{ "real_num", "double_num", "amount", "nested_amount" } );
                            PreparedStatement returning = jdbc.getConnection().prepareStatement( prepared + " RETURNING " + preparedFields ) ) {
                        bindProjectedExpressions( plain, 4 );
                        assertEquals( 1, plain.executeUpdate() );
                        List<Object> expected = readProjectedExpression( sql, 4, preparedFields );
                        bindProjectedExpressions( keys, 5 );
                        assertEquals( 1, keys.executeUpdate() );
                        try ( ResultSet projected = keys.getGeneratedKeys() ) {
                            assertNumericWidths( projected, expected );
                        }
                        bindProjectedExpressions( returning, 6 );
                        try ( ResultSet projected = returning.executeQuery() ) {
                            assertNumericWidths( projected, expected );
                        }
                        for ( int id : new int[]{ 5, 6 } ) {
                            assertEquals( expected, readProjectedExpression( sql, id, preparedFields ) );
                        }
                    }
                    sql.executeUpdate( "DELETE FROM projected_numeric_expr" );
                }
            } finally {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET true" );
                sql.executeUpdate( "DROP TABLE projected_numeric_expr" );
            }
        }
    }


    private static void bindProjectedExpressions( PreparedStatement statement, int id ) throws SQLException {
        statement.setInt( 1, id );
        statement.setDouble( 2, -0.0 );
        statement.setFloat( 3, -0.0f );
        statement.setBigDecimal( 4, new BigDecimal( "42.495" ) );
        statement.setBigDecimal( 5, new BigDecimal( "41.9946" ) );
    }


    private static List<Object> readProjectedExpression( Statement sql, int id, String fields ) throws SQLException {
        try ( ResultSet stored = sql.executeQuery( "SELECT " + fields + " FROM projected_numeric_expr WHERE id = " + id ) ) {
            assertTrue( stored.next() );
            List<Object> values = new ArrayList<>();
            for ( int column = 1; column <= stored.getMetaData().getColumnCount(); column++ ) {
                values.add( stored.getObject( column ) );
            }
            return values;
        }
    }


    @Test
    void projectedDecimalCastUsesTheSameRangeCheckAsAnOrdinaryInsert() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE projected_decimal_cast (id INTEGER PRIMARY KEY, amount DECIMAL(12,4))" );
            try {
                String insert = "INSERT INTO projected_decimal_cast VALUES (?, CAST(? AS DECIMAL(5,2)))";
                try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( insert );
                        PreparedStatement keys = jdbc.getConnection().prepareStatement( insert, new String[]{ "amount" } );
                        PreparedStatement returning = jdbc.getConnection().prepareStatement( insert + " RETURNING amount" ) ) {
                    BigDecimal input = new BigDecimal( "1000" );
                    plain.setInt( 1, 1 );
                    plain.setBigDecimal( 2, input );
                    keys.setInt( 1, 2 );
                    keys.setBigDecimal( 2, input );
                    returning.setInt( 1, 3 );
                    returning.setBigDecimal( 2, input );
                    try {
                        assertEquals( 1, plain.executeUpdate() );
                    } catch ( SQLException failure ) {
                        assertEquals( "22003", failure.getSQLState() );
                        assertEquals( failure.getSQLState(), assertThrows( SQLException.class, keys::executeUpdate ).getSQLState() );
                        assertEquals( failure.getSQLState(), assertThrows( SQLException.class, returning::executeQuery ).getSQLState() );
                        try ( ResultSet stored = sql.executeQuery( "SELECT amount FROM projected_decimal_cast" ) ) {
                            assertFalse( stored.next() );
                        }
                        return;
                    }
                    List<Object> expected;
                    try ( ResultSet stored = sql.executeQuery( "SELECT amount FROM projected_decimal_cast WHERE id = 1" ) ) {
                        assertTrue( stored.next() );
                        expected = List.of( stored.getBigDecimal( 1 ) );
                    }
                    assertEquals( 1, keys.executeUpdate() );
                    try ( ResultSet projected = keys.getGeneratedKeys() ) {
                        assertNumericWidths( projected, expected );
                    }
                    try ( ResultSet projected = returning.executeQuery() ) {
                        assertNumericWidths( projected, expected );
                    }
                    try ( ResultSet stored = sql.executeQuery( "SELECT amount FROM projected_decimal_cast WHERE id IN (2,3)" ) ) {
                        for ( int row = 0; row < 2; row++ ) {
                            assertTrue( stored.next() );
                            assertEquals( expected.get( 0 ), stored.getBigDecimal( 1 ) );
                        }
                        assertFalse( stored.next() );
                    }
                }
            } finally {
                sql.executeUpdate( "DROP TABLE projected_decimal_cast" );
            }
        }
    }


    @Test
    void approximateProjectionsKeepStoredCarriersAndSpecialFloatingValues() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE native_projected_float (id INTEGER PRIMARY KEY, real_num REAL, double_num DOUBLE)" );
            try {
                String insert = "INSERT INTO native_projected_float VALUES (?, ?, ?)";
                String fields = "real_num, double_num";
                for ( boolean parameterize : new boolean[]{ true, false } ) {
                    sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET " + parameterize );
                    try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( insert );
                            PreparedStatement keys = jdbc.getConnection().prepareStatement( insert, new String[]{ "real_num", "double_num" } );
                            PreparedStatement returning = jdbc.getConnection().prepareStatement( insert + " RETURNING " + fields ) ) {
                        int id = 0;
                        for ( double sample : new double[]{ 0.1, -0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY } ) {
                            bindApproximate( plain, ++id, sample );
                            assertEquals( 1, plain.executeUpdate() );
                            List<Object> expected;
                            try ( ResultSet stored = sql.executeQuery( "SELECT " + fields + " FROM native_projected_float WHERE id = " + id ) ) {
                                assertTrue( stored.next() );
                                expected = List.of( stored.getObject( 1 ), stored.getObject( 2 ) );
                            }
                            int keyedId = ++id;
                            bindApproximate( keys, keyedId, sample );
                            assertEquals( 1, keys.executeUpdate() );
                            try ( ResultSet projected = keys.getGeneratedKeys() ) {
                                assertNumericWidths( projected, expected );
                            }
                            int returningId = ++id;
                            bindApproximate( returning, returningId, sample );
                            try ( ResultSet projected = returning.executeQuery() ) {
                                assertNumericWidths( projected, expected );
                            }
                            for ( int projectedId : new int[]{ keyedId, returningId } ) {
                                try ( ResultSet stored = sql.executeQuery( "SELECT " + fields + " FROM native_projected_float WHERE id = " + projectedId ) ) {
                                    assertNumericWidths( stored, expected );
                                }
                            }
                        }
                    }
                    sql.executeUpdate( "DELETE FROM native_projected_float" );
                }
            } finally {
                sql.executeUpdate( "ALTER CONFIG 'runtime/parameterizeDML' SET true" );
                sql.executeUpdate( "DROP TABLE native_projected_float" );
            }
        }
    }


    private static void bindApproximate( PreparedStatement insert, int id, double sample ) throws SQLException {
        insert.setInt( 1, id );
        insert.setDouble( 2, sample );
        insert.setFloat( 3, (float) sample );
    }


    @Test
    void readsBackIntegralProjectionsWithAFractionalPrimaryKey() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE projected_integral_key (bucket INTEGER NOT NULL, id SMALLINT NOT NULL, "
                    + "amount INTEGER, PRIMARY KEY(bucket, id))" );
            try {
                String insert = "INSERT INTO projected_integral_key (bucket, id, amount) VALUES (?, ?, ?)";
                try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( insert );
                        PreparedStatement keys = jdbc.getConnection().prepareStatement( insert, new String[]{ "id", "amount" } );
                        PreparedStatement returning = jdbc.getConnection().prepareStatement( insert + " RETURNING amount" ) ) {
                    bindFractionalPrimaryKey( plain, 0 );
                    assertEquals( 1, plain.executeUpdate() );
                    short expectedId;
                    int expectedAmount;
                    try ( ResultSet stored = sql.executeQuery( "SELECT id, amount FROM projected_integral_key WHERE bucket = 0" ) ) {
                        assertTrue( stored.next() );
                        expectedId = stored.getShort( 1 );
                        expectedAmount = stored.getInt( 2 );
                        assertFalse( stored.next() );
                    }
                    int adjacent = expectedId == 6 ? 7 : 6;
                    sql.executeUpdate( "INSERT INTO projected_integral_key VALUES (1, " + adjacent + ", 99), (2, " + adjacent + ", 99)" );

                    bindFractionalPrimaryKey( keys, 1 );
                    assertEquals( 1, keys.executeUpdate() );
                    try ( ResultSet rows = keys.getGeneratedKeys() ) {
                        assertTrue( rows.next() );
                        assertEquals( expectedId, rows.getShort( 1 ) );
                        assertEquals( expectedAmount, rows.getInt( 2 ) );
                        assertFalse( rows.next() );
                    }

                    bindFractionalPrimaryKey( returning, 2 );
                    try ( ResultSet rows = returning.executeQuery() ) {
                        assertTrue( rows.next() );
                        assertEquals( expectedAmount, rows.getInt( 1 ) );
                        assertFalse( rows.next() );
                    }
                }
            } finally {
                sql.executeUpdate( "DROP TABLE projected_integral_key" );
            }
        }
    }


    private static void bindFractionalPrimaryKey( PreparedStatement insert, int bucket ) throws SQLException {
        insert.setInt( 1, bucket );
        insert.setBigDecimal( 2, new BigDecimal( "6.75" ) );
        insert.setBigDecimal( 3, new BigDecimal( "31.6" ) );
    }


    private static void bindIntegralProjection( PreparedStatement insert, int id, boolean floatingAndScaledInputs ) throws SQLException {
        insert.setInt( 1, id );
        if ( floatingAndScaledInputs ) {
            insert.setDouble( 2, -7.75 );
            insert.setBigDecimal( 3, new BigDecimal( "42.00" ) );
        } else {
            insert.setBigDecimal( 2, new BigDecimal( "-7.75" ) );
            insert.setBigDecimal( 3, new BigDecimal( "42.6" ) );
        }
        insert.setBigDecimal( 4, new BigDecimal( "9007199254740993.75" ) );
    }


    private static List<Number> readIntegralProjection( ResultSet rows ) throws SQLException {
        assertTrue( rows.next() );
        int small = rows.getShort( 1 );
        int ordinary = rows.getInt( 2 );
        long wide = rows.getLong( 3 );
        assertEquals( Integer.valueOf( small ), rows.getObject( 1, Integer.class ) );
        assertEquals( Integer.valueOf( ordinary ), rows.getObject( 2, Integer.class ) );
        assertEquals( Long.valueOf( wide ), rows.getObject( 3, Long.class ) );
        assertEquals( Integer.toString( small ), rows.getString( 1 ) );
        assertEquals( Integer.toString( ordinary ), rows.getString( 2 ) );
        assertEquals( Long.toString( wide ), rows.getString( 3 ) );
        assertFalse( rows.next() );
        return List.of( small, ordinary, wide );
    }


    @Test
    void readsBackTheInsertedDecimalPrimaryKeyInsteadOfAnAdjacentRow() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE projected_decimal_key (bucket INTEGER NOT NULL, id DECIMAL(12,4) NOT NULL, amount DECIMAL(12,4), PRIMARY KEY(bucket, id))" );
            try {
                String insert = "INSERT INTO projected_decimal_key (bucket, id, amount) VALUES (?, ?, ?)";
                try ( PreparedStatement plain = jdbc.getConnection().prepareStatement( insert );
                        PreparedStatement keys = jdbc.getConnection().prepareStatement( insert, new String[]{ "id", "amount" } );
                        PreparedStatement returning = jdbc.getConnection().prepareStatement( insert + " RETURNING id, amount" ) ) {
                    BigDecimal amount = new BigDecimal( "1.234567" );
                    for ( int sign : new int[]{ -1, 1 } ) {
                        BigDecimal input = new BigDecimal( "1.23455" ).multiply( BigDecimal.valueOf( sign ) );
                        plain.setInt( 1, 0 );
                        plain.setBigDecimal( 2, input );
                        plain.setBigDecimal( 3, amount );
                        plain.executeUpdate();
                        BigDecimal storedId;
                        BigDecimal storedAmount;
                        try ( ResultSet row = sql.executeQuery( "SELECT id, amount FROM projected_decimal_key WHERE bucket = 0 AND id * " + sign + " > 0" ) ) {
                            assertTrue( row.next() );
                            storedId = row.getBigDecimal( 1 );
                            storedAmount = row.getBigDecimal( 2 );
                        }
                        BigDecimal lower = input.setScale( 4, RoundingMode.FLOOR );
                        BigDecimal adjacent = lower.equals( storedId ) ? input.setScale( 4, RoundingMode.CEILING ) : lower;
                        int bucket = sign == -1 ? 1 : 2;
                        sql.executeUpdate( "INSERT INTO projected_decimal_key VALUES (" + bucket + ", " + adjacent.toPlainString() + ", 99)" );
                        PreparedStatement projected = sign == -1 ? keys : returning;
                        projected.setInt( 1, bucket );
                        projected.setBigDecimal( 2, input );
                        projected.setBigDecimal( 3, amount );
                        if ( sign == -1 ) {
                            assertEquals( 1, projected.executeUpdate() );
                        }
                        try ( ResultSet row = sign == -1 ? projected.getGeneratedKeys() : projected.executeQuery() ) {
                            assertTrue( row.next() );
                            assertEquals( storedId, row.getBigDecimal( 1 ) );
                            assertEquals( storedAmount, row.getBigDecimal( 2 ) );
                            assertFalse( row.next() );
                        }
                    }
                }
            } finally {
                sql.executeUpdate( "DROP TABLE projected_decimal_key" );
            }
        }
    }


    private static void bindDecimalRows( PreparedStatement insert, int id, BigDecimal first, BigDecimal second ) throws SQLException {
        insert.setInt( 1, id + 1 );
        insert.setBigDecimal( 2, first );
        insert.setInt( 3, id );
        insert.setBigDecimal( 4, second );
    }


    private static List<BigDecimal> readDecimals( ResultSet rows ) throws SQLException {
        List<BigDecimal> result = new ArrayList<>();
        while ( rows.next() ) {
            result.add( rows.getBigDecimal( 1 ) );
        }
        return result;
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
