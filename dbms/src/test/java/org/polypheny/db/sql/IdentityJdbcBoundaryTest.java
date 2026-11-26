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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;


@Tag("adapter")
class IdentityJdbcBoundaryTest {

    @BeforeAll
    static void start() {
        TestHelper.getInstance();
    }


    @Test
    void keyResultSetsRespectRowLimitsAndReportTheirSchema() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Connection connection = jdbc.getConnection();
                Statement sql = connection.createStatement() ) {
            sql.executeUpdate( "CREATE TABLE identity_jdbc_limits (id BIGSERIAL PRIMARY KEY, payload INTEGER)" );
            try {
                sql.setMaxRows( 1 );
                assertEquals( 2, sql.executeUpdate( "INSERT INTO identity_jdbc_limits (payload) VALUES (1), (2)", Statement.RETURN_GENERATED_KEYS ) );
                assertKey( sql, 1 );

                try ( PreparedStatement insert = connection.prepareStatement( "INSERT INTO identity_jdbc_limits (payload) VALUES (?), (?)", Statement.RETURN_GENERATED_KEYS ) ) {
                    insert.setLargeMaxRows( 1 );
                    insert.setInt( 1, 3 );
                    insert.setInt( 2, 4 );
                    assertEquals( 2, insert.executeLargeUpdate() );
                    assertKey( insert, 3 );
                }

                assertEquals( 1, sql.executeUpdate( "INSERT INTO identity_jdbc_limits VALUES (42, 5)", Statement.RETURN_GENERATED_KEYS ) );
                try ( ResultSet keys = sql.getGeneratedKeys() ) {
                    assertEmptyCursor( keys );
                    assertFalse( keys.next() );
                    assertEmptyCursor( keys );
                }
                try ( ResultSet rows = sql.executeQuery( "SELECT COUNT(*) FROM identity_jdbc_limits" ) ) {
                    assertTrue( rows.next() );
                    assertEquals( 5, rows.getInt( 1 ) );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE identity_jdbc_limits" );
            }
        }
    }


    @Test
    void aPreparedInsertRetainsBindingsAcrossExecutionsAndBatches() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Connection connection = jdbc.getConnection();
                Statement sql = connection.createStatement() ) {
            sql.executeUpdate( "CREATE TABLE identity_jdbc_bindings (id BIGSERIAL PRIMARY KEY, payload INTEGER)" );
            try ( PreparedStatement insert = connection.prepareStatement( "INSERT INTO identity_jdbc_bindings (payload) VALUES (?)", Statement.RETURN_GENERATED_KEYS ) ) {
                insert.setInt( 1, 8 );
                assertEquals( 1, insert.executeUpdate() );
                assertKey( insert, 1 );
                assertFalse( insert.execute() );
                assertKey( insert, 2 );

                insert.addBatch();
                insert.setInt( 1, 9 );
                insert.addBatch();
                assertArrayEquals( new int[]{ 1, 1 }, insert.executeBatch() );
                try ( ResultSet keys = insert.getGeneratedKeys() ) {
                    assertTrue( keys.next() );
                    assertEquals( 3, keys.getLong( 1 ) );
                    assertTrue( keys.next() );
                    assertEquals( 4, keys.getLong( 1 ) );
                    assertFalse( keys.next() );
                }
                assertEquals( 1, insert.executeLargeUpdate() );
                assertKey( insert, 5 );

                insert.clearParameters();
                assertEquals( "07001", assertThrows( SQLException.class, insert::executeUpdate ).getSQLState() );
                assertEquals( "07001", assertThrows( SQLException.class, insert::addBatch ).getSQLState() );
                insert.setNull( 1, Types.INTEGER );
                assertEquals( 1, insert.executeUpdate() );
                assertKey( insert, 6 );

                try ( ResultSet rows = sql.executeQuery( "SELECT payload FROM identity_jdbc_bindings ORDER BY id" ) ) {
                    for ( int payload : new int[]{ 8, 8, 8, 9, 9 } ) {
                        assertTrue( rows.next() );
                        assertEquals( payload, rows.getInt( 1 ) );
                    }
                    assertTrue( rows.next() );
                    assertNull( rows.getObject( 1 ) );
                    assertFalse( rows.next() );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE identity_jdbc_bindings" );
            }
        }
    }


    @Test
    void mixedIntegerWidthsShareOneAddressableKeyColumn() throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE identity_jdbc_narrow (id SERIAL PRIMARY KEY)" );
            sql.executeUpdate( "CREATE TABLE identity_jdbc_wide (id BIGSERIAL PRIMARY KEY)" );
            try {
                sql.executeUpdate( "ALTER TABLE identity_jdbc_wide ALTER COLUMN id RESTART WITH 9223372036854775807" );
                sql.addBatch( "INSERT INTO identity_jdbc_narrow DEFAULT VALUES" );
                sql.addBatch( "INSERT INTO identity_jdbc_wide DEFAULT VALUES" );
                assertArrayEquals( new int[]{ 1, 1 }, sql.executeBatch() );
                try ( ResultSet keys = sql.getGeneratedKeys() ) {
                    assertEquals( 1, keys.getMetaData().getColumnCount() );
                    assertEquals( Types.BIGINT, keys.getMetaData().getColumnType( 1 ) );
                    assertTrue( keys.next() );
                    assertEquals( Long.valueOf( 1 ), keys.getObject( "id" ) );
                    assertTrue( keys.next() );
                    assertEquals( Long.valueOf( Long.MAX_VALUE ), keys.getObject( "id" ) );
                    assertFalse( keys.next() );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE IF EXISTS identity_jdbc_wide" );
                sql.executeUpdate( "DROP TABLE IF EXISTS identity_jdbc_narrow" );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void rawBatchFailureReportsItsCommittedPrefix( boolean large ) throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Statement sql = jdbc.getConnection().createStatement() ) {
            sql.executeUpdate( "CREATE TABLE identity_jdbc_partial (id BIGSERIAL PRIMARY KEY)" );
            try {
                sql.addBatch( "INSERT INTO identity_jdbc_partial DEFAULT VALUES; -- one committed member" );
                sql.addBatch( "INSERT INTO identity_jdbc_missing DEFAULT VALUES" );
                sql.addBatch( "INSERT INTO identity_jdbc_partial DEFAULT VALUES" );
                BatchUpdateException failure = large
                        ? assertThrows( BatchUpdateException.class, sql::executeLargeBatch )
                        : assertThrows( BatchUpdateException.class, sql::executeBatch );
                assertEquals( "42P01", failure.getSQLState() );
                assertArrayEquals( new long[]{ 1 }, failure.getLargeUpdateCounts() );
                assertKey( sql, 1 );

                try ( JdbcConnection observer = new JdbcConnection( true );
                        Statement read = observer.getConnection().createStatement();
                        ResultSet rows = read.executeQuery( "SELECT id FROM identity_jdbc_partial" ) ) {
                    assertTrue( rows.next() );
                    assertEquals( 1, rows.getLong( 1 ) );
                    assertFalse( rows.next() );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE identity_jdbc_partial" );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void preparedBatchFailureReportsRollbackAndSequenceExhaustion( boolean large ) throws SQLException {
        try ( JdbcConnection jdbc = new JdbcConnection( true );
                Connection connection = jdbc.getConnection();
                Statement sql = connection.createStatement() ) {
            sql.executeUpdate( "CREATE TABLE identity_jdbc_exhaustion (id INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 10 MAXVALUE 10) PRIMARY KEY, payload INTEGER)" );
            try ( PreparedStatement insert = connection.prepareStatement( "INSERT INTO identity_jdbc_exhaustion (payload) VALUES (?)", Statement.RETURN_GENERATED_KEYS ) ) {
                insert.setInt( 1, 1 );
                insert.addBatch();
                insert.setInt( 1, 2 );
                insert.addBatch();
                BatchUpdateException failure = large
                        ? assertThrows( BatchUpdateException.class, insert::executeLargeBatch )
                        : assertThrows( BatchUpdateException.class, insert::executeBatch );
                assertEquals( "2200H", failure.getSQLState() );
                assertArrayEquals( new long[0], failure.getLargeUpdateCounts() );
                try ( ResultSet keys = insert.getGeneratedKeys();
                        ResultSet rows = sql.executeQuery( "SELECT id FROM identity_jdbc_exhaustion" ) ) {
                    assertFalse( keys.next() );
                    assertFalse( rows.next() );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE identity_jdbc_exhaustion" );
            }
        }
    }


    private static void assertKey( Statement statement, long expected ) throws SQLException {
        try ( ResultSet keys = statement.getGeneratedKeys() ) {
            assertTrue( keys.next() );
            assertEquals( expected, keys.getLong( 1 ) );
            assertEquals( Catalog.DEFAULT_NAMESPACE_NAME, keys.getMetaData().getSchemaName( 1 ) );
            assertFalse( keys.next() );
        }
    }


    private static void assertEmptyCursor( ResultSet keys ) throws SQLException {
        assertFalse( keys.isBeforeFirst() );
        assertFalse( keys.isAfterLast() );
        assertFalse( keys.isFirst() );
        assertFalse( keys.isLast() );
        assertEquals( 0, keys.getRow() );
    }

}
