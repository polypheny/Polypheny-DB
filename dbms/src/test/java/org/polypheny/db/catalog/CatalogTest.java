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

package org.polypheny.db.catalog;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.entity.LogicalAdapter;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
public class CatalogTest {

    private static TestHelper helper;


    @BeforeAll
    public static void start() {
        helper = TestHelper.getInstance();
        addTestData();
    }


    @AfterAll
    public static void stop() throws SQLException {
        deleteOldData();

        helper.checkAllTrxClosed();
    }


    private static void addTestData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE NAMESPACE schema1" );
                statement.executeUpdate( "CREATE TABLE schema1.table1( id INTEGER NOT NULL, PRIMARY KEY(id))" );
                statement.executeUpdate( "ALTER TABLE schema1.table1 ADD COLUMN name VARCHAR (255) NULL" );
                if ( helper.storeSupportsIndex() ) {
                    statement.executeUpdate( "ALTER TABLE schema1.table1 ADD UNIQUE INDEX index1 ON id ON STORE hsqldb" );
                }
                statement.executeUpdate( "CREATE TABLE schema1.table2( id INTEGER NOT NULL, PRIMARY KEY(id) )" );
                statement.executeUpdate( "ALTER TABLE schema1.table2 ADD CONSTRAINT fk_id FOREIGN KEY (id) REFERENCES schema1.table1(id) ON UPDATE RESTRICT ON DELETE RESTRICT" );
                statement.executeUpdate( "CREATE DOCUMENT SCHEMA private" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while adding test data", e );
        }
    }


    private static void deleteOldData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "ALTER TABLE schema1.table2 DROP FOREIGN KEY fk_id" );
                    statement.executeUpdate( "ALTER TABLE schema1.table1 DROP INDEX index1" );
                } catch ( SQLException e ) {
                    log.error( "Exception while deleting old data", e );
                }
                try {
                    statement.executeUpdate( "DROP TABLE schema1.table1" );
                } catch ( SQLException e ) {
                    log.error( "Exception while deleting old data", e );
                }
                try {
                    statement.executeUpdate( "DROP TABLE schema1.table2" );
                } catch ( SQLException e ) {
                    log.error( "Exception while deleting old data", e );
                }
                statement.executeUpdate( "DROP SCHEMA schema1" );
                statement.executeUpdate( "DROP SCHEMA private" );
                connection.commit();
            }
        }
    }


    @Test
    public void testGetCatalogs() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getCatalogs();
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            assertEquals( 2, totalColumns, "Wrong number of columns" );

            // Check column names
            assertEquals( "TABLE_CAT", rsmd.getColumnName( 1 ), "Wrong column name" );
            assertEquals( "DEFAULT_SCHEMA", rsmd.getColumnName( 2 ), "Wrong column name" );

            // Check data
            final Object[] databaseApp = new Object[]{ "APP", "public" };

            TestHelper.checkResultSet(
                    connection.getMetaData().getCatalogs(),
                    ImmutableList.of( databaseApp ) );
        }
    }


    @Test
    public void testGetSchema() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            final Object[] schemaTest = new Object[]{ "schema1", null, "RELATIONAL" };
            final Object[] schemaPublic = new Object[]{ "public", null, "RELATIONAL" };
            final Object[] schemaPrivate = new Object[]{ "private", null, "DOCUMENT" };

            TestHelper.checkResultSet(
                    connection.getMetaData().getSchemas( "APP", null ),
                    ImmutableList.of( schemaPublic, schemaTest, schemaPrivate ),
                    true );

            TestHelper.checkResultSet(
                    connection.getMetaData().getSchemas( "APP", "schema1" ),
                    ImmutableList.of( schemaTest ),
                    true );
        }
    }


    @Test
    public void testGetTable() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            final Object[] table1 = new Object[]{ null, "schema1", "table1", "ENTITY", "", null, null, null, null, null };
            final Object[] table2 = new Object[]{ null, "schema1", "table2", "ENTITY", "", null, null, null, null, null };

            TestHelper.checkResultSet(
                    connection.getMetaData().getTables( "APP", "schema1", null, null ),
                    ImmutableList.of( table1, table2 ) );
        }
    }


    @Test
    public void testGetColumn() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            // Check data
            final Object[] column1 = new Object[]{ null, "schema1", "table1", "id", 4, "INTEGER", null, null, null, null,
                    0, "", null, null, null, null, 1, "NO", null, null, null, null, "NO", "NO", null };
            final Object[] column2 = new Object[]{ null, "schema1", "table1", "name", 12, "VARCHAR", 255, null, null, null,
                    1, "", null, null, null, null, 2, "YES", null, null, null, null, "NO", "NO", "CASE_INSENSITIVE" };

            TestHelper.checkResultSet(
                    connection.getMetaData().getColumns( "APP", "schema1", "table1", null ),
                    ImmutableList.of( column1, column2 ) );
        }
    }


    @Test
    public void testGetIndex() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            LogicalAdapter adapter = Catalog.snapshot().getAdapter( "hsqldb" ).orElseThrow();
            // Check data
            final Object[] index1 = new Object[]{ null, "schema1", "table1", false, null, "index1", 0, 1, "id", null, -1, null, null, adapter.id, 1 };

            if ( helper.storeSupportsIndex() ) {
                TestHelper.checkResultSet(
                        connection.getMetaData().getIndexInfo( "APP", "schema1", "table1", false, false ),
                        ImmutableList.of( index1 ) );
            } else {
                TestHelper.checkResultSet(
                        connection.getMetaData().getIndexInfo( "APP", "schema1", "table1", false, false ),
                        ImmutableList.of() );
            }
        }
    }


    @Test
    public void testGetForeignKeys() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            final Object[] foreignKeys = new Object[]{ null, "schema1", "table1", /* TODO was "id" */ null,
                    null, "schema1", "table2", "id", 1, 1, 1, "fk_id", null, null };

            TestHelper.checkResultSet(
                    connection.getMetaData().getExportedKeys( "APP", "schema1", "table1" ),
                    ImmutableList.of( foreignKeys ) );
        }
    }


    @Test
    public void testConcurrentTableOperations() throws InterruptedException, SQLException {
        CountDownLatch startLatch = new CountDownLatch( 1 );
        AtomicBoolean thread1Success = new AtomicBoolean( false );
        AtomicBoolean thread2Success = new AtomicBoolean( false );

        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE concurrent_test(id INTEGER PRIMARY KEY)" );
                statement.executeUpdate( "CREATE TABLE concurrent_test2(id INTEGER PRIMARY KEY)" );
            }
        }

        Thread insertThread = new Thread( () -> {
            try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
                Connection connection = jdbcConnection.getConnection();

                try ( Statement statement = connection.createStatement() ) {
                    statement.executeUpdate( "INSERT INTO concurrent_test2 (id) VALUES (1)" );
                    startLatch.countDown();
                    Thread.sleep( 100 ); // Give the other Thread a chance to lock concurrent_test
                    connection.commit();
                    thread1Success.set( true );
                }
            } catch ( SQLException | InterruptedException e ) {
                throw new RuntimeException( e );
            }
        } );

        Thread dropThread = new Thread( () -> {
            try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
                Connection connection = jdbcConnection.getConnection();

                try ( Statement statement = connection.createStatement() ) {
                    startLatch.await(); // Wait for signal to start
                    statement.executeUpdate( "DROP TABLE IF EXISTS concurrent_test" );
                    thread2Success.set( true );
                }
            } catch ( SQLException | InterruptedException e ) {
                throw new RuntimeException( e );
            }
        } );

        insertThread.start();
        dropThread.start();

        insertThread.join();
        dropThread.join();

        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE concurrent_test2" );
            }
        }

        assertTrue( thread1Success.get() );
        assertTrue( thread2Success.get() );
    }

}
