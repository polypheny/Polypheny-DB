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

package org.polypheny.db.adapter.parquet;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.parquet.relational.ParquetRelationalSource;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import com.google.common.collect.ImmutableList;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

class ParquetPluginTest {

    private static final String SOURCE_NAME = "parquet_test";
    private static TestHelper helper;
    private static boolean sourceCreated = false;


    private static String table( String tableName ) {
        return SOURCE_NAME + "__" + tableName;
    }

    //region initialization and finalization
    @BeforeAll
    static void start() throws SQLException {
        // starts the Polypheny test instance
        helper = TestHelper.getInstance();

        // open a JDBC connection to the running Polypheny test server
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            // creates the Parquet source adapter and imports the parquet files as source tables
             try ( Statement statement = connection.createStatement() ) {
                String sql = "ALTER ADAPTERS ADD \"" + SOURCE_NAME + "\" USING '" + ParquetRelationalSource.NAME + "' AS 'SOURCE' "
                        + "WITH '{method:\"upload\",directory:\"classpath://orders_db\",directoryName:\"classpath://orders_db\",url:\"file:/\",\"" + ParquetRelationalSource.SCHEMA_MODE_SETTING + "\":\"flat\"}'";

                System.out.println( sql );
                statement.executeUpdate( sql );
                connection.commit();
                sourceCreated = true;
            }
        }
    }


    @AfterAll
    static void end() throws SQLException {
        if ( sourceCreated ) {
            // open a JDBC connection
            try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
                Connection connection = jdbcConnection.getConnection();
                try ( Statement statement = connection.createStatement() ) {
                    // remove the source adapter and the imported source tables from the catalog
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"" + SOURCE_NAME + "\"" );
                    connection.commit();
                }
            }
            // verify no transactions left open
            helper.checkAllTrxClosed();
        }
    }
    //endregion

    // validate that tables loaded and contain data
    @Test
    void importsAllTablesAndReadsRows() throws SQLException {
        assertTableHasRows( table( "customers" ) );
        assertTableHasRows( table( "orders" ) );
        assertTableHasRows( table( "order_items" ) );
        assertTableHasRows( table( "products" ) );
    }

    // validate that tables are read-only, this means delete attempt is invalid
    @Test
    void parquetSourceIsReadOnly() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                assertThrows( SQLException.class, () -> statement.executeUpdate( "DELETE FROM " + table( "customers" ) ) );
            }
        }
    }

    private void assertTableHasRows( String tableName ) throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery( "SELECT COUNT(*) FROM " + tableName ) ) {

                assertTrue( resultSet.next(), "COUNT query returned no row for table " + tableName );
                assertTrue( resultSet.getLong( 1 ) > 0, "Expected table " + tableName + " to contain rows" );
            }
        }
    }

    // read 3 rows and compare result with expected
    @Test
    void readsExpectedRowsFromCustomers() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(
                            "SELECT customer_id, name, email, country, signup_date "
                                    + "FROM " + table( "customers" ) + " "
                                    + "WHERE customer_id <= 3 "
                                    + "ORDER BY customer_id" ) ) {

                TestHelper.checkResultSet(
                        resultSet,
                        ImmutableList.of(
                                new Object[]{ 1L, "Carla Schmid", "carla.schmid2317@mail.com", "Israel", Timestamp.valueOf( "2021-05-17 00:00:00" ) },
                                new Object[]{ 2L, "Urs Meier", "urs.meier3475@demo.net", "UK", Timestamp.valueOf( "2024-10-25 00:00:00" ) },
                                new Object[]{ 3L, "Rita Garcia", "rita.garcia8608@mail.com", "France", Timestamp.valueOf( "2023-02-04 00:00:00" ) }
                        )
                );
            }
        }
    }
    // check filter pushdown - where close
    @Test
    void filtersRowsWithWhereClause() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(
                            "SELECT customer_id, name, email, country, signup_date "
                                    + "FROM " + table( "customers" ) + " "
                                    + "WHERE country = 'France' "
                                    + "ORDER BY customer_id "
                                    + "LIMIT 3" ) ) {

                TestHelper.checkResultSet(
                        resultSet,
                        ImmutableList.of(
                                new Object[]{ 3L, "Rita Garcia", "rita.garcia8608@mail.com", "France", Timestamp.valueOf( "2023-02-04 00:00:00" ) },
                                new Object[]{ 8L, "Sara Dubois", "sara.dubois8067@mail.com", "France", Timestamp.valueOf( "2022-10-11 00:00:00" ) },
                                new Object[]{ 17L, "Noah Horvat", "noah.horvat6459@example.org", "France", Timestamp.valueOf( "2021-02-20 00:00:00" ) }
                        )
                );
            }
        }
    }
    // text projection - get only requested columns
    @Test
    void projectsOnlyRequestedColumns() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(
                            "SELECT customer_id, name "
                                    + "FROM " + table( "customers" ) + " "
                                    + "WHERE customer_id <= 3 "
                                    + "ORDER BY customer_id" ) ) {

                TestHelper.checkResultSet(
                        resultSet,
                        ImmutableList.of(
                                new Object[]{ 1L, "Carla Schmid" },
                                new Object[]{ 2L, "Urs Meier" },
                                new Object[]{ 3L, "Rita Garcia" }
                        )
                );
            }
        }
    }
    // test comparison filter
    @Test
    void supportsGreaterThanFilter() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(
                            "SELECT customer_id, name "
                                    + "FROM " + table( "customers" ) + " "
                                    + "WHERE customer_id > 3 "
                                    + "ORDER BY customer_id "
                                    + "LIMIT 3" ) ) {

                TestHelper.checkResultSet(
                        resultSet,
                        ImmutableList.of(
                                new Object[]{ 4L, "Lars Cohen" },
                                new Object[]{ 5L, "Lars Keller" },
                                new Object[]{ 6L, "Will Novak" }
                        )
                );
            }
        }
    }

    // test filter operations
    @Test
    void supportsAllComparisonFilterOperations() throws SQLException {
        assertCustomerFilterResult(
                "customer_id = 3",
                ImmutableList.of(
                        new Object[]{ 3L, "Rita Garcia" }
                )
        );

        assertCustomerFilterResult(
                "customer_id != 3",
                ImmutableList.of(
                        new Object[]{ 1L, "Carla Schmid" },
                        new Object[]{ 2L, "Urs Meier" },
                        new Object[]{ 4L, "Lars Cohen" }
                )
        );

        assertCustomerFilterResult(
                "customer_id > 3",
                ImmutableList.of(
                        new Object[]{ 4L, "Lars Cohen" },
                        new Object[]{ 5L, "Lars Keller" },
                        new Object[]{ 6L, "Will Novak" }
                )
        );

        assertCustomerFilterResult(
                "customer_id >= 3",
                ImmutableList.of(
                        new Object[]{ 3L, "Rita Garcia" },
                        new Object[]{ 4L, "Lars Cohen" },
                        new Object[]{ 5L, "Lars Keller" }
                )
        );

        assertCustomerFilterResult(
                "customer_id < 3",
                ImmutableList.of(
                        new Object[]{ 1L, "Carla Schmid" },
                        new Object[]{ 2L, "Urs Meier" }
                )
        );

        assertCustomerFilterResult(
                "customer_id <= 3",
                ImmutableList.of(
                        new Object[]{ 1L, "Carla Schmid" },
                        new Object[]{ 2L, "Urs Meier" },
                        new Object[]{ 3L, "Rita Garcia" }
                )
        );
    }

    private void assertCustomerFilterResult( String filter, ImmutableList<Object[]> expected ) throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(
                            "SELECT customer_id, name "
                                    + "FROM " + table( "customers" ) + " "
                                    + "WHERE " + filter + " "
                                    + "ORDER BY customer_id "
                                    + "LIMIT 3" ) ) {

                TestHelper.checkResultSet( resultSet, expected );
            }
        }
    }

    // test that update not available
    @Test
    void rejectsUpdateOnParquetSource() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                assertThrows(
                        SQLException.class,
                        () -> statement.executeUpdate(
                                "UPDATE " + table( "customers" ) + " "
                                        + "SET country = 'Switzerland' "
                                        + "WHERE customer_id = 1" )
                );
            }
        }
    }

}
