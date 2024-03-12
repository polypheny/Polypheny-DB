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

package org.polypheny.db.misc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Tag("adapter")
public class DataMigratorTest {


    private static TestHelper helper;


    @BeforeAll
    public static void start() {
        // Ensures that Polypheny-DB is running
        helper = TestHelper.getInstance();
    }


    @BeforeEach
    public void beforeEach() {
        helper.randomizeCatalogIds();
    }


    @Test
    public void basicTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE datamigratortest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    statement.executeUpdate( "INSERT INTO datamigratortest VALUES (1,5,'foo')" );

                    // Add data store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'Hsqldb' AS 'Store'"
                            + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );
                    connection.commit();
                    // Add placement
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" ADD PLACEMENT ON STORE \"store1\"" );

                    // Remove placement on initial store
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" DROP PLACEMENT ON STORE \"hsqldb\"" );

                    // Checks
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM datamigratortest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 5, "foo" } ) );
                } finally {
                    // Drop table and store
                    statement.executeUpdate( "DROP TABLE datamigratortest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"store1\"" );
                }
            }
        }
    }


    @Test
    public void partialPlacementsTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE datamigratortest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                LogicalTable table = Catalog.snapshot().rel().getTable( Catalog.defaultNamespaceId, "datamigratortest" ).orElseThrow();

                try {
                    statement.executeUpdate( "INSERT INTO datamigratortest VALUES (1,5,'foo')" );

                    // Add data store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'Hsqldb' AS 'Store'"
                            + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // Add placement tprimary/tvarchar
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" ADD PLACEMENT (tvarchar) ON STORE \"store1\"" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM datamigratortest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 5, "foo" } ) );

                    assertEquals( 2, Catalog.snapshot().alloc().getPlacementsFromLogical( table.id ).size() );

                    // Remove tvarchar column placement on initial store tprimary/tinteger
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" MODIFY PLACEMENT (tinteger) ON STORE \"hsqldb\"" );

                    assertEquals( 2, Catalog.snapshot().alloc().getPlacementsFromLogical( table.id ).size() );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM datamigratortest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 5, "foo" } ) );

                    assertEquals( 2, Catalog.snapshot().alloc().getPlacementsFromLogical( table.id ).size() );

                    // Add tinteger column placement on store 1
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" MODIFY PLACEMENT (tinteger,tvarchar) ON STORE \"store1\"" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM datamigratortest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 5, "foo" } ) );

                    // Remove placement on initial store
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" DROP PLACEMENT ON STORE \"hsqldb\"" );

                } finally {
                    // Drop table and store
                    statement.executeUpdate( "DROP TABLE datamigratortest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"store1\"" );
                }
            }
        }
    }


    @Test
    public void alternativeCommandsTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE datamigratortest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    statement.executeUpdate( "INSERT INTO datamigratortest VALUES (1,5,'foo')" );

                    // Add data store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'Hsqldb' AS 'Store'"
                            + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // Add placement
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" ADD PLACEMENT (tvarchar) ON STORE \"store1\"" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM datamigratortest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 5, "foo" } ) );

                    // Remove tvarchar column placement on initial store
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" MODIFY PLACEMENT DROP COLUMN tvarchar ON STORE \"hsqldb\"" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM datamigratortest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 5, "foo" } ) );

                    // Add tinteger column placement on store 1
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" MODIFY PLACEMENT ADD COLUMN tinteger ON STORE \"store1\"" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM datamigratortest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 5, "foo" } ) );

                    // Remove placement on initial store
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" DROP PLACEMENT ON STORE \"hsqldb\"" );

                } finally {
                    // Drop table and store
                    statement.executeUpdate( "DROP TABLE datamigratortest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"store1\"" );
                }
            }
        }
    }


    @Test
    public void threeStoresTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE datamigratortest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "tboolean BOOLEAN NOT NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    statement.executeUpdate( "INSERT INTO datamigratortest VALUES (1,5,'foo',true)" );

                    // Add data store 1
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'Hsqldb' AS 'Store'"
                            + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );
                    // Add placement
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" ADD PLACEMENT (tvarchar) ON STORE \"store1\"" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM datamigratortest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 5, "foo", true } ) );

                    // Add data store 2
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store2\" USING 'Hsqldb' AS 'Store'"
                            + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );
                    // Add placement
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" ADD PLACEMENT (tboolean) ON STORE \"store2\"" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM datamigratortest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 5, "foo", true } ) );

                    // Remove tvarchar and tboolean column placement on initial store
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" MODIFY PLACEMENT (tinteger) ON STORE \"hsqldb\"" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM datamigratortest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 5, "foo", true } ) );

                    // Update
                    statement.executeUpdate( "UPDATE datamigratortest SET tinteger = 12 where tprimary = 1" );

                    // Add tinteger column placement on store 1
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" MODIFY PLACEMENT (tinteger,tvarchar) ON STORE \"store1\"" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM datamigratortest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 12, "foo", true } ) );

                    // Remove placement on initial store
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" DROP PLACEMENT ON STORE \"hsqldb\"" );

                    // Add tinteger and tvarchar column placement on store 2
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" MODIFY PLACEMENT (tinteger,tvarchar,tboolean) ON STORE \"store2\"" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM datamigratortest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 12, "foo", true } ) );

                    // Remove placement on initial store
                    statement.executeUpdate( "ALTER TABLE \"datamigratortest\" DROP PLACEMENT ON STORE \"store1\"" );

                    // Check if we still have everything on store 2
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM datamigratortest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 12, "foo", true } ) );

                } finally {
                    // Drop table and store
                    statement.executeUpdate( "DROP TABLE datamigratortest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"store1\"" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"store2\"" );
                }
            }
        }
    }

}
