/*
 * Copyright 2019-2020 The Polypheny Project
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


import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings("SqlDialectInspection")
@Slf4j
public class CatalogTest {

    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        //deleteOldData();
        addTestData();
    }


    @AfterClass
    public static void stop() {
        deleteOldData();
    }


    @SuppressWarnings({ "SqlNoDataSourceInspection" })
    private static void addTestData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection() ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE SCHEMA schema1" );
                statement.executeUpdate( "CREATE TABLE schema1.table1( id INTEGER NOT NULL, PRIMARY KEY(id))" );
                statement.executeUpdate( "ALTER TABLE schema1.table1 ADD COLUMN name VARCHAR (255) NOT NULL " );
                statement.executeUpdate( "ALTER TABLE schema1.table1 ADD UNIQUE INDEX index1 ON id USING BTREE" );
                statement.executeUpdate( "CREATE TABLE schema1.table2( id INTEGER NOT NULL, PRIMARY KEY(id) )" );
                statement.executeUpdate( "ALTER TABLE schema1.table2 ADD CONSTRAINT fk_id FOREIGN KEY (id) REFERENCES schema1.table1(id) ON UPDATE CASCADE ON DELETE SET NULL" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while adding test data", e );
        }
    }


    @SuppressWarnings({ "SqlNoDataSourceInspection" })
    private static void deleteOldData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection() ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER TABLE schema1.table2 DROP FOREIGN KEY fk_id" );
                statement.executeUpdate( "ALTER TABLE schema1.table1 DROP INDEX index1" );
                statement.executeUpdate( "DROP TABLE schema1.table1" );
                statement.executeUpdate( "DROP TABLE schema1.table2" );
                statement.executeUpdate( "DROP SCHEMA schema1" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while deleting old data", e );
        }
    }


    @Test
    public void testGetCatalogs() {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection() ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getCatalogs();
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 3, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TABLE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "OWNER", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "DEFAULT_SCHEMA", rsmd.getColumnName( 3 ) );

            // Check data
            final Object[] databaseApp = new Object[]{ "APP", "system", "public" };

            checkResultSet(
                    connection.getMetaData().getCatalogs(),
                    ImmutableList.of( databaseApp ) );
        } catch ( SQLException e ) {
            log.error( "Exception while testing getCatalogs()", e );
        }
    }


    @Test
    public void testGetSchema() {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection() ) {
            Connection connection = polyphenyDbConnection.getConnection();

            final Object[] schemaTest = new Object[]{ "schema1", "APP", "pa", "RELATIONAL" };
            final Object[] schemaPublic = new Object[]{ "public", "APP", "system", "RELATIONAL" };

            checkResultSet(
                    connection.getMetaData().getSchemas( "APP", null ),
                    ImmutableList.of( schemaPublic, schemaTest ) );

            checkResultSet(
                    connection.getMetaData().getSchemas( "APP", "schema1" ),
                    ImmutableList.of( schemaTest ) );

        } catch ( SQLException e ) {
            log.error( "Exception while testing getSchemas()", e );
        }
    }


    @Test
    public void testGetTable() {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection() ) {
            Connection connection = polyphenyDbConnection.getConnection();

            final Object[] table1 = new Object[]{ "APP", "schema1", "table1", "TABLE", "", null, null, null, null, null, "pa", null };
            final Object[] table2 = new Object[]{ "APP", "schema1", "table2", "TABLE", "", null, null, null, null, null, "pa", null };

            checkResultSet(
                    connection.getMetaData().getTables( "APP", "schema1", null, null ),
                    ImmutableList.of( table1, table2 ) );


        } catch ( SQLException e ) {
            log.error( "Exception while testing getTables()", e );
        }
    }


    @Test
    public void testGetColumn() {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection() ) {
            Connection connection = polyphenyDbConnection.getConnection();

            // Check data
            final Object[] column1 = new Object[]{ "APP", "schema1", "table1", "id", 4, "INTEGER", null, null, null, null, 0, "", null, null, null, null, 1, "NO", null };
            final Object[] column2 = new Object[]{ "APP", "schema1", "table1", "name", 12, "VARCHAR", 255, null, null, null, 0, "", null, null, null, null, 2, "NO", "CASE_INSENSITIVE" };

            checkResultSet(
                    connection.getMetaData().getColumns( "APP", "schema1", "table1", null ),
                    ImmutableList.of( column1, column2 ) );


        } catch ( SQLException e ) {
            log.error( "Exception while testing getTables()", e );
        }
    }


    @Test
    public void testGetIndex() {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection() ) {
            Connection connection = polyphenyDbConnection.getConnection();

            // Check data
            final Object[] index1 = new Object[]{ "APP", "schema1", "table1", false, null, "index1", 0, 1, "id", null, -1, null, null, null, 1 };

            checkResultSet(
                    connection.getMetaData().getIndexInfo( "APP", "schema1", "table1", false, false ),
                    ImmutableList.of( index1 ) );


        } catch ( SQLException e ) {
            log.error( "Exception while testing getTables()", e );
        }
    }


    @Test
    public void testGetForeignKeys() {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection() ) {
            Connection connection = polyphenyDbConnection.getConnection();

            final Object[] foreignKeys = new Object[]{ "APP", "schema1", "table1", "id", "APP", "schema1", "table2", "id", 1, 0, 2, "fk_id", null, null };

            checkResultSet(
                    connection.getMetaData().getExportedKeys( "APP", "schema1", "table1" ),
                    ImmutableList.of( foreignKeys ) );


        } catch ( SQLException e ) {
            log.error( "Exception while testing getTables()", e );
        }
    }


    private void checkResultSet( ResultSet resultSet, List<Object[]> expected ) throws SQLException {
        int i = 0;
        while ( resultSet.next() ) {
            Assert.assertTrue( "Result set has more rows than expected", i < expected.size() );
            Object[] expectedRow = expected.get( i++ );

            Assert.assertEquals( "Wrong number of columns", expectedRow.length, resultSet.getMetaData().getColumnCount() );
            int j = 0;
            while ( j < expectedRow.length ) {
                if ( expectedRow.length >= j + 1 ) {
                    Assert.assertEquals( "Unexpected data in column '" + resultSet.getMetaData().getColumnName( j + 1 ) + "'", expectedRow[j++], resultSet.getObject( j ) );
                } else {
                    fail( "More data available then expected." );
                }
            }
        }
        Assert.assertEquals( "Wrong number of rows in the result set", expected.size(), i );
    }

}
