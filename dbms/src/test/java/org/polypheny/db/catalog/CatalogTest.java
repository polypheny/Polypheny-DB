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
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while testing getTables()", e );
        }
    }


    @SuppressWarnings({ "SqlNoDataSourceInspection" })
    private static void deleteOldData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection() ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP SCHEMA IF EXISTS schema1" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while testing getTables()", e );
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

            while ( resultSet.next() ) {
                System.out.println( resultSet );
            }

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
            ResultSet resultSet = connection.getMetaData().getSchemas();
            ResultSetMetaData rsmd = resultSet.getMetaData();

            final Object[] schemaTest = new Object[]{ "schema1", "APP", "pa", "RELATIONAL" };
            final Object[] schemaPublic = new Object[]{ "public", "APP", "system", "RELATIONAL" };

            checkResultSet(
                    connection.getMetaData().getSchemas( "APP", null ),
                    ImmutableList.of( schemaPublic, schemaTest ) );

        } catch ( SQLException e ) {
            log.error( "Exception while testing getSchemas()", e );
        }
    }


    private void checkResultSet( ResultSet resultSet, List<Object[]> expected ) throws SQLException {
        int i = 0;
        while ( resultSet.next() ) {
            Assert.assertTrue( "Result set has more rows than expected", i < expected.size() );
            Object[] expectedRow = expected.get( i++ );

            int j = 0;
            while ( j < 4 ) {
                j++;
                System.out.println( resultSet.getObject( j ) );
            }

            Assert.assertEquals( "Wrong number of columns", expectedRow.length, resultSet.getMetaData().getColumnCount() );
            j = 0;
            while ( j < expectedRow.length ) {
                Assert.assertEquals( "Unexpected data in column '" + resultSet.getMetaData().getColumnName( j + 1 ) + "'", expectedRow[j++], resultSet.getObject( j ) );
            }
        }
        Assert.assertEquals( "Wrong number of rows in the result set", expected.size(), i );
    }

}
