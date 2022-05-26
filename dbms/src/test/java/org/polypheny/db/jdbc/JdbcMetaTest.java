/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.jdbc;


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


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
public class JdbcMetaTest {


    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestData();
    }


    private static void addTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE SCHEMA test" );
                statement.executeUpdate( "CREATE TABLE foo( id INTEGER NOT NULL, name VARCHAR(20) NULL, bar VARCHAR(33) COLLATE CASE SENSITIVE, PRIMARY KEY (id) )" );
                statement.executeUpdate( "CREATE TABLE test.foo2( id INTEGER NOT NULL, name VARCHAR(20) NOT NULL, foobar VARCHAR(33) NULL, PRIMARY KEY (id, name) )" );
                statement.executeUpdate( "ALTER TABLE test.foo2 ADD CONSTRAINT u_foo1 UNIQUE (name, foobar)" );
                statement.executeUpdate( "ALTER TABLE foo ADD CONSTRAINT fk_foo_1 FOREIGN KEY (name, bar) REFERENCES test.foo2(name, foobar) ON UPDATE RESTRICT ON DELETE RESTRICT" );
                statement.executeUpdate( "ALTER TABLE test.foo2 ADD CONSTRAINT fk_foo_2 FOREIGN KEY (id) REFERENCES public.foo(id)" );
                statement.executeUpdate( "ALTER TABLE foo ADD UNIQUE INDEX i_foo ON id ON STORE hsqldb" );
                statement.executeUpdate( "ALTER TABLE test.foo2 ADD INDEX i_foo2 ON (name, foobar) USING \"default\" ON STORE hsqldb" );
                statement.executeUpdate( "CREATE DOCUMENT SCHEMA doc" );
                connection.commit();
            }
        }
    }


    @AfterClass
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER TABLE test.foo2 DROP FOREIGN KEY fk_foo_2 " );
                statement.executeUpdate( "DROP TABLE foo" );
                statement.executeUpdate( "DROP SCHEMA test" );
                statement.executeUpdate( "DROP SCHEMA doc" );
                connection.commit();
            }
        }
    }

    // --------------- Tests ---------------


    @Test
    public void testMetaGetTables() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getTables( null, null, null, null );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 11, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TABLE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_SCHEM", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_NAME", rsmd.getColumnName( 3 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_TYPE", rsmd.getColumnName( 4 ) );
            Assert.assertEquals( "Wrong column name", "REMARKS", rsmd.getColumnName( 5 ) );
            Assert.assertEquals( "Wrong column name", "TYPE_CAT", rsmd.getColumnName( 6 ) );
            Assert.assertEquals( "Wrong column name", "TYPE_SCHEM", rsmd.getColumnName( 7 ) );
            Assert.assertEquals( "Wrong column name", "TYPE_NAME", rsmd.getColumnName( 8 ) );
            Assert.assertEquals( "Wrong column name", "SELF_REFERENCING_COL_NAME", rsmd.getColumnName( 9 ) );
            Assert.assertEquals( "Wrong column name", "REF_GENERATION", rsmd.getColumnName( 10 ) );
            Assert.assertEquals( "Wrong column name", "OWNER", rsmd.getColumnName( 11 ) );

            // Check data
            final Object[] tableFoo = new Object[]{ "APP", "public", "foo", "ENTITY", "", null, null, null, null, null, "pa" };
            final Object[] tableFoo2 = new Object[]{ "APP", "test", "foo2", "ENTITY", "", null, null, null, null, null, "pa" };
            TestHelper.checkResultSet(
                    connection.getMetaData().getTables( "APP", null, "foo", null ),
                    ImmutableList.of( tableFoo ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getTables( "AP_", "%", "foo2", null ),
                    ImmutableList.of( tableFoo2 ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getTables( null, null, "foo%", null ),
                    ImmutableList.of( tableFoo, tableFoo2 ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getTables( "%", "test", "%", null ),
                    ImmutableList.of( tableFoo2 ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getTables( "%", "tes_", "foo_", null ),
                    ImmutableList.of( tableFoo2 ) );
        }
    }


    @Test
    public void testMetaGetColumns() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getColumns( null, null, null, null );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 19, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TABLE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_SCHEM", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_NAME", rsmd.getColumnName( 3 ) );
            Assert.assertEquals( "Wrong column name", "COLUMN_NAME", rsmd.getColumnName( 4 ) );
            Assert.assertEquals( "Wrong column name", "DATA_TYPE", rsmd.getColumnName( 5 ) );
            Assert.assertEquals( "Wrong column name", "TYPE_NAME", rsmd.getColumnName( 6 ) );
            Assert.assertEquals( "Wrong column name", "COLUMN_SIZE", rsmd.getColumnName( 7 ) );
            Assert.assertEquals( "Wrong column name", "BUFFER_LENGTH", rsmd.getColumnName( 8 ) );
            Assert.assertEquals( "Wrong column name", "DECIMAL_DIGITS", rsmd.getColumnName( 9 ) );
            Assert.assertEquals( "Wrong column name", "NUM_PREC_RADIX", rsmd.getColumnName( 10 ) );
            Assert.assertEquals( "Wrong column name", "NULLABLE", rsmd.getColumnName( 11 ) );
            Assert.assertEquals( "Wrong column name", "REMARKS", rsmd.getColumnName( 12 ) );
            Assert.assertEquals( "Wrong column name", "COLUMN_DEF", rsmd.getColumnName( 13 ) );
            Assert.assertEquals( "Wrong column name", "SQL_DATA_TYPE", rsmd.getColumnName( 14 ) );
            Assert.assertEquals( "Wrong column name", "SQL_DATETIME_SUB", rsmd.getColumnName( 15 ) );
            Assert.assertEquals( "Wrong column name", "CHAR_OCTET_LENGTH", rsmd.getColumnName( 16 ) );
            Assert.assertEquals( "Wrong column name", "ORDINAL_POSITION", rsmd.getColumnName( 17 ) );
            Assert.assertEquals( "Wrong column name", "IS_NULLABLE", rsmd.getColumnName( 18 ) );
            Assert.assertEquals( "Wrong column name", "COLLATION", rsmd.getColumnName( 19 ) );

            // Check data
            final Object[] columnId = new Object[]{ "APP", "public", "foo", "id", 4, "INTEGER", null, null, null, null, 0, "", null, null, null, null, 1, "NO", null };
            final Object[] columnName = new Object[]{ "APP", "public", "foo", "name", 12, "VARCHAR", 20, null, null, null, 1, "", null, null, null, null, 2, "YES", "CASE_INSENSITIVE" };
            final Object[] columnBar = new Object[]{ "APP", "public", "foo", "bar", 12, "VARCHAR", 33, null, null, null, 1, "", null, null, null, null, 3, "YES", "CASE_SENSITIVE" };
            TestHelper.checkResultSet(
                    connection.getMetaData().getColumns( "APP", null, "foo", null ),
                    ImmutableList.of( columnId, columnName, columnBar ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getColumns( "APP", null, "foo", "id" ),
                    ImmutableList.of( columnId ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getColumns( "APP", null, "foo", "id%" ),
                    ImmutableList.of( columnId ) );
        }
    }


    @Test
    public void testMetaGetSchemas() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getSchemas( null, null );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 4, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TABLE_SCHEM", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_CATALOG", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "OWNER", rsmd.getColumnName( 3 ) );
            Assert.assertEquals( "Wrong column name", "SCHEMA_TYPE", rsmd.getColumnName( 4 ) );

            // Check data
            final Object[] schemaPublic = new Object[]{ "public", "APP", "system", "RELATIONAL" };
            final Object[] schemaDoc = new Object[]{ "doc", "APP", "pa", "DOCUMENT" };
            final Object[] schemaTest = new Object[]{ "test", "APP", "pa", "RELATIONAL" };

            TestHelper.checkResultSet(
                    connection.getMetaData().getSchemas( "APP", null ),
                    ImmutableList.of( schemaPublic, schemaTest, schemaDoc ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getSchemas( "%", "%" ),
                    ImmutableList.of( schemaPublic, schemaTest, schemaDoc ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getSchemas( "APP", "test" ),
                    ImmutableList.of( schemaTest ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getSchemas( null, "public" ),
                    ImmutableList.of( schemaPublic ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getSchemas( "AP_", "pub%" ),
                    ImmutableList.of( schemaPublic ) );
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
            Assert.assertEquals( "Wrong number of columns", 3, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TABLE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "OWNER", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "DEFAULT_SCHEMA", rsmd.getColumnName( 3 ) );

            // Check data
            final Object[] databaseApp = new Object[]{ "APP", "system", "public" };

            TestHelper.checkResultSet(
                    connection.getMetaData().getCatalogs(),
                    ImmutableList.of( databaseApp ) );
        }
    }


    @Test
    public void testGetTableTypes() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getTableTypes();
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 1, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TABLE_TYPE", rsmd.getColumnName( 1 ) );

            // Check data
            final List<Object[]> tableTypeTable = ImmutableList.of( new Object[]{ "ENTITY" }, new Object[]{ "SOURCE" }, new Object[]{ "VIEW" }, new Object[]{ "MATERIALIZED_VIEW" } );

            TestHelper.checkResultSet(
                    connection.getMetaData().getTableTypes(),
                    tableTypeTable );
        }
    }


    @Test
    public void testGetPrimaryKeys() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getPrimaryKeys( null, null, null );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 6, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TABLE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_SCHEM", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_NAME", rsmd.getColumnName( 3 ) );
            Assert.assertEquals( "Wrong column name", "COLUMN_NAME", rsmd.getColumnName( 4 ) );
            Assert.assertEquals( "Wrong column name", "KEY_SEQ", rsmd.getColumnName( 5 ) );
            Assert.assertEquals( "Wrong column name", "PK_NAME", rsmd.getColumnName( 6 ) );

            // Check data
            final Object[] primaryKey = new Object[]{ "APP", "public", "foo", "id", 1, null };
            final Object[] compositePrimaryKey1 = new Object[]{ "APP", "test", "foo2", "id", 1, null };
            final Object[] compositePrimaryKey2 = new Object[]{ "APP", "test", "foo2", "name", 2, null };

            TestHelper.checkResultSet(
                    connection.getMetaData().getPrimaryKeys( "APP", "public", "foo" ),
                    ImmutableList.of( primaryKey ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getPrimaryKeys( "APP", "test", "%" ),
                    ImmutableList.of( compositePrimaryKey1, compositePrimaryKey2 ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getPrimaryKeys( "AP%", "test", null ),
                    ImmutableList.of( compositePrimaryKey1, compositePrimaryKey2 ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getPrimaryKeys( "AP_", "t%", null ),
                    ImmutableList.of( compositePrimaryKey1, compositePrimaryKey2 ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getPrimaryKeys( null, "t___", null ),
                    ImmutableList.of( compositePrimaryKey1, compositePrimaryKey2 ) );
        }
    }


    @Test
    public void testGetImportedKeys() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getImportedKeys( null, null, null );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 14, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "PKTABLE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "PKTABLE_SCHEM", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "PKTABLE_NAME", rsmd.getColumnName( 3 ) );
            Assert.assertEquals( "Wrong column name", "PKCOLUMN_NAME", rsmd.getColumnName( 4 ) );
            Assert.assertEquals( "Wrong column name", "FKTABLE_CAT", rsmd.getColumnName( 5 ) );
            Assert.assertEquals( "Wrong column name", "FKTABLE_SCHEM", rsmd.getColumnName( 6 ) );
            Assert.assertEquals( "Wrong column name", "FKTABLE_NAME", rsmd.getColumnName( 7 ) );
            Assert.assertEquals( "Wrong column name", "FKCOLUMN_NAME", rsmd.getColumnName( 8 ) );
            Assert.assertEquals( "Wrong column name", "KEY_SEQ", rsmd.getColumnName( 9 ) );
            Assert.assertEquals( "Wrong column name", "UPDATE_RULE", rsmd.getColumnName( 10 ) );
            Assert.assertEquals( "Wrong column name", "DELETE_RULE", rsmd.getColumnName( 11 ) );
            Assert.assertEquals( "Wrong column name", "FK_NAME", rsmd.getColumnName( 12 ) );
            Assert.assertEquals( "Wrong column name", "PK_NAME", rsmd.getColumnName( 13 ) );
            Assert.assertEquals( "Wrong column name", "DEFERRABILITY", rsmd.getColumnName( 14 ) );

            // Check data
            final Object[] foreignKey1a = new Object[]{ "APP", "test", "foo2", "name", "APP", "public", "foo", "name", 1, 1, 1, "fk_foo_1", null, null };
            final Object[] foreignKey1b = new Object[]{ "APP", "test", "foo2", "foobar", "APP", "public", "foo", "bar", 2, 1, 1, "fk_foo_1", null, null };
            final Object[] foreignKey2 = new Object[]{ "APP", "public", "foo", "id", "APP", "test", "foo2", "id", 1, 1, 1, "fk_foo_2", null, null };

            TestHelper.checkResultSet(
                    connection.getMetaData().getImportedKeys( "APP", "public", "foo" ),
                    ImmutableList.of( foreignKey1a, foreignKey1b ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getImportedKeys( "%", "te%", "foo2" ),
                    ImmutableList.of( foreignKey2 ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getImportedKeys( "AP_", null, "%" ),
                    ImmutableList.of( foreignKey1a, foreignKey1b, foreignKey2 ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getImportedKeys( null, null, null ),
                    ImmutableList.of( foreignKey1a, foreignKey1b, foreignKey2 ) );

        }
    }


    @Test
    public void testGetExportedKeys() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getExportedKeys( null, null, null );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 14, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "PKTABLE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "PKTABLE_SCHEM", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "PKTABLE_NAME", rsmd.getColumnName( 3 ) );
            Assert.assertEquals( "Wrong column name", "PKCOLUMN_NAME", rsmd.getColumnName( 4 ) );
            Assert.assertEquals( "Wrong column name", "FKTABLE_CAT", rsmd.getColumnName( 5 ) );
            Assert.assertEquals( "Wrong column name", "FKTABLE_SCHEM", rsmd.getColumnName( 6 ) );
            Assert.assertEquals( "Wrong column name", "FKTABLE_NAME", rsmd.getColumnName( 7 ) );
            Assert.assertEquals( "Wrong column name", "FKCOLUMN_NAME", rsmd.getColumnName( 8 ) );
            Assert.assertEquals( "Wrong column name", "KEY_SEQ", rsmd.getColumnName( 9 ) );
            Assert.assertEquals( "Wrong column name", "UPDATE_RULE", rsmd.getColumnName( 10 ) );
            Assert.assertEquals( "Wrong column name", "DELETE_RULE", rsmd.getColumnName( 11 ) );
            Assert.assertEquals( "Wrong column name", "FK_NAME", rsmd.getColumnName( 12 ) );
            Assert.assertEquals( "Wrong column name", "PK_NAME", rsmd.getColumnName( 13 ) );
            Assert.assertEquals( "Wrong column name", "DEFERRABILITY", rsmd.getColumnName( 14 ) );

            // Check data
            final Object[] foreignKey1a = new Object[]{ "APP", "test", "foo2", "name", "APP", "public", "foo", "name", 1, 1, 1, "fk_foo_1", null, null };
            final Object[] foreignKey1b = new Object[]{ "APP", "test", "foo2", "foobar", "APP", "public", "foo", "bar", 2, 1, 1, "fk_foo_1", null, null };
            final Object[] foreignKey2 = new Object[]{ "APP", "public", "foo", "id", "APP", "test", "foo2", "id", 1, 1, 1, "fk_foo_2", null, null };

            TestHelper.checkResultSet(
                    connection.getMetaData().getExportedKeys( "APP", "public", "foo" ),
                    ImmutableList.of( foreignKey2 ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getExportedKeys( "%", "te%", "foo2" ),
                    ImmutableList.of( foreignKey1a, foreignKey1b ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getExportedKeys( "AP_", null, "%" ),
                    ImmutableList.of( foreignKey2, foreignKey1a, foreignKey1b ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getExportedKeys( null, null, null ),
                    ImmutableList.of( foreignKey2, foreignKey1a, foreignKey1b ) );

        }
    }


    @Test
    public void testGetTypeInfo() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getTypeInfo();
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 18, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TYPE_NAME", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "DATA_TYPE", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "PRECISION", rsmd.getColumnName( 3 ) );
            Assert.assertEquals( "Wrong column name", "LITERAL_PREFIX", rsmd.getColumnName( 4 ) );
            Assert.assertEquals( "Wrong column name", "LITERAL_SUFFIX", rsmd.getColumnName( 5 ) );
            Assert.assertEquals( "Wrong column name", "CREATE_PARAMS", rsmd.getColumnName( 6 ) );
            Assert.assertEquals( "Wrong column name", "NULLABLE", rsmd.getColumnName( 7 ) );
            Assert.assertEquals( "Wrong column name", "CASE_SENSITIVE", rsmd.getColumnName( 8 ) );
            Assert.assertEquals( "Wrong column name", "SEARCHABLE", rsmd.getColumnName( 9 ) );
            Assert.assertEquals( "Wrong column name", "UNSIGNED_ATTRIBUTE", rsmd.getColumnName( 10 ) );
            Assert.assertEquals( "Wrong column name", "FIXED_PREC_SCALE", rsmd.getColumnName( 11 ) );
            Assert.assertEquals( "Wrong column name", "AUTO_INCREMENT", rsmd.getColumnName( 12 ) );
            Assert.assertEquals( "Wrong column name", "LOCAL_TYPE_NAME", rsmd.getColumnName( 13 ) );
            Assert.assertEquals( "Wrong column name", "MINIMUM_SCALE", rsmd.getColumnName( 14 ) );
            Assert.assertEquals( "Wrong column name", "MAXIMUM_SCALE", rsmd.getColumnName( 15 ) );
            Assert.assertEquals( "Wrong column name", "SQL_DATA_TYPE", rsmd.getColumnName( 16 ) );
            Assert.assertEquals( "Wrong column name", "SQL_DATETIME_SUB", rsmd.getColumnName( 17 ) );
            Assert.assertEquals( "Wrong column name", "NUM_PREC_RADIX", rsmd.getColumnName( 18 ) );
        }
    }


    @Test
    public void testGetIndexInfo() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getIndexInfo( null, null, null, false, false );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 15, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TABLE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_SCHEM", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_NAME", rsmd.getColumnName( 3 ) );
            Assert.assertEquals( "Wrong column name", "NON_UNIQUE", rsmd.getColumnName( 4 ) );
            Assert.assertEquals( "Wrong column name", "INDEX_QUALIFIER", rsmd.getColumnName( 5 ) );
            Assert.assertEquals( "Wrong column name", "INDEX_NAME", rsmd.getColumnName( 6 ) );
            Assert.assertEquals( "Wrong column name", "TYPE", rsmd.getColumnName( 7 ) );
            Assert.assertEquals( "Wrong column name", "ORDINAL_POSITION", rsmd.getColumnName( 8 ) );
            Assert.assertEquals( "Wrong column name", "COLUMN_NAME", rsmd.getColumnName( 9 ) );
            Assert.assertEquals( "Wrong column name", "ASC_OR_DESC", rsmd.getColumnName( 10 ) );
            Assert.assertEquals( "Wrong column name", "CARDINALITY", rsmd.getColumnName( 11 ) );
            Assert.assertEquals( "Wrong column name", "PAGES", rsmd.getColumnName( 12 ) );
            Assert.assertEquals( "Wrong column name", "FILTER_CONDITION", rsmd.getColumnName( 13 ) );
            Assert.assertEquals( "Wrong column name", "LOCATION", rsmd.getColumnName( 14 ) );
            Assert.assertEquals( "Wrong column name", "INDEX_TYPE", rsmd.getColumnName( 15 ) );

            // Check data
            final Object[] index1 = new Object[]{ "APP", "public", "foo", false, null, "i_foo", 0, 1, "id", null, -1, null, null, 1, 1 };
            final Object[] index2a = new Object[]{ "APP", "test", "foo2", true, null, "i_foo2", 0, 1, "name", null, -1, null, null, 1, 1 };
            final Object[] index2b = new Object[]{ "APP", "test", "foo2", true, null, "i_foo2", 0, 2, "foobar", null, -1, null, null, 1, 1 };

            TestHelper.checkResultSet(
                    connection.getMetaData().getIndexInfo( "APP", "public", "foo", false, false ),
                    ImmutableList.of( index1 ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getIndexInfo( "AP_", "tes_", "foo_", false, false ),
                    ImmutableList.of( index2a, index2b ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getIndexInfo( "%", "%", "%", false, false ),
                    ImmutableList.of( index1, index2a, index2b ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getIndexInfo( null, null, null, false, false ),
                    ImmutableList.of( index1, index2a, index2b ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getIndexInfo( null, "%", null, true, false ),
                    ImmutableList.of( index1 ) );
        }
    }

}
