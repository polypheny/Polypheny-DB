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

package org.polypheny.db.jdbc;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
public class JdbcMetaTest {


    private static TestHelper helper;


    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        helper = TestHelper.getInstance();
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

                if ( helper.storeSupportsIndex() ) {
                    statement.executeUpdate( "ALTER TABLE foo ADD UNIQUE INDEX i_foo ON id ON STORE hsqldb" );
                    statement.executeUpdate( "ALTER TABLE test.foo2 ADD INDEX i_foo2 ON (name, foobar) USING \"default\" ON STORE hsqldb" );
                }

                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER TABLE test.foo2 DROP FOREIGN KEY fk_foo_2 " );
                statement.executeUpdate( "DROP TABLE foo" );
                statement.executeUpdate( "DROP SCHEMA test" ); // todo There should be an alias to use the SQL default term SCHEMA instead of NAMESPACE
                connection.commit();
            }
        }
    }

    // --------------- Tests ---------------


    @SuppressWarnings("SqlSourceToSinkFlow")
    @ParameterizedTest(name = "Namespace creation using keyword: {0}")
    @ValueSource(strings = { "SCHEMA", "NAMESPACE" })
    public void testNamespaceCreation( String name ) throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( String.format( "CREATE %s namespacetest", name ) );
                statement.executeUpdate( String.format( "DROP %s namespacetest", name ) );
                statement.executeUpdate( String.format( "CREATE GRAPH %s namespacetest", name ) );
                statement.executeUpdate( String.format( "DROP %s namespacetest", name ) );
                statement.executeUpdate( String.format( "CREATE DOCUMENT %s namespacetest", name ) );
                statement.executeUpdate( String.format( "DROP %s namespacetest", name ) );
                connection.commit();
            }
        }
    }


    @Test
    public void testMetaGetTables() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getTables( null, null, null, null );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            assertEquals( 11, totalColumns, "Wrong number of columns" );

            // Check column names
            assertEquals( "TABLE_CAT", rsmd.getColumnName( 1 ), "Wrong column name" );
            assertEquals( "TABLE_SCHEM", rsmd.getColumnName( 2 ), "Wrong column name" );
            assertEquals( "TABLE_NAME", rsmd.getColumnName( 3 ), "Wrong column name" );
            assertEquals( "TABLE_TYPE", rsmd.getColumnName( 4 ), "Wrong column name" );
            assertEquals( "REMARKS", rsmd.getColumnName( 5 ), "Wrong column name" );
            assertEquals( "TYPE_CAT", rsmd.getColumnName( 6 ), "Wrong column name" );
            assertEquals( "TYPE_SCHEM", rsmd.getColumnName( 7 ), "Wrong column name" );
            assertEquals( "TYPE_NAME", rsmd.getColumnName( 8 ), "Wrong column name" );
            assertEquals( "SELF_REFERENCING_COL_NAME", rsmd.getColumnName( 9 ), "Wrong column name" );
            assertEquals( "REF_GENERATION", rsmd.getColumnName( 10 ), "Wrong column name" );
            assertEquals( "OWNER", rsmd.getColumnName( 11 ), "Wrong column name" );

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
                    ImmutableList.of( tableFoo, tableFoo2 ), true );
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
            assertEquals( 19, totalColumns, "Wrong number of columns" );

            // Check column names
            assertEquals( "TABLE_CAT", rsmd.getColumnName( 1 ) );
            assertEquals( "TABLE_SCHEM", rsmd.getColumnName( 2 ) );
            assertEquals( "TABLE_NAME", rsmd.getColumnName( 3 ) );
            assertEquals( "COLUMN_NAME", rsmd.getColumnName( 4 ) );
            assertEquals( "DATA_TYPE", rsmd.getColumnName( 5 ) );
            assertEquals( "TYPE_NAME", rsmd.getColumnName( 6 ) );
            assertEquals( "COLUMN_SIZE", rsmd.getColumnName( 7 ) );
            assertEquals( "BUFFER_LENGTH", rsmd.getColumnName( 8 ) );
            assertEquals( "DECIMAL_DIGITS", rsmd.getColumnName( 9 ) );
            assertEquals( "NUM_PREC_RADIX", rsmd.getColumnName( 10 ) );
            assertEquals( "NULLABLE", rsmd.getColumnName( 11 ) );
            assertEquals( "REMARKS", rsmd.getColumnName( 12 ) );
            assertEquals( "COLUMN_DEF", rsmd.getColumnName( 13 ) );
            assertEquals( "SQL_DATA_TYPE", rsmd.getColumnName( 14 ) );
            assertEquals( "SQL_DATETIME_SUB", rsmd.getColumnName( 15 ) );
            assertEquals( "CHAR_OCTET_LENGTH", rsmd.getColumnName( 16 ) );
            assertEquals( "ORDINAL_POSITION", rsmd.getColumnName( 17 ) );
            assertEquals( "IS_NULLABLE", rsmd.getColumnName( 18 ) );
            assertEquals( "COLLATION", rsmd.getColumnName( 19 ) );

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
            assertEquals( 4, totalColumns, "Wrong number of columns" );

            // Check column names
            assertEquals( "TABLE_SCHEM", rsmd.getColumnName( 1 ) );
            assertEquals( "TABLE_CATALOG", rsmd.getColumnName( 2 ) );
            assertEquals( "OWNER", rsmd.getColumnName( 3 ) );
            assertEquals( "SCHEMA_TYPE", rsmd.getColumnName( 4 ) );

            // Check data
            final Object[] schemaPublic = new Object[]{ "public", "APP", "pa", "RELATIONAL" };
            //final Object[] schemaDoc = new Object[]{ "doc", "APP", "pa", "DOCUMENT" };
            final Object[] schemaTest = new Object[]{ "test", "APP", "pa", "RELATIONAL" };

            TestHelper.checkResultSet(
                    connection.getMetaData().getSchemas( "APP", null ),
                    ImmutableList.of( schemaPublic, schemaTest ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getSchemas( "%", "%" ),
                    ImmutableList.of( schemaPublic, schemaTest ) );
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
            assertEquals( 3, totalColumns, "Wrong number of columns" );

            // Check column names
            assertEquals( "TABLE_CAT", rsmd.getColumnName( 1 ) );
            assertEquals( "OWNER", rsmd.getColumnName( 2 ) );
            assertEquals( "DEFAULT_SCHEMA", rsmd.getColumnName( 3 ) );

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
            assertEquals( 1, totalColumns, "Wrong number of columns" );

            // Check column names
            assertEquals( "TABLE_TYPE", rsmd.getColumnName( 1 ) );

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
            assertEquals( 6, totalColumns, "Wrong number of columns" );

            // Check column names
            assertEquals( "TABLE_CAT", rsmd.getColumnName( 1 ) );
            assertEquals( "TABLE_SCHEM", rsmd.getColumnName( 2 ) );
            assertEquals( "TABLE_NAME", rsmd.getColumnName( 3 ) );
            assertEquals( "COLUMN_NAME", rsmd.getColumnName( 4 ) );
            assertEquals( "KEY_SEQ", rsmd.getColumnName( 5 ) );
            assertEquals( "PK_NAME", rsmd.getColumnName( 6 ) );

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
            assertEquals( 14, totalColumns, "Wrong number of columns" );

            // Check column names
            assertEquals( "PKTABLE_CAT", rsmd.getColumnName( 1 ) );
            assertEquals( "PKTABLE_SCHEM", rsmd.getColumnName( 2 ) );
            assertEquals( "PKTABLE_NAME", rsmd.getColumnName( 3 ) );
            assertEquals( "PKCOLUMN_NAME", rsmd.getColumnName( 4 ) );
            assertEquals( "FKTABLE_CAT", rsmd.getColumnName( 5 ) );
            assertEquals( "FKTABLE_SCHEM", rsmd.getColumnName( 6 ) );
            assertEquals( "FKTABLE_NAME", rsmd.getColumnName( 7 ) );
            assertEquals( "FKCOLUMN_NAME", rsmd.getColumnName( 8 ) );
            assertEquals( "KEY_SEQ", rsmd.getColumnName( 9 ) );
            assertEquals( "UPDATE_RULE", rsmd.getColumnName( 10 ) );
            assertEquals( "DELETE_RULE", rsmd.getColumnName( 11 ) );
            assertEquals( "FK_NAME", rsmd.getColumnName( 12 ) );
            assertEquals( "PK_NAME", rsmd.getColumnName( 13 ) );
            assertEquals( "DEFERRABILITY", rsmd.getColumnName( 14 ) );

            // Check data
            final Object[] foreignKey1a = new Object[]{ "APP", "test", "foo2", "name", "APP", "public", "foo", "name", 1, 1, 1, "fk_foo_1", null, null };
            final Object[] foreignKey1b = new Object[]{ "APP", "test", "foo2", "foobar", "APP", "public", "foo", "bar", 2, 1, 1, "fk_foo_1", null, null };
            final Object[] foreignKey2 = new Object[]{ "APP", "public", "foo", "id", "APP", "test", "foo2", "id", 1, 1, 1, "fk_foo_2", null, null };

            TestHelper.checkResultSet(
                    connection.getMetaData().getImportedKeys( "APP", "public", "foo" ),
                    ImmutableList.of( foreignKey1a, foreignKey1b ), true );
            TestHelper.checkResultSet(
                    connection.getMetaData().getImportedKeys( "%", "te%", "foo2" ),
                    ImmutableList.of( foreignKey2 ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getImportedKeys( "AP_", null, "%" ),
                    ImmutableList.of( foreignKey1a, foreignKey1b, foreignKey2 ), true );
            TestHelper.checkResultSet(
                    connection.getMetaData().getImportedKeys( null, null, null ),
                    ImmutableList.of( foreignKey1a, foreignKey1b, foreignKey2 ), true );

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
            assertEquals( 14, totalColumns, "Wrong number of columns" );

            // Check column names
            assertEquals( "PKTABLE_CAT", rsmd.getColumnName( 1 ) );
            assertEquals( "PKTABLE_SCHEM", rsmd.getColumnName( 2 ) );
            assertEquals( "PKTABLE_NAME", rsmd.getColumnName( 3 ) );
            assertEquals( "PKCOLUMN_NAME", rsmd.getColumnName( 4 ) );
            assertEquals( "FKTABLE_CAT", rsmd.getColumnName( 5 ) );
            assertEquals( "FKTABLE_SCHEM", rsmd.getColumnName( 6 ) );
            assertEquals( "FKTABLE_NAME", rsmd.getColumnName( 7 ) );
            assertEquals( "FKCOLUMN_NAME", rsmd.getColumnName( 8 ) );
            assertEquals( "KEY_SEQ", rsmd.getColumnName( 9 ) );
            assertEquals( "UPDATE_RULE", rsmd.getColumnName( 10 ) );
            assertEquals( "DELETE_RULE", rsmd.getColumnName( 11 ) );
            assertEquals( "FK_NAME", rsmd.getColumnName( 12 ) );
            assertEquals( "PK_NAME", rsmd.getColumnName( 13 ) );
            assertEquals( "DEFERRABILITY", rsmd.getColumnName( 14 ) );

            // Check data
            final Object[] foreignKey1a = new Object[]{ "APP", "test", "foo2", "name", "APP", "public", "foo", "name", 1, 1, 1, "fk_foo_1", null, null };
            final Object[] foreignKey1b = new Object[]{ "APP", "test", "foo2", "foobar", "APP", "public", "foo", "bar", 2, 1, 1, "fk_foo_1", null, null };
            final Object[] foreignKey2 = new Object[]{ "APP", "public", "foo", "id", "APP", "test", "foo2", "id", 1, 1, 1, "fk_foo_2", null, null };

            TestHelper.checkResultSet(
                    connection.getMetaData().getExportedKeys( "APP", "public", "foo" ),
                    ImmutableList.of( foreignKey2 ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getExportedKeys( "%", "te%", "foo2" ),
                    ImmutableList.of( foreignKey1a, foreignKey1b ), true );
            TestHelper.checkResultSet(
                    connection.getMetaData().getExportedKeys( "AP_", null, "%" ),
                    ImmutableList.of( foreignKey2, foreignKey1a, foreignKey1b ), true );
            TestHelper.checkResultSet(
                    connection.getMetaData().getExportedKeys( null, null, null ),
                    ImmutableList.of( foreignKey2, foreignKey1a, foreignKey1b ), true );

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
            assertEquals( 18, totalColumns, "Wrong number of columns" );

            // Check column names
            assertEquals( "TYPE_NAME", rsmd.getColumnName( 1 ) );
            assertEquals( "DATA_TYPE", rsmd.getColumnName( 2 ) );
            assertEquals( "PRECISION", rsmd.getColumnName( 3 ) );
            assertEquals( "LITERAL_PREFIX", rsmd.getColumnName( 4 ) );
            assertEquals( "LITERAL_SUFFIX", rsmd.getColumnName( 5 ) );
            assertEquals( "CREATE_PARAMS", rsmd.getColumnName( 6 ) );
            assertEquals( "NULLABLE", rsmd.getColumnName( 7 ) );
            assertEquals( "CASE_SENSITIVE", rsmd.getColumnName( 8 ) );
            assertEquals( "SEARCHABLE", rsmd.getColumnName( 9 ) );
            assertEquals( "UNSIGNED_ATTRIBUTE", rsmd.getColumnName( 10 ) );
            assertEquals( "FIXED_PREC_SCALE", rsmd.getColumnName( 11 ) );
            assertEquals( "AUTO_INCREMENT", rsmd.getColumnName( 12 ) );
            assertEquals( "LOCAL_TYPE_NAME", rsmd.getColumnName( 13 ) );
            assertEquals( "MINIMUM_SCALE", rsmd.getColumnName( 14 ) );
            assertEquals( "MAXIMUM_SCALE", rsmd.getColumnName( 15 ) );
            assertEquals( "SQL_DATA_TYPE", rsmd.getColumnName( 16 ) );
            assertEquals( "SQL_DATETIME_SUB", rsmd.getColumnName( 17 ) );
            assertEquals( "NUM_PREC_RADIX", rsmd.getColumnName( 18 ) );
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
            assertEquals( 15, totalColumns, "Wrong number of columns" );

            // Check column names
            assertEquals( "TABLE_CAT", rsmd.getColumnName( 1 ) );
            assertEquals( "TABLE_SCHEM", rsmd.getColumnName( 2 ) );
            assertEquals( "TABLE_NAME", rsmd.getColumnName( 3 ) );
            assertEquals( "NON_UNIQUE", rsmd.getColumnName( 4 ) );
            assertEquals( "INDEX_QUALIFIER", rsmd.getColumnName( 5 ) );
            assertEquals( "INDEX_NAME", rsmd.getColumnName( 6 ) );
            assertEquals( "TYPE", rsmd.getColumnName( 7 ) );
            assertEquals( "ORDINAL_POSITION", rsmd.getColumnName( 8 ) );
            assertEquals( "COLUMN_NAME", rsmd.getColumnName( 9 ) );
            assertEquals( "ASC_OR_DESC", rsmd.getColumnName( 10 ) );
            assertEquals( "CARDINALITY", rsmd.getColumnName( 11 ) );
            assertEquals( "PAGES", rsmd.getColumnName( 12 ) );
            assertEquals( "FILTER_CONDITION", rsmd.getColumnName( 13 ) );
            assertEquals( "LOCATION", rsmd.getColumnName( 14 ) );
            assertEquals( "INDEX_TYPE", rsmd.getColumnName( 15 ) );

            // Check data
            final Object[] index1 = new Object[]{ "APP", "public", "foo", false, null, "i_foo", 0, 1, "id", null, -1, null, null, 0, 1 };
            final Object[] index2a = new Object[]{ "APP", "test", "foo2", true, null, "i_foo2", 0, 1, "name", null, -1, null, null, 0, 1 };
            final Object[] index2b = new Object[]{ "APP", "test", "foo2", true, null, "i_foo2", 0, 2, "foobar", null, -1, null, null, 0, 1 };

            if ( !helper.storeSupportsIndex() ) {
                return;
            }
            TestHelper.checkResultSet(
                    connection.getMetaData().getIndexInfo( "APP", "public", "foo", false, false ),
                    ImmutableList.of( index1 ) );
            TestHelper.checkResultSet(
                    connection.getMetaData().getIndexInfo( "AP_", "tes_", "foo_", false, false ),
                    ImmutableList.of( index2a, index2b ), true );
            TestHelper.checkResultSet(
                    connection.getMetaData().getIndexInfo( "%", "%", "%", false, false ),
                    ImmutableList.of( index1, index2a, index2b ), true );
            TestHelper.checkResultSet(
                    connection.getMetaData().getIndexInfo( null, null, null, false, false ),
                    ImmutableList.of( index1, index2a, index2b ), true );
            TestHelper.checkResultSet(
                    connection.getMetaData().getIndexInfo( null, "%", null, true, false ),
                    ImmutableList.of( index1 ) );
        }
    }

}
