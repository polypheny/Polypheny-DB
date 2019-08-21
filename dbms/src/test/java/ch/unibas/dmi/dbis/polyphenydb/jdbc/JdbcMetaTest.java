/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.PolyphenyDb;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(JUnit4.class)
public class JdbcMetaTest {

    private static final Logger LOG = LoggerFactory.getLogger( JdbcMetaTest.class );

    private static final PolyphenyDb polyphenyDb = new PolyphenyDb();

    @BeforeClass
    public static void setup() {
        LOG.info( "Starting Polypheny-DB..." );

        Runnable runnable = () -> {
            try {
                polyphenyDb.runPolyphenyDb();
            } catch ( GenericCatalogException | InstantiationException e ) {
                LOG.error( "Exception while starting Polypheny-DB", e );
            }
        };
        Thread thread = new Thread( runnable );
        thread.start();

        // Wait 10 seconds
        try {
            TimeUnit.SECONDS.sleep( 10 );
        } catch ( InterruptedException e ) {
            // Ignore
        }

        addTestData();
    }


    @SuppressWarnings({ "SqlNoDataSourceInspection", "SqlResolve" })
    private static void addTestData() {
        try ( PolyphenyDbConnection polyphenyDbConnection = new PolyphenyDbConnection() ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE SCHEMA test" );
                statement.executeUpdate( "CREATE TABLE foo( id INTEGER, name VARCHAR(20) NULL, bar VARCHAR(33) NOT NULL COLLATE CASE SENSITIVE, PRIMARY KEY (id) )" );
                statement.executeUpdate( "CREATE TABLE test.foo2( id INTEGER, name VARCHAR(20) NULL, foobar VARCHAR(33) NOT NULL, PRIMARY KEY (id, name) )" );
                statement.executeUpdate( "ALTER TABLE test.foo2 ADD CONSTRAINT u_foo1 UNIQUE (name, foobar)" );
                statement.executeUpdate( "ALTER TABLE foo ADD CONSTRAINT fk_foo_1 FOREIGN KEY (name, bar) REFERENCES test.foo2(name, foobar) ON UPDATE CASCADE ON DELETE SET NULL" );
                statement.executeUpdate( "ALTER TABLE test.foo2 ADD CONSTRAINT fk_foo_2 FOREIGN KEY (id) REFERENCES public.foo(id)" );
                statement.executeUpdate( "ALTER TABLE foo ADD UNIQUE INDEX i_foo ON id USING BTREE" );
                statement.executeUpdate( "ALTER TABLE test.foo2 ADD INDEX i_foo2 ON (name, foobar) USING HASH" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            LOG.error( "Exception while testing getTables()", e );
        }
    }

    @AfterClass
    public static void tearDown() {
        //LOG.info( "shutdown - closing DB connection" );
    }


    // --------------- Tests ---------------

    @Test
    public void testMetaGetTables() {
        try ( PolyphenyDbConnection polyphenyDbConnection = new PolyphenyDbConnection() ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getTables( null, null, null, null );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 12, totalColumns );

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
            Assert.assertEquals( "Wrong column name", "DEFINITION", rsmd.getColumnName( 12 ) );

            // Check data
            final Object[] tableFoo = new Object[]{ "APP", "public", "foo", "TABLE", "", null, null, null, null, null, "pa", null };
            final Object[] tableFoo2 = new Object[]{ "APP", "test", "foo2", "TABLE", "", null, null, null, null, null, "pa", null };
            checkResultSet(
                    connection.getMetaData().getTables( "APP", null, "foo", null ),
                    ImmutableList.of( tableFoo ) );
            checkResultSet(
                    connection.getMetaData().getTables( "AP_", "%", "foo2", null ),
                    ImmutableList.of( tableFoo2 ) );
            checkResultSet(
                    connection.getMetaData().getTables( null, null, "foo%", null ),
                    ImmutableList.of( tableFoo, tableFoo2 ) );
            checkResultSet(
                    connection.getMetaData().getTables( "%", "test", "%", null ),
                    ImmutableList.of( tableFoo2 ) );
            checkResultSet(
                    connection.getMetaData().getTables( "%", "tes_", "foo_", null ),
                    ImmutableList.of( tableFoo2 ) );
        } catch ( SQLException e ) {
            LOG.error( "Exception while testing getTables()", e );
        }
    }


    @Test
    public void testMetaGetColumns() {
        try ( PolyphenyDbConnection polyphenyDbConnection = new PolyphenyDbConnection() ) {
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
            final Object[] columnId = new Object[]{ "APP", "public", "foo", "id", 4, "INTEGER", null, null, null, null, 1, "", null, null, null, null, 1, "YES", null };
            final Object[] columnName = new Object[]{ "APP", "public", "foo", "name", 12, "VARCHAR", 20, null, null, null, 1, "", null, null, null, null, 2, "YES", "CASE_INSENSITIVE" };
            final Object[] columnBar = new Object[]{ "APP", "public", "foo", "bar", 12, "VARCHAR", 33, null, null, null, 0, "", null, null, null, null, 3, "NO", "CASE_SENSITIVE" };
            checkResultSet(
                    connection.getMetaData().getColumns( "APP", null, "foo", null ),
                    ImmutableList.of( columnId, columnName, columnBar ) );
            checkResultSet(
                    connection.getMetaData().getColumns( "APP", null, "foo", "id" ),
                    ImmutableList.of( columnId ) );
            checkResultSet(
                    connection.getMetaData().getColumns( "APP", null, "foo", "id%" ),
                    ImmutableList.of( columnId ) );
        } catch ( SQLException e ) {
            LOG.error( "Exception while testing getColumns()", e );
        }
    }


    @Test
    public void testMetaGetSchemas() {
        try ( PolyphenyDbConnection polyphenyDbConnection = new PolyphenyDbConnection() ) {
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
            final Object[] schemaTest = new Object[]{ "test", "APP", "pa", "RELATIONAL" };

            checkResultSet(
                    connection.getMetaData().getSchemas( "APP", null ),
                    ImmutableList.of( schemaPublic, schemaTest ) );
            checkResultSet(
                    connection.getMetaData().getSchemas( "%", "%" ),
                    ImmutableList.of( schemaPublic, schemaTest ) );
            checkResultSet(
                    connection.getMetaData().getSchemas( "APP", "test" ),
                    ImmutableList.of( schemaTest ) );
            checkResultSet(
                    connection.getMetaData().getSchemas( null, "public" ),
                    ImmutableList.of( schemaPublic ) );
            checkResultSet(
                    connection.getMetaData().getSchemas( "AP_", "pub%" ),
                    ImmutableList.of( schemaPublic ) );
        } catch ( SQLException e ) {
            LOG.error( "Exception while testing getSchemas()", e );
        }
    }


    @Test
    public void testGetCatalogs() {
        try ( PolyphenyDbConnection polyphenyDbConnection = new PolyphenyDbConnection() ) {
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
            LOG.error( "Exception while testing getCatalogs()", e );
        }
    }


    @Test
    public void testGetTableTypes() {
        try ( PolyphenyDbConnection polyphenyDbConnection = new PolyphenyDbConnection() ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getTableTypes();
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 1, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TABLE_TYPE", rsmd.getColumnName( 1 ) );

            // Check data
            final Object[] tableTypeTable = new Object[]{ "TABLE" };

            checkResultSet(
                    connection.getMetaData().getTableTypes(),
                    ImmutableList.of( tableTypeTable ) );
        } catch ( SQLException e ) {
            LOG.error( "Exception while testing getTableTypes()", e );
        }
    }


    @Test
    public void testGetPrimaryKeys() {
        try ( PolyphenyDbConnection polyphenyDbConnection = new PolyphenyDbConnection() ) {
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

            checkResultSet(
                    connection.getMetaData().getPrimaryKeys( "APP", "public", "foo" ),
                    ImmutableList.of( primaryKey ) );
            checkResultSet(
                    connection.getMetaData().getPrimaryKeys( "APP", "test", "%" ),
                    ImmutableList.of( compositePrimaryKey1, compositePrimaryKey2 ) );
            checkResultSet(
                    connection.getMetaData().getPrimaryKeys( "AP%", "test", null ),
                    ImmutableList.of( compositePrimaryKey1, compositePrimaryKey2 ) );
            checkResultSet(
                    connection.getMetaData().getPrimaryKeys( "AP_", "%", null ),
                    ImmutableList.of( primaryKey, compositePrimaryKey1, compositePrimaryKey2 ) );
            checkResultSet(
                    connection.getMetaData().getPrimaryKeys( null, null, null ),
                    ImmutableList.of( primaryKey, compositePrimaryKey1, compositePrimaryKey2 ) );
        } catch ( SQLException e ) {
            LOG.error( "Exception while testing getPrimaryKeys()", e );
        }
    }


    @Test
    public void testGetImportedKeys() {
        try ( PolyphenyDbConnection polyphenyDbConnection = new PolyphenyDbConnection() ) {
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
            final Object[] foreignKey1a = new Object[]{ "APP", "test", "foo2", "name", "APP", "public", "foo", "name", 1, 0, 2, "fk_foo_1", null, null };
            final Object[] foreignKey1b = new Object[]{ "APP", "test", "foo2", "foobar", "APP", "public", "foo", "bar", 2, 0, 2, "fk_foo_1", null, null };
            final Object[] foreignKey2 = new Object[]{ "APP", "public", "foo", "id", "APP", "test", "foo2", "id", 1, 1, 1, "fk_foo_2", null, null };

            checkResultSet(
                    connection.getMetaData().getImportedKeys( "APP", "public", "foo" ),
                    ImmutableList.of( foreignKey1a, foreignKey1b ) );
            checkResultSet(
                    connection.getMetaData().getImportedKeys( "%", "te%", "foo2" ),
                    ImmutableList.of( foreignKey2 ) );
            checkResultSet(
                    connection.getMetaData().getImportedKeys( "AP_", null, "%" ),
                    ImmutableList.of( foreignKey1a, foreignKey1b, foreignKey2 ) );
            checkResultSet(
                    connection.getMetaData().getImportedKeys( null, null, null ),
                    ImmutableList.of( foreignKey1a, foreignKey1b, foreignKey2 ) );

        } catch ( SQLException e ) {
            LOG.error( "Exception while testing getImportedKeys()", e );
        }
    }


    @Test
    public void testGetExportedKeys() {
        try ( PolyphenyDbConnection polyphenyDbConnection = new PolyphenyDbConnection() ) {
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
            final Object[] foreignKey1a = new Object[]{ "APP", "test", "foo2", "name", "APP", "public", "foo", "name", 1, 0, 2, "fk_foo_1", null, null };
            final Object[] foreignKey1b = new Object[]{ "APP", "test", "foo2", "foobar", "APP", "public", "foo", "bar", 2, 0, 2, "fk_foo_1", null, null };
            final Object[] foreignKey2 = new Object[]{ "APP", "public", "foo", "id", "APP", "test", "foo2", "id", 1, 1, 1, "fk_foo_2", null, null };

            checkResultSet(
                    connection.getMetaData().getExportedKeys( "APP", "public", "foo" ),
                    ImmutableList.of( foreignKey2 ) );
            checkResultSet(
                    connection.getMetaData().getExportedKeys( "%", "te%", "foo2" ),
                    ImmutableList.of( foreignKey1a, foreignKey1b ) );
            checkResultSet(
                    connection.getMetaData().getExportedKeys( "AP_", null, "%" ),
                    ImmutableList.of( foreignKey2, foreignKey1a, foreignKey1b ) );
            checkResultSet(
                    connection.getMetaData().getExportedKeys( null, null, null ),
                    ImmutableList.of( foreignKey2, foreignKey1a, foreignKey1b ) );

        } catch ( SQLException e ) {
            LOG.error( "Exception while testing getExportedKeys()", e );
        }
    }


    @Test
    public void testGetTypeInfo() {
        try ( PolyphenyDbConnection polyphenyDbConnection = new PolyphenyDbConnection() ) {
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

        } catch ( SQLException e ) {
            LOG.error( "Exception while testing getTypeInfo()", e );
        }
    }


    @Test
    public void testGetIndexInfo() {
        try ( PolyphenyDbConnection polyphenyDbConnection = new PolyphenyDbConnection() ) {
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
            final Object[] index1 = new Object[]{ "APP", "public", "foo", false, null, "i_foo", 0, 1, "id", null, -1, null, null, null, 1 };
            final Object[] index2a = new Object[]{ "APP", "test", "foo2", true, null, "i_foo2", 0, 1, "name", null, -1, null, null, null, 2 };
            final Object[] index2b = new Object[]{ "APP", "test", "foo2", true, null, "i_foo2", 0, 2, "foobar", null, -1, null, null, null, 2 };

            checkResultSet(
                    connection.getMetaData().getIndexInfo( "APP", "public", "foo", false, false ),
                    ImmutableList.of( index1 ) );
            checkResultSet(
                    connection.getMetaData().getIndexInfo( "AP_", "tes_", "foo_", false, false ),
                    ImmutableList.of( index2a, index2b ) );
            checkResultSet(
                    connection.getMetaData().getIndexInfo( "%", "%", "%", false, false ),
                    ImmutableList.of( index1, index2a, index2b ) );
            checkResultSet(
                    connection.getMetaData().getIndexInfo( null, null, null, false, false ),
                    ImmutableList.of( index1, index2a, index2b ) );
            checkResultSet(
                    connection.getMetaData().getIndexInfo( null, "%", null, true, false ),
                    ImmutableList.of( index1 ) );

        } catch ( SQLException e ) {
            LOG.error( "Exception while testing getIndexInfo()", e );
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
                Assert.assertEquals( "Unexpected data in column '" + resultSet.getMetaData().getColumnName( j + 1 ) + "'", expectedRow[j++], resultSet.getObject( j ) );
            }
        }
        Assert.assertEquals( "Wrong number of rows in the result set", expected.size(), i );
    }


    static class PolyphenyDbConnection implements AutoCloseable {

        private Connection conn;

        private final static String dbHost = "localhost";
        private final static int port = 20591;


        PolyphenyDbConnection() throws SQLException {
            try {
                Class.forName( "ch.unibas.dmi.dbis.polyphenydb.jdbc.Driver" );
            } catch ( ClassNotFoundException e ) {
                LOG.error( "Polypheny-DB Driver not found", e );
            }
            final String url = "jdbc:polypheny://" + dbHost + ":" + port;
            //String url = "jdbc:polypheny://" + dbHost + ":" + port + "/" + dbName + "?prepareThreshold=0";
            LOG.debug( "Connecting to database @ {}", url );

            Properties props = new Properties();
            props.setProperty( "user", "pa" );
            //props.setProperty( "password", password );
            //props.setProperty( "ssl", sslEnabled );
            props.setProperty( "wire_protocol", "PROTO3" );

            conn = DriverManager.getConnection( url, props );
            //conn.setAutoCommit( false );
        }


        Connection getConnection() {
            return conn;
        }


        @Override
        public void close() throws SQLException {
            conn.close();
        }
    }

}
