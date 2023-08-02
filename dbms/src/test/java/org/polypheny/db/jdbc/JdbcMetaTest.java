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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.LinkedList;
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

    private final static String dbHost = "localhost";
    private final static int port = 20590;


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
               // statement.executeUpdate( "CREATE DOCUMENT SCHEMA doc" ); // todo There should be an alias to use the SQL default term SCHEMA instead of NAMESPACE
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
                statement.executeUpdate( "DROP SCHEMA test" ); // todo There should be an alias to use the SQL default term SCHEMA instead of NAMESPACE
                //statement.executeUpdate( "DROP NAMESPACE doc" );
                connection.commit();
            }
        }
    }


    public Connection jdbcConnect( String url ) throws SQLException {
        log.debug( "Connecting to database @ {}", url );
        return DriverManager.getConnection( url );
    }


    // --------------- Tests ---------------
    @Test
    public void testNameWhatever() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection(); {
            DatabaseMetaData metadata = connection.getMetaData();
            //test goes here
        }
    }


    @Test
    public void testAllProceduresAreCallable() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( "All procedures should  be callable", metadata.allProceduresAreCallable() );
        }
    }


    @Test
    public void testAllTablesAreSelectable() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( "All tables should be selectable", metadata.allTablesAreSelectable() );
        }
    }


    @Test
    public void testNullsSortedHigh() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.nullsAreSortedHigh() );
        }
    }


    @Test
    public void testNullsSortedLow() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.nullsAreSortedLow() );
        }
    }


    @Test
    public void testNullsSortedStart() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.nullsAreSortedAtStart() );
        }
    }


    @Test
    public void testNullSortedEnd() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.nullsAreSortedAtEnd() );
        }
    }


    @Test
    public void testIsReadOnly() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.isReadOnly() );
        }
    }


    @Test
    public void testURL() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( "jdbc:polypheny://localhost:20590", metadata.getURL() );
        }
    }


    @Test
    public void testUserName() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( "pa", metadata.getUserName() );
        }
    }


    @Test
    public void testDriverName() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( "JDBC driver for PolyphenyDB", metadata.getDriverName() );
        }
    }


    @Test
    public void testUsesLocalFiles() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.usesLocalFiles() );
        }
    }


    @Test
    public void testUsesLocalFilePerTable() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.usesLocalFilePerTable() );
        }
    }


    @Test
    public void testMixedCaseIdentifiers() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsMixedCaseIdentifiers() );
        }
    }


    @Test
    public void testStoresUpperCaseIdentifiers() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.storesUpperCaseIdentifiers() );
        }
    }


    @Test
    public void testStoresLowerCaseIdentifiers() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.storesLowerCaseIdentifiers() );
        }
    }


    @Test
    public void testStoresMixedCaseIdentifiers() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.storesMixedCaseIdentifiers() );
        }
    }


    @Test
    public void testStoresUpperCaseQuotedIdentifiers() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.storesUpperCaseQuotedIdentifiers() );
        }
    }


    @Test
    public void testStoresLowerCaseQuotedIdentifiers() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.storesLowerCaseQuotedIdentifiers() );
        }
    }


    @Test
    public void testStoresMixedCaseQuotedIdentifiers() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.storesMixedCaseQuotedIdentifiers() );
        }
    }


    @Test
    public void testSupportsMixedCaseQuotedIdentifiers() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsMixedCaseQuotedIdentifiers() );
        }
    }


    @Test
    public void testIdentifierQuoteString() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            String expected = "\"";
            assertEquals( expected, metadata.getIdentifierQuoteString() );
        }
    }


    @Test
    public void testSQLNumericFuncs() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            String expected = "ABS, ACOS, ASIN, ATAN, ATAN2, CEILING, COS, COT, DEGREES, EXP, FLOOR, LOG, LOG10, MOD, PI, POWER, RADIANS, RAND, ROUND, SIGN, SIN, SQRT, TAN, TRUNCATE";
            assertEquals( expected, metadata.getNumericFunctions() );
        }
    }


    @Test
    public void testSQLStringFuncs() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            String expected = "ASCII, CHAR, CONCAT, DIFFERENCE, INSERT, LCASE, LEFT, LENGTH, LOCATE, LTRIM, REPEAT, REPLACE, RIGHT, RTRIM, SOUNDEX, SPACE, SUBSTRING, UCASE";

            assertEquals( expected, metadata.getStringFunctions() );
        }
    }


    @Test
    public void testSQLSystemFuncs() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            String expected = "CONVERT, DATABASE, IFNULL, USER";
            assertEquals( expected, metadata.getSystemFunctions() );
        }
    }


    @Test
    public void testSQLTimeDateFuncs() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            String expected = "CURDATE, CURTIME, DAYNAME, DAYOFMONTH, DAYOFWEEK, DAYOFYEAR, HOUR, MINUTE, MONTH, MONTHNAME, NOW, QUARTER, SECOND, TIMESTAMPADD, TIMESTAMPDIFF, WEEK, YEAR";
            assertEquals( expected, metadata.getTimeDateFunctions() );
        }
    }


    @Test
    public void testSearchStringEscape() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            String expected = "\\";
            assertEquals( expected, metadata.getSearchStringEscape() );
        }
    }


    @Test
    public void testExtraNameCharacters() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            String expected = "";
            assertEquals( expected, metadata.getExtraNameCharacters() );
        }
    }


    @Test
    public void testSupportsAlterTableWithAddColumn() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsAlterTableWithAddColumn() );
        }
    }


    @Test
    public void testSupportsAlterTableWithDropColumn() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsAlterTableWithDropColumn() );
        }
    }


    @Test
    public void testSupportsColumnAliasing() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsColumnAliasing() );
        }
    }


    @Test
    public void testNullPlusNullIsNullColumn() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.nullPlusNonNullIsNull() );
        }
    }


    @Test
    public void testSupportsConvert() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsConvert() );
        }
    }


    @Test
    public void testValidConversionNotSupported() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsConvert( JDBCType.INTEGER.getVendorTypeNumber(), JDBCType.INTEGER.getVendorTypeNumber() ) );
        }
    }


    @Test
    public void testSupportsTableCorrelation() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsTableCorrelationNames() );
        }
    }


    @Test
    public void testSupportsDifferentTableCorrelation() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsDifferentTableCorrelationNames() );
        }
    }


    @Test
    public void testSupportsExpsInOrderBy() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsExpressionsInOrderBy() );
        }
    }


    @Test
    public void testSupportsOrderedByUnrelated() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsOrderByUnrelated() );
        }
    }


    @Test
    public void testSupportsGroupBy() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsGroupBy() );
        }
    }


    @Test
    public void testSupportsGroupByUnrelated() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsGroupByUnrelated() );
        }
    }


    @Test
    public void testSupportsGroupByBeyondSelect() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsGroupByBeyondSelect() );
        }
    }


    @Test
    public void testSupportsLikeEscapeClause() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsLikeEscapeClause() );
        }
    }


    @Test
    public void testSupportsMultipleResults() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsMultipleResultSets() );
        }
    }


    @Test
    public void testSupportsMultipleTransactions() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsMultipleTransactions() );
        }
    }


    @Test
    public void testSupportsNonNullableCollumns() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsNonNullableColumns() );
        }
    }


    @Test
    public void testSupportsMinimumSQLGrammar() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsMinimumSQLGrammar() );
        }
    }


    @Test
    public void testSupportsCoreSQLGrammar() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsCoreSQLGrammar() );
        }
    }


    @Test
    public void testSupportsExtendedSQLGrammar() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsExtendedSQLGrammar() );
        }
    }


    @Test
    public void testANSI92EntryLevelSQL() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsANSI92EntryLevelSQL() );
        }
    }


    @Test
    public void testANSI92IntermediateSQL() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsANSI92IntermediateSQL() );
        }
    }


    @Test
    public void testANSI92FullSQL() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsANSI92FullSQL() );
        }
    }


    @Test
    public void testSupportsIntegrityEnhancementFacility() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsIntegrityEnhancementFacility() );
        }
    }


    @Test
    public void testsupportsOuterJoins() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsOuterJoins() );
        }
    }


    @Test
    public void testSupportsFullOuterJoins() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsFullOuterJoins() );
        }
    }


    @Test
    public void testSupportsLimitedOuterJoins() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsLimitedOuterJoins() );
        }
    }


    @Test
    public void testGetSchemaTerm() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( "namespace", metadata.getSchemaTerm() );
        }
    }


    @Test
    public void testGetProcedureTerm() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( "procedure", metadata.getProcedureTerm() );
        }
    }


    @Test
    public void testGetDatabaseTerm() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( "database", metadata.getCatalogTerm() );
        }
    }


    @Test
    public void testIsCatalogAtStart() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.isCatalogAtStart() );
        }
    }


    @Test
    public void testGetCatalogSeparator() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( ".", metadata.getCatalogSeparator() );
        }
    }


    @Test
    public void testSupportsSchemasInDataManipulation() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsSchemasInDataManipulation() );
        }
    }


    @Test
    public void testSupportsSchemasInProcedureCalls() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsSchemasInProcedureCalls() );
        }
    }


    @Test
    public void testSupportsSchemasInTableDefinitions() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsSchemasInTableDefinitions() );
        }
    }


    @Test
    public void testSupportsSchemasInIndexDefinition() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsSchemasInIndexDefinitions() );
        }
    }


    @Test
    public void testSupportsSchemasInPrivilegeDefinitions() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsSchemasInPrivilegeDefinitions() );
        }
    }


    @Test
    public void testSupportsCatalogsInDataManipulation() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsCatalogsInDataManipulation() );
        }
    }


    @Test
    public void testSupportsCatalogsInProcedureCalls() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsCatalogsInProcedureCalls() );
        }
    }


    @Test
    public void testSupportsCatalogsInTableDefinitions() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsCatalogsInTableDefinitions() );
        }
    }


    @Test
    public void testSupportsCatalogsInIndexDefinition() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsCatalogsInIndexDefinitions() );
        }
    }


    @Test
    public void testSupportsCatalogsInPrivilegeDefinitions() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsCatalogsInPrivilegeDefinitions() );
        }
    }


    @Test
    public void testSupportsPositionedDeletes() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsPositionedDelete() );
        }
    }


    @Test
    public void testSupportsPositionedUpdate() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsPositionedUpdate() );
        }
    }


    @Test
    public void testSelectForUpdate() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsSelectForUpdate() );
        }
    }


    @Test
    public void testSupportsStoredProcedures() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsStoredProcedures() );
        }
    }


    @Test
    public void testSupportsSubqueriesInComparison() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsSubqueriesInComparisons() );
        }
    }


    @Test
    public void testSupportsSubqueriesInExists() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsSubqueriesInExists() );
        }
    }


    @Test
    public void testSupportsSubqueriesInIns() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsSubqueriesInIns() );
        }
    }


    @Test
    public void testSupportsSubqueriesInQuantifieds() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsSubqueriesInQuantifieds() );
        }
    }


    @Test
    public void testSupportsCorrelatedSubqueries() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsSubqueriesInQuantifieds() );
        }
    }


    @Test
    public void testSupportsUnion() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsUnion() );
        }
    }


    @Test
    public void testSupportsUnionAll() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsUnionAll() );
        }
    }


    @Test
    public void testSupportsOpenCursorAcrossCommit() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsOpenCursorsAcrossCommit() );
        }
    }


    @Test
    public void testSupportsOpenCursorAcrossRollback() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsOpenCursorsAcrossRollback() );
        }
    }


    @Test
    public void testSupportsOpenStatementAcrossCommit() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsOpenStatementsAcrossCommit() );
        }
    }


    @Test
    public void testSupportsOpenStatementAcrossRollback() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsOpenStatementsAcrossRollback() );
        }
    }


    @Test
    public void testGetMaxBinaryLiteralLength() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxBinaryLiteralLength() );
        }
    }


    @Test
    public void testGetMaxCharLiteralLength() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxCharLiteralLength() );
        }
    }


    @Test
    public void testGetMaxColumnNameLength() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxColumnNameLength() );
        }
    }


    @Test
    public void testGetMaxColumnsInGroupBy() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxColumnsInGroupBy() );
        }
    }


    @Test
    public void testGetMaxColumnsInIndex() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxColumnsInIndex() );
        }
    }


    @Test
    public void testGetMaxColumnsInOrderBy() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxColumnsInOrderBy() );
        }
    }


    @Test
    public void testGetMaxColumnsInSelect() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxColumnsInSelect() );
        }
    }


    @Test
    public void testGetMaxColumnsInTable() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxColumnsInTable() );
        }
    }


    @Test
    public void testGetMaxConnections() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxConnections() );
        }
    }


    @Test
    public void testGetMaxCursorNameLength() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxCursorNameLength() );
        }
    }


    @Test
    public void testGetMaxIndexLength() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxIndexLength() );
        }
    }


    @Test
    public void testGetMaxSchemaNameLength() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxSchemaNameLength() );
        }
    }


    @Test
    public void testGetMaxProcedureNameLength() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxProcedureNameLength() );
        }
    }


    @Test
    public void testGetMaxCatalogNameLength() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxCatalogNameLength() );
        }
    }


    @Test
    public void testGetMaxRowSize() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxRowSize() );
        }
    }


    @Test
    public void testDoesMaxRowSizeIncludeBlobs() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.doesMaxRowSizeIncludeBlobs() );
        }
    }


    @Test
    public void testGetMaxStatementLength() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxStatementLength() );
        }
    }


    @Test
    public void testGetMaxStatements() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxStatements() );
        }
    }


    @Test
    public void testGetMaxTableNameLength() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxTableNameLength() );
        }
    }


    @Test
    public void testGetMaxTablesInSelect() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxTablesInSelect() );
        }
    }


    @Test
    public void testGetMaxUserNameLength() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( 0, metadata.getMaxUserNameLength() );
        }
    }


    @Test
    public void testGetDefaultTransactionIsolation() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertEquals( Connection.TRANSACTION_READ_COMMITTED, metadata.getDefaultTransactionIsolation() );
        }
    }


    @Test
    public void testSupportsTransactions() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsTransactions() );
        }
    }


    @Test
    public void testSupportsTransactionNone() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsTransactionIsolationLevel( Connection.TRANSACTION_NONE ) );
        }
    }


    @Test
    public void testSupportsTransactionReadCommitted() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsTransactionIsolationLevel( Connection.TRANSACTION_READ_COMMITTED ) );
        }
    }


    @Test
    public void testSupportsTransactionReadUncommitted() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsTransactionIsolationLevel( Connection.TRANSACTION_READ_UNCOMMITTED ) );
        }
    }


    @Test
    public void testSupportsTransactionRepeatableRead() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsTransactionIsolationLevel( Connection.TRANSACTION_REPEATABLE_READ ) );
        }
    }


    @Test
    public void testSupportsTransactionSerializable() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsTransactionIsolationLevel( Connection.TRANSACTION_SERIALIZABLE ) );
        }
    }


    @Test
    public void testSupportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.supportsDataDefinitionAndDataManipulationTransactions() );
        }
    }


    @Test
    public void testSupportsDataManipulationTransactionsOnly() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertTrue( metadata.supportsDataManipulationTransactionsOnly() );
        }
    }


    @Test
    public void testDataDefinitionCausesTransactionCommit() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.dataDefinitionCausesTransactionCommit() );
        }
    }


    @Test
    public void testDataDefinitionIgnoredInTransactions() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            assertFalse( metadata.dataDefinitionIgnoredInTransactions() );
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testgetProceduresThrowsExceptionIfStrict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet rs = metadata.getProcedures( null, null, "pattern" );
        }
    }


    @Test
    public void testGetProceduresReturnsEmpty() throws SQLException {
        try ( Connection connection = jdbcConnect( "jdbc:polypheny://pa:pa@" + dbHost + ":" + port + "/?strict=true" ) ) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet resultSet = metadata.getProcedures( null, null, "pattern" );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 9, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "PROCEDURE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "PROCEDURE_SCHEM", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "PROCEDURE_NAME", rsmd.getColumnName( 3 ) );
            Assert.assertEquals( "Wrong column name", "reserved for future use", rsmd.getColumnName( 4 ) );
            Assert.assertEquals( "Wrong column name", "reserved for future use", rsmd.getColumnName( 5 ) );
            Assert.assertEquals( "Wrong column name", "reserved for future use", rsmd.getColumnName( 6 ) );
            Assert.assertEquals( "Wrong column name", "REMARKS", rsmd.getColumnName( 7 ) );
            Assert.assertEquals( "Wrong column name", "PROCEDURE_TYPE", rsmd.getColumnName( 8 ) );
            Assert.assertEquals( "Wrong column name", "SPECIFIC_NAME", rsmd.getColumnName( 9 ) );

            TestHelper.checkResultSet(
                    resultSet,
                    ImmutableList.of( new Object[]{} ) );
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testgetProcedureColumnsThrowsExceptionIfStrict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet rs = metadata.getProcedureColumns( null, null, null, null );
        }
    }


    @Test
    public void testGetProcedureColumnsReturnsEmpty() throws SQLException {
        try ( Connection connection = jdbcConnect( "jdbc:polypheny://pa:pa@" + dbHost + ":" + port + "/?strict=true" ) ) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet resultSet = metadata.getProcedures( null, null, null );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 9, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "PROCEDURE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "PROCEDURE_SCHEM", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "PROCEDURE_NAME", rsmd.getColumnName( 3 ) );
            Assert.assertEquals( "Wrong column name", "COLUMN_NAME", rsmd.getColumnName( 4 ) );
            Assert.assertEquals( "Wrong column name", "COLUMN_TYPE", rsmd.getColumnName( 5 ) );
            Assert.assertEquals( "Wrong column name", "DATA_TYPE", rsmd.getColumnName( 6 ) );
            Assert.assertEquals( "Wrong column name", "TYPE_NAME", rsmd.getColumnName( 7 ) );
            Assert.assertEquals( "Wrong column name", "PRECISION", rsmd.getColumnName( 8 ) );
            Assert.assertEquals( "Wrong column name", "LENGTH", rsmd.getColumnName( 9 ) );
            Assert.assertEquals( "Wrong column name", "SCALE", rsmd.getColumnName( 10 ) );
            Assert.assertEquals( "Wrong column name", "RADIX", rsmd.getColumnName( 11 ) );
            Assert.assertEquals( "Wrong column name", "NULLABLE", rsmd.getColumnName( 12 ) );
            Assert.assertEquals( "Wrong column name", "REMARKS", rsmd.getColumnName( 13 ) );
            Assert.assertEquals( "Wrong column name", "COLUMN_DEF", rsmd.getColumnName( 14 ) );
            Assert.assertEquals( "Wrong column name", "SQL_DATA_TYPE", rsmd.getColumnName( 15 ) );
            Assert.assertEquals( "Wrong column name", "SQL_DATETIME_SUB", rsmd.getColumnName( 16 ) );
            Assert.assertEquals( "Wrong column name", "CHAR_OCTET_LENGTH", rsmd.getColumnName( 17 ) );
            Assert.assertEquals( "Wrong column name", "ORDINAL_POSITION", rsmd.getColumnName( 18 ) );
            Assert.assertEquals( "Wrong column name", "IS_NULLABLE", rsmd.getColumnName( 19 ) );
            Assert.assertEquals( "Wrong column name", "SPECIFIC_NAME", rsmd.getColumnName( 20 ) );

            TestHelper.checkResultSet(
                    resultSet,
                    ImmutableList.of( new Object[]{} ) );
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


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testColumnPrivilegesThrowsExceptionIfStrict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet rs = metadata.getColumnPrivileges( null, null, null, null );
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testColumnPrivilegesReturnsDummy() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getColumnPrivileges( null, "test", null, null );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 4, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TABLE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_SCHEM", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_NAME", rsmd.getColumnName( 3 ) );
            Assert.assertEquals( "Wrong column name", "COLUMN_NAME", rsmd.getColumnName( 4 ) );
            Assert.assertEquals( "Wrong column name", "GRANTOR", rsmd.getColumnName( 5 ) );
            Assert.assertEquals( "Wrong column name", "GRANTEE", rsmd.getColumnName( 6 ) );
            Assert.assertEquals( "Wrong column name", "PRIVILEGE", rsmd.getColumnName( 7 ) );
            Assert.assertEquals( "Wrong column name", "IS_GRANTABLE", rsmd.getColumnName( 8 ) );

            // Check data
            final List<Object[]> expected = new LinkedList<>();
            expected.add( new Object[]{ "APP", "test", "foo2", "name", null, "pa", "INSERT", "NO" } );
            expected.add( new Object[]{ "APP", "test", "foo2", "name", null, "pa", "REFERENCE", "NO" } );
            expected.add( new Object[]{ "APP", "test", "foo2", "name", null, "pa", "SELECT", "NO" } );
            expected.add( new Object[]{ "APP", "test", "foo2", "name", null, "pa", "UPDATE", "NO" } );

            TestHelper.checkResultSet(
                    connection.getMetaData().getColumnPrivileges( null, "test", "foo2", "name" ),
                    expected );
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testTablePrivilegesThrowsExceptionIfStrict() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false );
                Connection connection = polyphenyDbConnection.getConnection() ) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet rs = metadata.getTablePrivileges( null, null, null );
        }
    }


    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testTablePrivilegesReturnsDummy() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getTablePrivileges( null, "test", "foo2" );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 4, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TABLE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_SCHEM", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_NAME", rsmd.getColumnName( 3 ) );
            Assert.assertEquals( "Wrong column name", "GRANTOR", rsmd.getColumnName( 4 ) );
            Assert.assertEquals( "Wrong column name", "GRANTEE", rsmd.getColumnName( 5 ) );
            Assert.assertEquals( "Wrong column name", "PRIVILEGE", rsmd.getColumnName( 6 ) );
            Assert.assertEquals( "Wrong column name", "IS_GRANTABLE", rsmd.getColumnName( 7 ) );

            // Check data
            final List<Object[]> expected = new LinkedList<>();
            expected.add( new Object[]{ "APP", "test", "foo2", null, "pa", "DELETE", "NO" } );
            expected.add( new Object[]{ "APP", "test", "foo2", null, "pa", "INSERT", "NO" } );
            expected.add( new Object[]{ "APP", "test", "foo2", null, "pa", "REFERENCE", "NO" } );
            expected.add( new Object[]{ "APP", "test", "foo2", null, "pa", "SELECT", "NO" } );
            expected.add( new Object[]{ "APP", "test", "foo2", null, "pa", "UPDATE", "NO" } );

            TestHelper.checkResultSet(
                    connection.getMetaData().getTablePrivileges( null, "test", "foo2" ),
                    expected );
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
            Assert.assertEquals( "Wrong column name", "SCOPE_CATALOG", rsmd.getColumnName( 19 ) );
            Assert.assertEquals( "Wrong column name", "SCOPE_SCHEMA", rsmd.getColumnName( 20 ) );
            Assert.assertEquals( "Wrong column name", "SCOPE_TABLE", rsmd.getColumnName( 21 ) );
            Assert.assertEquals( "Wrong column name", "SOURCE_DATA_TYPE", rsmd.getColumnName( 22 ) );
            Assert.assertEquals( "Wrong column name", "IS_AUTOINCREMENT", rsmd.getColumnName( 23 ) );
            Assert.assertEquals( "Wrong column name", "IS_GENERATEDCOLUMN", rsmd.getColumnName( 24 ) );
            Assert.assertEquals( "Wrong column name", "COLLATION", rsmd.getColumnName( 15 ) );

            // Check data
            final Object[] columnId = new Object[]{ "APP", "public", "foo", "id", 4, "INTEGER", null, null, null, null, 0, "", null, null, null, null, 1, "NO", null, null, null, null, "No", "No", null };
            final Object[] columnName = new Object[]{ "APP", "public", "foo", "name", 12, "VARCHAR", 20, null, null, null, 1, "", null, null, null, null, 2, "YES", null, null, null, null, "No", "No", "CASE_INSENSITIVE" };
            final Object[] columnBar = new Object[]{ "APP", "public", "foo", "bar", 12, "VARCHAR", 33, null, null, null, 1, "", null, null, null, null, 3, "YES", null, null, null, null, "No", "No", "CASE_SENSITIVE" };
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
            final Object[] index1 = new Object[]{ "APP", "public", "foo", false, null, "i_foo", 0, 1, "id", null, -1, null, null, 0, 1 };
            final Object[] index2a = new Object[]{ "APP", "test", "foo2", true, null, "i_foo2", 0, 1, "name", null, -1, null, null, 0, 1 };
            final Object[] index2b = new Object[]{ "APP", "test", "foo2", true, null, "i_foo2", 0, 2, "foobar", null, -1, null, null, 0, 1 };

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
