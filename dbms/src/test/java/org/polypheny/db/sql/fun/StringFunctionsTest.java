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

package org.polypheny.db.sql.fun;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.runtime.Hook;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class StringFunctionsTest {

    private static final Pattern REPLACE_CALL = Pattern.compile( "\\bREPLACE\\s*\\(", Pattern.CASE_INSENSITIVE );


    @BeforeAll
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
                statement.executeUpdate( "CREATE TABLE TableA(ID INTEGER NOT NULL, DataA VARCHAR(255), PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TableA VALUES (1, 'First')" );
                statement.executeUpdate( "INSERT INTO TableA VALUES (2, 'Second')" );
                statement.executeUpdate( "INSERT INTO TableA VALUES (3, 'Third')" );

                statement.executeUpdate( "CREATE TABLE TableB(ID INTEGER NOT NULL, DataB VARCHAR(255), PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TableB VALUES (1, 'Fourth')" );
                statement.executeUpdate( "INSERT INTO TableB VALUES (2, 'Fifth')" );
                statement.executeUpdate( "INSERT INTO TableB VALUES (3, 'Sixth')" );

                statement.executeUpdate( "CREATE TABLE TableC(ID INTEGER NOT NULL, DataC VARCHAR(255), PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TableC VALUES (1, ' Seventh')" );
                statement.executeUpdate( "INSERT INTO TableC VALUES (2, 'Eighth ')" );
                statement.executeUpdate( "INSERT INTO TableC VALUES (3, ' ninth ')" );

                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE TableA" );
                statement.executeUpdate( "DROP TABLE TableB" );
                statement.executeUpdate( "DROP TABLE TableC" );
            }
            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    public void stringReplaceLiterals() throws SQLException {
        try ( JdbcConnection connection = new JdbcConnection( true );
                Statement statement = connection.getConnection().createStatement();
                ResultSet result = statement.executeQuery( """
                        SELECT REPLACE('Hi', 'i', 'ello'),
                               REPLACE('😀', '', '-'),
                               REPLACE('abc', 'b', ''),
                               REPLACE('', 'b', '-'),
                               REPLACE(CAST(NULL AS VARCHAR(3)), 'b', '-'),
                               REPLACE('abc', CAST(NULL AS VARCHAR(3)), '-'),
                               REPLACE('abc', 'b', CAST(NULL AS VARCHAR(3)))
                        """ ) ) {
            assertEquals( 8, result.getMetaData().getPrecision( 1 ) );
            TestHelper.checkResultSet( result, ImmutableList.of( new Object[]{ "Hello", "😀", "ac", "", null, null, null } ) );
        }
    }


    @Test
    public void stringReplaceParameters() throws SQLException {
        AtomicBoolean localReplace = new AtomicBoolean();
        try ( Hook.Closeable hook = Hook.JAVA_PLAN.add( (Object plan) -> {
            if ( plan instanceof String javaPlan && javaPlan.contains( "Functions.replace(" ) ) {
                localReplace.set( true );
            }
        } ); JdbcConnection connection = new JdbcConnection( true );
                PreparedStatement statement = connection.getConnection().prepareStatement( """
                        SELECT REPLACE(CAST(? AS VARCHAR(10)), CAST(? AS VARCHAR(10)), CAST(? AS VARCHAR(10)))
                        """ ) ) {
            statement.setString( 1, "a.$.b.$" );
            statement.setString( 2, ".$" );
            statement.setString( 3, "_" );
            try ( ResultSet result = statement.executeQuery() ) {
                assertTrue( localReplace.get(), "Expected REPLACE in the generated Java plan" );
                TestHelper.checkResultSet( result, ImmutableList.of( new Object[]{ "a_.b_" } ) );
            }

            statement.setString( 1, "a.$.b.$" );
            statement.setString( 2, "" );
            statement.setNull( 3, Types.VARCHAR );
            try ( ResultSet result = statement.executeQuery() ) {
                TestHelper.checkResultSet( result, ImmutableList.of( new Object[]{ null } ) );
            }

            statement.setString( 1, "a.$.b.$" );
            statement.setNull( 2, Types.VARCHAR );
            statement.setString( 3, "_" );
            try ( ResultSet result = statement.executeQuery() ) {
                TestHelper.checkResultSet( result, ImmutableList.of( new Object[]{ null } ) );
            }

            statement.setNull( 1, Types.VARCHAR );
            statement.setString( 2, ".$" );
            statement.setString( 3, "_" );
            try ( ResultSet result = statement.executeQuery() ) {
                TestHelper.checkResultSet( result, ImmutableList.of( new Object[]{ null } ) );
            }
        }
    }


    @Test
    public void stringReplaceJdbcPushdown() throws SQLException {
        assumeTrue( List.of( "hsqldb", "postgresql", "monetdb" ).stream().anyMatch( PolyphenyDb.defaultStoreName::equalsIgnoreCase ) );
        Queue<String> pushedSql = new ConcurrentLinkedQueue<>();
        try ( Hook.Closeable hook = capturePushedSql( pushedSql );
                JdbcConnection connection = new JdbcConnection( true );
                Statement statement = connection.getConnection().createStatement();
                ResultSet result = statement.executeQuery( """
                        SELECT REPLACE(CAST(DataC AS VARCHAR(7)), 'i', 'ello'),
                               REPLACE(DataC, '', '-'),
                               REPLACE(DataC, 'i', ''),
                               REPLACE('', DataC, '-'),
                               REPLACE(CAST(NULLIF(ID, 3) AS VARCHAR(5)), 'i', '-'),
                               REPLACE(DataC, ' ', '-'),
                               REPLACE(DataC, 'i', ' ')
                        FROM TableC WHERE ID = 3
                        """ ) ) {
            assertReplacePushed( pushedSql, 7 );
            assertEquals( 28, result.getMetaData().getPrecision( 1 ) );
            TestHelper.checkResultSet( result, ImmutableList.of( new Object[]{ " nellonth ", " ninth ", " nnth ", "", null, "-ninth-", " n nth " } ) );
        }
    }


    @Test
    public void stringReplaceTextPushdown() throws SQLException {
        assumeTrue( List.of( "postgresql", "monetdb" ).stream().anyMatch( PolyphenyDb.defaultStoreName::equalsIgnoreCase ) );
        Queue<String> pushedSql = new ConcurrentLinkedQueue<>();
        try ( JdbcConnection connection = new JdbcConnection( true );
                Statement statement = connection.getConnection().createStatement() ) {
            statement.executeUpdate( "CREATE TABLE ReplaceClob (ID INTEGER NOT NULL, Body TEXT, PRIMARY KEY (ID))" );
            try {
                statement.executeUpdate( "INSERT INTO ReplaceClob VALUES (1, 'xx')" );
                statement.executeUpdate( "INSERT INTO ReplaceClob VALUES (2, '')" );
                statement.executeUpdate( "INSERT INTO ReplaceClob VALUES (3, NULL)" );
                try ( Hook.Closeable hook = capturePushedSql( pushedSql );
                        ResultSet result = statement.executeQuery( """
                                SELECT Body, REPLACE(Body, 'x', ''), REPLACE(Body, 'x', 'y')
                                FROM ReplaceClob ORDER BY ID
                                """ ) ) {
                    assertReplacePushed( pushedSql, 2 );
                    TestHelper.checkResultSet( result, ImmutableList.of(
                            new Object[]{ "xx", "", "yy" },
                            new Object[]{ "", "", "" },
                            new Object[]{ null, null, null } ) );
                }
            } finally {
                statement.executeUpdate( "DROP TABLE ReplaceClob" );
            }
        }
    }


    @Test
    public void stringReplacePostgresCharPushdown() throws SQLException {
        assumeTrue( "postgresql".equalsIgnoreCase( PolyphenyDb.defaultStoreName ) );
        Queue<String> pushedSql = new ConcurrentLinkedQueue<>();
        try ( Hook.Closeable hook = capturePushedSql( pushedSql );
                JdbcConnection connection = new JdbcConnection( true );
                Statement statement = connection.getConnection().createStatement();
                ResultSet result = statement.executeQuery( """
                        SELECT REPLACE(CAST(DataA AS CHAR(7)), ' ', '-'),
                               REPLACE(DataA, 'i', '😀'),
                               REPLACE(REPLACE(DataA, 'i', CAST('😀' AS VARCHAR(2))), '😀', '-'),
                               REPLACE(DataA, 'i', CASE WHEN CHAR_LENGTH(DataA) > 3 THEN '😀' ELSE '😃' END)
                        FROM TableA WHERE ID = 1
                        """ ) ) {
            assertReplacePushed( pushedSql, 5 );
            TestHelper.checkResultSet( result, ImmutableList.of( new Object[]{ "First--", "F😀rst", "F-rst", "F😀rst" } ) );
        }
    }


    @Test
    @Tag("docker")
    public void stringReplaceMariaDbPushdown() throws Exception {
        assumeTrue( "hsqldb".equalsIgnoreCase( PolyphenyDb.defaultStoreName ) );
        assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for the MariaDB source" );
        String database = "polypheny_replace";
        String username = "polypheny";
        String password = "polypheny";
        String source = "mariadb_replace_src";
        String table = "replace_maria_data";
        try ( TestHelper.DockerMysql maria = TestHelper.startMariaDbDocker( database, username, password ) ) {
            maria.execute( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, val VARCHAR(16), repl VARCHAR(16))" );
            maria.execute( "INSERT INTO " + table + " VALUES (1, 'a b', NULL)" );
            TestHelper.addMysqlSource( source, maria.getHost(), maria.getPort(), database, username, password, table );
            Queue<String> pushedSql = new ConcurrentLinkedQueue<>();
            try ( Hook.Closeable hook = capturePushedSql( pushedSql );
                    JdbcConnection connection = new JdbcConnection( true );
                    Statement statement = connection.getConnection().createStatement() ) {
                try ( ResultSet result = statement.executeQuery( """
                        SELECT REPLACE(val, '', '-'), REPLACE(val, 'b', ''), REPLACE('', val, '-'),
                               REPLACE(val, ' ', '-'), REPLACE(val, 'b', ' '),
                               REPLACE(val, '', repl), REPLACE(val, 'missing', repl), REPLACE(val, 'b', repl),
                               REPLACE(val, repl, '-'), REPLACE(repl, val, '-')
                        FROM replace_maria_data WHERE id = 1
                        """ ) ) {
                    assertReplacePushed( pushedSql, 10 );
                    TestHelper.checkResultSet( result, ImmutableList.of( new Object[]{ "a b", "a ", "", "a-b", "a  ", null, null, null, null, null } ) );
                }

                pushedSql.clear();
                try ( ResultSet result = statement.executeQuery( """
                        SELECT REPLACE(val, 'b', CASE WHEN RAND(5) < 2 THEN 'z' ELSE repl END)
                        FROM replace_maria_data WHERE id = 1
                        """ ) ) {
                    assertTrue( pushedSql.stream().noneMatch( sql -> REPLACE_CALL.matcher( sql ).find() ),
                            () -> "Unexpected pushdown of REPLACE with a nullable RAND operand: " + pushedSql );
                    TestHelper.checkResultSet( result, ImmutableList.of( new Object[]{ "a z" } ) );
                }

                pushedSql.clear();
                try ( ResultSet result = statement.executeQuery( """
                        SELECT REPLACE(val, 'b', r)
                        FROM (SELECT val, CASE WHEN RAND(5) < 2 THEN 'z' ELSE repl END AS r
                              FROM replace_maria_data ORDER BY id FETCH NEXT 1 ROWS ONLY) AS d
                        """ ) ) {
                    assertTrue( pushedSql.stream().noneMatch( sql -> REPLACE_CALL.matcher( sql ).find() ),
                            () -> "Unexpected pushdown of REPLACE over a derived RAND operand: " + pushedSql );
                    TestHelper.checkResultSet( result, ImmutableList.of( new Object[]{ "a z" } ) );
                }

                pushedSql.clear();
                try ( PreparedStatement replace = connection.getConnection().prepareStatement( """
                        SELECT REPLACE(val, '', CAST(? AS VARCHAR(16))) FROM replace_maria_data WHERE id = 1
                        """ ) ) {
                    replace.setNull( 1, Types.VARCHAR );
                    try ( ResultSet result = replace.executeQuery() ) {
                        assertReplacePushed( pushedSql, 1 );
                        TestHelper.checkResultSet( result, ImmutableList.of( new Object[]{ null } ) );
                    }
                }
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + source + "\"" );
            }
        }
    }


    private static Hook.Closeable capturePushedSql( Queue<String> pushedSql ) {
        return Hook.QUERY_PLAN.add( (Object plan) -> {
            if ( plan instanceof String sql ) {
                pushedSql.add( sql );
            }
        } );
    }


    private static void assertReplacePushed( Queue<String> pushedSql, int count ) {
        assertTrue( pushedSql.stream().anyMatch( sql -> REPLACE_CALL.matcher( sql ).results().count() == count ),
                () -> "Expected " + count + " pushed REPLACE calls, got " + pushedSql );
    }


    @Test
    public void concatenatesTwoString() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // String Concatenate ||
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "FirstFourth" },
                        new Object[]{ 2, "SecondFifth" },
                        new Object[]{ 3, "ThirdSixth" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,S.DataA||T.DataB FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void stringLength() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                //,CHARLENGTH() WITH String Concatenate ||
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 11 },
                        new Object[]{ 2, 11 },
                        new Object[]{ 3, 10 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,CHAR_LENGTH(S.DataA||T.DataB) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );

                expectedResult = ImmutableList.of(
                        new Object[]{ 1, 5 },
                        new Object[]{ 2, 6 },
                        new Object[]{ 3, 5 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,CHAR_LENGTH(S.DataA) FROM TableA AS S" ),
                        expectedResult,
                        true );

            }
        }
    }


    @Test
    public void caseSensitive() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // UPPER(CONCATENATED STRING) MAKES STRING UPPERCASE
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "FIRSTFOURTH" },
                        new Object[]{ 2, "SECONDFIFTH" },
                        new Object[]{ 3, "THIRDSIXTH" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id , Upper(S.DataA||T.DataB) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );

                // LOWER(CONCATENATED STRING) MAKES STRING LOWER CASE
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "firstfourth" },
                        new Object[]{ 2, "secondfifth" },
                        new Object[]{ 3, "thirdsixth" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id , LOWER(S.DataA||T.DataB) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void stringPosition() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // POSITION(string1 IN string2) Returns the position of the first occurrence of string1 in string2
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, 10 },
                        new Object[]{ 2, 10 },
                        new Object[]{ 3, 9 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,POSITION('th' IN S.DataA||T.DataB) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );

                // POSITION(string1 IN string2 FROM integer) Returns the position of the first occurrence of string1 in string2 starting at a given point (not standard SQL)
                /*expectedResult = ImmutableList.of(
                        new Object[]{ 1, 2 },
                        new Object[]{ 2, 2 },
                        new Object[]{ 3, 1 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,POSITION('th' IN S.DataA||T.DataB FROM 9) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult );*/
            }
        }
    }


    @Test
    public void stringTrim() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // TRIM( { BOTH | LEADING | TRAILING } string1 FROM string2) Removes the longest string containing only the characters in string1 from the start/end/both ends of string1
                // TODO check whitespace
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, " Sevent" },
                        new Object[]{ 2, "Eighth " },
                        new Object[]{ 3, " ninth " }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT U.id, TRIM(TRAILING 'h' FROM U.dataC) FROM TableC AS U " ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void stringOverlay() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // OVERLAY(string1 PLACING string2 FROM integer [ FOR integer2 ])
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Foutyh" },
                        new Object[]{ 2, "Fifty" },
                        new Object[]{ 3, "Sixty" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT T.id ,Overlay(T.DataB placing 'ty' from 4) FROM TableB AS T" ),
                        expectedResult,
                        true );

                // OVERLAY() with Concatenated String Inside
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "FirstFoutyh" },
                        new Object[]{ 2, "SecondFityh" },
                        new Object[]{ 3, "ThirdSixty" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,Overlay(S.DataA||T.DataB placing 'ty' from 9) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void subString() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // SUBSTRING(string FROM integer) Returns a substring of a character string starting at a given point
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "First" },
                        new Object[]{ 2, "Second" },
                        new Object[]{ 3, "Third" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,SUBSTRING(S.DataA,1) FROM TableA AS S" ),
                        expectedResult,
                        true );

                // SUBSTRING(string FROM integer FOR integer) Returns a substring of a character string starting at a given point with a given length
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "First" },
                        new Object[]{ 2, "Secon" },
                        new Object[]{ 3, "Third" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id ,SUBSTRING(S.DataA||T.DataB,1,5) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void stringInitcap() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // INITCAP(string)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "First" },
                        new Object[]{ 2, "Second" },
                        new Object[]{ 3, "Third" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id , INITCAP(DataA) FROM TableA" ),
                        expectedResult,
                        true );

                // INITCAP(string) With String concatenation
                expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Firstfourth" },
                        new Object[]{ 2, "Secondfifth" },
                        new Object[]{ 3, "Thirdsixth" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.id , INITCAP(S.DataA||T.DataB) FROM TableA AS S NATURAL JOIN TableB AS T" ),
                        expectedResult,
                        true );
            }
        }
    }

}
