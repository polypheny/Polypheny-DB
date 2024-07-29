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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.test;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Ordering;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.polypheny.db.sql.sql2alg.SqlToAlgConverter;
import org.polypheny.db.util.Sources;
import org.polypheny.db.util.Util;


/**
 * Unit test of the Polypheny-DB CSV adapter.
 */
@SuppressWarnings("ALL")
@Disabled
public class CsvTest {

    /**
     * Quotes a string for Java or JSON.
     */
    private static String escapeString( String s ) {
        return escapeString( new StringBuilder(), s ).toString();
    }


    /**
     * Quotes a string for Java or JSON, into a builder.
     */
    private static StringBuilder escapeString( StringBuilder buf, String s ) {
        buf.append( '"' );
        int n = s.length();
        char lastChar = 0;
        for ( int i = 0; i < n; ++i ) {
            char c = s.charAt( i );
            switch ( c ) {
                case '\\':
                    buf.append( "\\\\" );
                    break;
                case '"':
                    buf.append( "\\\"" );
                    break;
                case '\n':
                    buf.append( "\\n" );
                    break;
                case '\r':
                    if ( lastChar != '\n' ) {
                        buf.append( "\\r" );
                    }
                    break;
                default:
                    buf.append( c );
                    break;
            }
            lastChar = c;
        }
        return buf.append( '"' );
    }


    /**
     * Returns a function that checks the contents of a result set against an expected string.
     */
    private static Consumer<ResultSet> expect( final String... expected ) {
        return resultSet -> {
            try {
                final List<String> lines = new ArrayList<>();
                CsvTest.collect( lines, resultSet );
                assertEquals( Arrays.asList( expected ), lines );
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        };
    }


    /**
     * Returns a function that checks the contents of a result set against an expected string.
     */
    private static Consumer<ResultSet> expectUnordered( String... expected ) {
        final List<String> expectedLines = Ordering.natural().immutableSortedCopy( Arrays.asList( expected ) );
        return resultSet -> {
            try {
                final List<String> lines = new ArrayList<>();
                CsvTest.collect( lines, resultSet );
                Collections.sort( lines );
                assertEquals( expectedLines, lines );
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        };
    }


    private static void collect( List<String> result, ResultSet resultSet ) throws SQLException {
        final StringBuilder buf = new StringBuilder();
        while ( resultSet.next() ) {
            buf.setLength( 0 );
            int n = resultSet.getMetaData().getColumnCount();
            String sep = "";
            for ( int i = 1; i <= n; i++ ) {
                buf.append( sep )
                        .append( resultSet.getMetaData().getColumnLabel( i ) )
                        .append( "=" )
                        .append( resultSet.getString( i ) );
                sep = "; ";
            }
            result.add( Util.toLinux( buf.toString() ) );
        }
    }


    private void close( Connection connection, Statement statement ) {
        if ( statement != null ) {
            try {
                statement.close();
            } catch ( SQLException e ) {
                // ignore
            }
        }
        if ( connection != null ) {
            try {
                connection.close();
            } catch ( SQLException e ) {
                // ignore
            }
        }
    }


    /**
     * Tests the vanity driver.
     */
    @Disabled
    @Test
    public void testVanityDriver() throws SQLException {
        Properties info = new Properties();
        Connection connection = DriverManager.getConnection( "jdbc:csv:", info );
        connection.close();
    }


    /**
     * Tests the vanity driver with properties in the URL.
     */
    @Disabled
    @Test
    public void testVanityDriverArgsInUrl() throws SQLException {
        Connection connection = DriverManager.getConnection( "jdbc:csv:" + "directory='foo'" );
        connection.close();
    }


    /**
     * Tests an inline schema with a non-existent directory.
     */
    @Test
    public void testBadDirectory() throws SQLException {
        Properties info = new Properties();
        info.put(
                "model",
                """
                        inline:{
                          version: '1.0',
                           schemas: [
                             {
                               type: 'custom',
                               name: 'bad',
                               factory: 'org.polypheny.db.adapter.csv.CsvSchemaFactory',
                               operand: {
                                 directory: '/does/not/exist'
                               }
                             }
                           ]
                        }""" );

        Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info );
        // must print "directory ... not found" to stdout, but not fail
        ResultSet tables = connection.getMetaData().getTables( null, null, null, null );
        tables.next();
        tables.close();
        connection.close();
    }


    /**
     * Reads from a table.
     */
    @Test
    public void testSelect() {
        sql( "model", "select * from EMPS" ).ok();
    }


    @Test
    public void testSelectSingleProjectGz() {
        sql( "smart", "select name from EMPS" ).ok();
    }


    @Test
    public void testSelectSingleProject() {
        sql( "smart", "select name from DEPTS" ).ok();
    }


    /**
     * Test case for "Type inference multiplying Java long by SQL INTEGER".
     */
    @Test
    public void testSelectLongMultiplyInteger() {
        final String sql = "select empno * 3 as e3\n" + "from long_emps where empno = 100";

        sql( "bug", sql ).checking( resultSet -> {
            try {
                assertThat( resultSet.next(), is( true ) );
                Long o = (Long) resultSet.getObject( 1 );
                assertThat( o, is( 300L ) );
                assertThat( resultSet.next(), is( false ) );
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        } ).ok();
    }


    @Test
    public void testCustomTable() {
        sql( "model-with-custom-table", "select * from CUSTOM_TABLE.EMPS" ).ok();
    }


    @Test
    public void testPushDownProjectDumb() {
        // rule does not fire, because we're using 'dumb' tables in simple model
        final String sql = "explain plan for select * from EMPS";
        final String expected = "PLAN=EnumerableInterpreter\n  BindableScan(table=[[SALES, EMPS]])\n";
        sql( "model", sql ).returns( expected ).ok();
    }


    @Test
    public void testPushDownProject() {
        final String sql = "explain plan for select * from EMPS";
        final String expected = "PLAN=CsvScan(table=[[SALES, EMPS]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n";
        sql( "smart", sql ).returns( expected ).ok();
    }


    @Test
    public void testPushDownProject2() {
        sql( "smart", "explain plan for select name, empno from EMPS" )
                .returns( "PLAN=CsvScan(table=[[SALES, EMPS]], fields=[[1, 0]])\n" )
                .ok();
        // make sure that it works...
        sql( "smart", "select name, empno from EMPS" )
                .returns( "NAME=Fred; EMPNO=100", "NAME=Eric; EMPNO=110", "NAME=John; EMPNO=110", "NAME=Wilma; EMPNO=120", "NAME=Alice; EMPNO=130" )
                .ok();
    }


    @Test
    public void testPushDownProjectAggregate() {
        final String sql = "explain plan for\n" + "select gender, count(*) from EMPS group by gender";
        final String expected = """
                PLAN=EnumerableAggregate(group=[{0}], EXPR$1=[COUNT()])
                  CsvScan(table=[[SALES, EMPS]], fields=[[3]])
                """;
        sql( "smart", sql ).returns( expected ).ok();
    }


    @Test
    public void testPushDownProjectAggregateWithFilter() {
        final String sql = "explain plan for\n" + "select max(empno) from EMPS where gender='F'";
        final String expected = """
                PLAN=EnumerableAggregate(group=[{}], EXPR$0=[MAX($0)])
                  EnumerableCalc(expr#0..1=[{inputs}], expr#2=['F':VARCHAR], expr#3=[=($t1, $t2)], proj#0..1=[{exprs}], $condition=[$t3])
                    CsvScan(table=[[SALES, EMPS]], fields=[[0, 3]])
                """;
        sql( "smart", sql ).returns( expected ).ok();
    }


    @Test
    public void testPushDownProjectAggregateNested() {
        final String sql = """
                explain plan for
                select gender, max(qty)
                from (
                  select name, gender, count(*) qty
                  from EMPS
                  group by name, gender) t
                group by gender""";
        final String expected = """
                PLAN=EnumerableAggregate(group=[{1}], EXPR$1=[MAX($2)])
                  EnumerableAggregate(group=[{0, 1}], QTY=[COUNT()])
                    CsvScan(table=[[SALES, EMPS]], fields=[[1, 3]])
                """;
        sql( "smart", sql ).returns( expected ).ok();
    }


    @Test
    public void testFilterableSelect() {
        sql( "filterable-model", "select name from EMPS" ).ok();
    }


    @Test
    public void testFilterableSelectStar() {
        sql( "filterable-model", "select * from EMPS" ).ok();
    }


    /**
     * Filter that can be fully handled by CsvFilterableTable.
     */
    @Test
    public void testFilterableWhere() {
        final String sql = "select empno, gender, name from EMPS where name = 'John'";
        sql( "filterable-model", sql ).returns( "EMPNO=110; GENDER=M; NAME=John" ).ok();
    }


    /**
     * Filter that can be partly handled by CsvFilterableTable.
     */
    @Test
    public void testFilterableWhere2() {
        final String sql = "select empno, gender, name from EMPS\n" + " where gender = 'F' and empno > 125";
        sql( "filterable-model", sql ).returns( "EMPNO=130; GENDER=F; NAME=Alice" ).ok();
    }


    @Test
    public void testJson() {
        final String sql = """
                select _MAP['id'] as id,
                 _MAP['title'] as title,
                 CHAR_LENGTH(CAST(_MAP['title'] AS VARCHAR(30))) as len
                 from "archers"
                """;
        sql( "bug", sql )
                .returns( "ID=19990101; TITLE=Tractor trouble.; LEN=16", "ID=19990103; TITLE=Charlie's surprise.; LEN=19" )
                .ok();
    }


    private Fluent sql( String model, String sql ) {
        return new Fluent( model, sql, this::output );
    }


    private void checkSql( String sql, String model, Consumer<ResultSet> fn ) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put( "model", jsonPath( model ) );
            connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info );
            statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery( sql );
            fn.accept( resultSet );
        } finally {
            close( connection, statement );
        }
    }


    private String jsonPath( String model ) {
        return resourcePath( model + ".json" );
    }


    private String resourcePath( String path ) {
        return Sources.of( CsvTest.class.getResource( "/" + path ) ).file().getAbsolutePath();
    }


    private void output( ResultSet resultSet, PrintStream out ) throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        while ( resultSet.next() ) {
            for ( int i = 1; ; i++ ) {
                out.print( resultSet.getString( i ) );
                if ( i < columnCount ) {
                    out.print( ", " );
                } else {
                    out.println();
                    break;
                }
            }
        }
    }


    @Test
    public void testJoinOnString() {
        final String sql = "select * from emps\n" + "join depts on emps.name = depts.name";
        sql( "smart", sql ).ok();
    }


    @Test
    public void testWackyColumns() {
        final String sql = "select * from wacky_column_names where false";
        sql( "bug", sql ).returns().ok();

        final String sql2 = """
                select "joined at", "naME"
                from wacky_column_names
                where "2gender" = 'F'""";
        sql( "bug", sql2 )
                .returns( "joined at=2005-09-07; naME=Wilma", "joined at=2007-01-01; naME=Alice" )
                .ok();
    }


    /**
     * Test case for "In Csv adapter, convert DATE and TIME values to int, and TIMESTAMP values to long".
     */
    @Test
    public void testGroupByTimestampAdd() {
        final String sql = """
                select count(*) as c,
                  {fn timestampadd(SQL_TSI_DAY, 1, JOINEDAT) } as t
                from EMPS group by {fn timestampadd(SQL_TSI_DAY, 1, JOINEDAT ) }\s""";
        sql( "model", sql )
                .returnsUnordered(
                        "C=1; T=1996-08-04",
                        "C=1; T=2002-05-04",
                        "C=1; T=2005-09-08",
                        "C=1; T=2007-01-02",
                        "C=1; T=2001-01-02" )
                .ok();

        final String sql2 = """
                select count(*) as c,
                  {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT) } as t
                from EMPS group by {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT ) }\s""";
        sql( "model", sql2 )
                .returnsUnordered(
                        "C=1; T=2002-06-03",
                        "C=1; T=2005-10-07",
                        "C=1; T=2007-02-01",
                        "C=1; T=2001-02-01",
                        "C=1; T=1996-09-03" )
                .ok();
    }


    @Test
    public void testUnionGroupByWithoutGroupKey() {
        final String sql = """
                select count(*) as c1 from EMPS group by NAME
                union
                select count(*) as c1 from EMPS group by NAME""";
        sql( "model", sql ).ok();
    }


    @Test
    public void testBoolean() {
        sql( "smart", "select empno, slacker from emps where slacker" ).returns( "EMPNO=100; SLACKER=true" ).ok();
    }


    @Test
    public void testReadme() {
        final String sql = "SELECT d.name, COUNT(*) cnt"
                + " FROM emps AS e"
                + " JOIN depts AS d ON e.deptno = d.deptno"
                + " GROUP BY d.name";
        sql( "smart", sql ).returns( "NAME=Sales; CNT=1", "NAME=Marketing; CNT=2" ).ok();
    }


    /**
     * Test case for "Type inference when converting IN clause to semijoin".
     */
    @Test
    public void testInToSemiJoinWithCast() {
        // Note that the IN list needs at least 20 values to trigger the rewrite to a semijoin. Try it both ways.
        final String sql = """
                SELECT e.name
                FROM emps AS e
                WHERE cast(e.empno as bigint) in\s""";
        final int threshold = SqlToAlgConverter.DEFAULT_IN_SUB_QUERY_THRESHOLD;
        sql( "smart", sql + range( 130, threshold - 5 ) ).returns( "NAME=Alice" ).ok();
        sql( "smart", sql + range( 130, threshold ) ).returns( "NAME=Alice" ).ok();
        sql( "smart", sql + range( 130, threshold + 1000 ) ).returns( "NAME=Alice" ).ok();
    }


    /**
     * Test case for "Underflow exception due to scaling IN clause literals".
     */
    @Test
    public void testInToSemiJoinWithoutCast() {
        final String sql = "SELECT e.name\n"
                + "FROM emps AS e\n"
                + "WHERE e.empno in "
                + range( 130, SqlToAlgConverter.DEFAULT_IN_SUB_QUERY_THRESHOLD );
        sql( "smart", sql ).returns( "NAME=Alice" ).ok();
    }


    private String range( int first, int count ) {
        final StringBuilder sb = new StringBuilder();
        for ( int i = 0; i < count; i++ ) {
            sb.append( i == 0 ? "(" : ", " ).append( first + i );
        }
        return sb.append( ')' ).toString();
    }


    @Test
    public void testDateType() throws SQLException {
        Properties info = new Properties();
        info.put( "model", jsonPath( "bug" ) );

        try ( Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info ) ) {
            ResultSet res = connection.getMetaData().getColumns( null, null, "DATE", "JOINEDAT" );
            res.next();
            assertEquals( res.getInt( "DATA_TYPE" ), java.sql.Types.DATE );

            res = connection.getMetaData().getColumns( null, null, "DATE", "JOINTIME" );
            res.next();
            assertEquals( res.getInt( "DATA_TYPE" ), java.sql.Types.TIME );

            res = connection.getMetaData().getColumns( null, null, "DATE", "JOINTIMES" );
            res.next();
            assertEquals( res.getInt( "DATA_TYPE" ), java.sql.Types.TIMESTAMP );

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery( "select \"JOINEDAT\", \"JOINTIME\", \"JOINTIMES\" from \"DATE\" where EMPNO = 100" );
            resultSet.next();

            // date
            assertEquals( java.sql.Date.class, resultSet.getDate( 1 ).getClass() );
            assertEquals( java.sql.Date.valueOf( "1996-08-03" ), resultSet.getDate( 1 ) );

            // time
            assertEquals( java.sql.Time.class, resultSet.getTime( 2 ).getClass() );
            assertEquals( java.sql.Time.valueOf( "00:01:02" ), resultSet.getTime( 2 ) );

            // timestamp
            assertEquals( java.sql.Timestamp.class, resultSet.getTimestamp( 3 ).getClass() );
            assertEquals( java.sql.Timestamp.valueOf( "1996-08-03 00:01:02" ), resultSet.getTimestamp( 3 ) );

        }
    }


    /**
     * Test case for "CSV adapter incorrectly parses TIMESTAMP values after noon".
     */
    @Test
    public void testDateType2() throws SQLException {
        Properties info = new Properties();
        info.put( "model", jsonPath( "bug" ) );

        try ( Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info ) ) {
            Statement statement = connection.createStatement();
            final String sql = "select * from \"DATE\"\n" + "where EMPNO >= 140 and EMPNO < 200";
            ResultSet resultSet = statement.executeQuery( sql );
            int n = 0;
            while ( resultSet.next() ) {
                ++n;
                final int empId = resultSet.getInt( 1 );
                final String date = resultSet.getString( 2 );
                final String time = resultSet.getString( 3 );
                final String timestamp = resultSet.getString( 4 );
                assertThat( date, is( "2015-12-31" ) );
                switch ( empId ) {
                    case 140:
                        assertThat( time, is( "07:15:56" ) );
                        assertThat( timestamp, is( "2015-12-31 07:15:56" ) );
                        break;
                    case 150:
                        assertThat( time, is( "13:31:21" ) );
                        assertThat( timestamp, is( "2015-12-31 13:31:21" ) );
                        break;
                    default:
                        throw new AssertionError();
                }
            }
            assertThat( n, is( 2 ) );
            resultSet.close();
            statement.close();
        }
    }


    /**
     * Test case for "Query with ORDER BY or GROUP BY on TIMESTAMP column throws CompileException".
     */
    @Test
    public void testTimestampGroupBy() throws SQLException {
        Properties info = new Properties();
        info.put( "model", jsonPath( "bug" ) );
        // Use LIMIT to ensure that results are deterministic without ORDER BY
        final String sql = """
                select "EMPNO", "JOINTIMES"
                from (select * from "DATE" limit 1)
                group by "EMPNO","JOINTIMES\"""";
        try (
                Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info );
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery( sql )
        ) {
            assertThat( resultSet.next(), is( true ) );
            final Timestamp timestamp = resultSet.getTimestamp( 2 );
            assertThat( timestamp, isA( java.sql.Timestamp.class ) );
            // Note: This logic is time zone specific, but the same time zone is used in the CSV adapter and this test, so they should cancel out.
            assertThat( timestamp, is( java.sql.Timestamp.valueOf( "1996-08-03 00:01:02.0" ) ) );
        }
    }


    /**
     * As {@link #testTimestampGroupBy()} but with ORDER BY.
     */
    @Test
    public void testTimestampOrderBy() throws SQLException {
        Properties info = new Properties();
        info.put( "model", jsonPath( "bug" ) );
        final String sql = "select \"EMPNO\",\"JOINTIMES\" from \"DATE\"\n" + "order by \"JOINTIMES\"";
        try (
                Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info );
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery( sql )
        ) {
            assertThat( resultSet.next(), is( true ) );
            final Timestamp timestamp = resultSet.getTimestamp( 2 );
            assertThat( timestamp, is( java.sql.Timestamp.valueOf( "1996-08-03 00:01:02" ) ) );
        }
    }


    /**
     * As {@link #testTimestampGroupBy()} but with ORDER BY as well as GROUP BY.
     */
    @Test
    public void testTimestampGroupByAndOrderBy() throws SQLException {
        Properties info = new Properties();
        info.put( "model", jsonPath( "bug" ) );
        final String sql = "select \"EMPNO\", \"JOINTIMES\" from \"DATE\"\n" + "group by \"EMPNO\",\"JOINTIMES\" order by \"JOINTIMES\"";
        try (
                Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info );
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery( sql )
        ) {
            assertThat( resultSet.next(), is( true ) );
            final Timestamp timestamp = resultSet.getTimestamp( 2 );
            assertThat( timestamp, is( java.sql.Timestamp.valueOf( "1996-08-03 00:01:02" ) ) );
        }
    }


    /**
     * Test case for "NPE caused by wrong code generation for Timestamp fields".
     */
    @Test
    public void testFilterOnNullableTimestamp() throws Exception {
        Properties info = new Properties();
        info.put( "model", jsonPath( "bug" ) );

        try ( Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info ) ) {
            final Statement statement = connection.createStatement();

            // date
            final String sql1 = """
                    select JOINEDAT from "DATE"
                    where JOINEDAT < {d '2000-01-01'}
                    or JOINEDAT >= {d '2017-01-01'}""";
            final ResultSet joinedAt = statement.executeQuery( sql1 );
            assertThat( joinedAt.next(), is( true ) );
            assertThat( joinedAt.getDate( 1 ), is( java.sql.Date.valueOf( "1996-08-03" ) ) );

            // time
            final String sql2 = """
                    select JOINTIME from "DATE"
                    where JOINTIME >= {t '07:00:00'}
                    and JOINTIME < {t '08:00:00'}""";
            final ResultSet joinTime = statement.executeQuery( sql2 );
            assertThat( joinTime.next(), is( true ) );
            assertThat( joinTime.getTime( 1 ), is( java.sql.Time.valueOf( "07:15:56" ) ) );

            // timestamp
            final String sql3 = """
                    select JOINTIMES,
                      {fn timestampadd(SQL_TSI_DAY, 1, JOINTIMES)}
                    from "DATE"
                    where (JOINTIMES >= {ts '2003-01-01 00:00:00'}
                    and JOINTIMES < {ts '2006-01-01 00:00:00'})
                    or (JOINTIMES >= {ts '2003-01-01 00:00:00'}
                    and JOINTIMES < {ts '2007-01-01 00:00:00'})""";
            final ResultSet joinTimes = statement.executeQuery( sql3 );
            assertThat( joinTimes.next(), is( true ) );
            assertThat( joinTimes.getTimestamp( 1 ), is( java.sql.Timestamp.valueOf( "2005-09-07 00:00:00" ) ) );
            assertThat( joinTimes.getTimestamp( 2 ), is( java.sql.Timestamp.valueOf( "2005-09-08 00:00:00" ) ) );

            final String sql4 = "select JOINTIMES, extract(year from JOINTIMES)\n" + "from \"DATE\"";
            final ResultSet joinTimes2 = statement.executeQuery( sql4 );
            assertThat( joinTimes2.next(), is( true ) );
            assertThat( joinTimes2.getTimestamp( 1 ), is( java.sql.Timestamp.valueOf( "1996-08-03 00:01:02" ) ) );
        }
    }


    /**
     * Test case for "NullPointerException in EXTRACT with WHERE ... IN clause if field has null value".
     */
    @Test
    public void testFilterOnNullableTimestamp2() throws Exception {
        Properties info = new Properties();
        info.put( "model", jsonPath( "bug" ) );

        try ( Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info ) ) {
            final Statement statement = connection.createStatement();
            final String sql1 = """
                    select extract(year from JOINTIMES)
                    from "DATE"
                    where extract(year from JOINTIMES) in (2006, 2007)""";
            final ResultSet joinTimes = statement.executeQuery( sql1 );
            assertThat( joinTimes.next(), is( true ) );
            assertThat( joinTimes.getInt( 1 ), is( 2007 ) );

            final String sql2 = """
                    select extract(year from JOINTIMES),
                      count(0) from "DATE"
                    where extract(year from JOINTIMES) between 2007 and 2016
                    group by extract(year from JOINTIMES)""";
            final ResultSet joinTimes2 = statement.executeQuery( sql2 );
            assertThat( joinTimes2.next(), is( true ) );
            assertThat( joinTimes2.getInt( 1 ), is( 2007 ) );
            assertThat( joinTimes2.getLong( 2 ), is( 1L ) );
            assertThat( joinTimes2.next(), is( true ) );
            assertThat( joinTimes2.getInt( 1 ), is( 2015 ) );
            assertThat( joinTimes2.getLong( 2 ), is( 2L ) );
        }
    }


    /**
     * Test case for "Code generation incorrect (does not compile) for DATE, TIME and TIMESTAMP fields".
     */
    @Test
    public void testNonNullFilterOnDateType() throws SQLException {
        Properties info = new Properties();
        info.put( "model", jsonPath( "bug" ) );

        try ( Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info ) ) {
            final Statement statement = connection.createStatement();

            // date
            final String sql1 = "select JOINEDAT from \"DATE\"\n" + "where JOINEDAT is not null";
            final ResultSet joinedAt = statement.executeQuery( sql1 );
            assertThat( joinedAt.next(), is( true ) );
            assertThat( joinedAt.getDate( 1 ).getClass(), equalTo( java.sql.Date.class ) );
            assertThat( joinedAt.getDate( 1 ), is( java.sql.Date.valueOf( "1996-08-03" ) ) );

            // time
            final String sql2 = "select JOINTIME from \"DATE\"\n" + "where JOINTIME is not null";
            final ResultSet joinTime = statement.executeQuery( sql2 );
            assertThat( joinTime.next(), is( true ) );
            assertThat( joinTime.getTime( 1 ).getClass(), equalTo( java.sql.Time.class ) );
            assertThat( joinTime.getTime( 1 ), is( java.sql.Time.valueOf( "00:01:02" ) ) );

            // timestamp
            final String sql3 = "select JOINTIMES from \"DATE\"\n" + "where JOINTIMES is not null";
            final ResultSet joinTimes = statement.executeQuery( sql3 );
            assertThat( joinTimes.next(), is( true ) );
            assertThat( joinTimes.getTimestamp( 1 ).getClass(), equalTo( java.sql.Timestamp.class ) );
            assertThat( joinTimes.getTimestamp( 1 ), is( java.sql.Timestamp.valueOf( "1996-08-03 00:01:02" ) ) );
        }
    }


    /**
     * Test case for "Code generation incorrect (does not compile) for DATE, TIME and TIMESTAMP fields".
     */
    @Test
    public void testGreaterThanFilterOnDateType() throws SQLException {
        Properties info = new Properties();
        info.put( "model", jsonPath( "bug" ) );

        try ( Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info ) ) {
            final Statement statement = connection.createStatement();

            // date
            final String sql1 = "select JOINEDAT from \"DATE\"\n" + "where JOINEDAT > {d '1990-01-01'}";
            final ResultSet joinedAt = statement.executeQuery( sql1 );
            assertThat( joinedAt.next(), is( true ) );
            assertThat( joinedAt.getDate( 1 ).getClass(), equalTo( java.sql.Date.class ) );
            assertThat( joinedAt.getDate( 1 ), is( java.sql.Date.valueOf( "1996-08-03" ) ) );

            // time
            final String sql2 = "select JOINTIME from \"DATE\"\n" + "where JOINTIME > {t '00:00:00'}";
            final ResultSet joinTime = statement.executeQuery( sql2 );
            assertThat( joinTime.next(), is( true ) );
            assertThat( joinTime.getTime( 1 ).getClass(), equalTo( java.sql.Time.class ) );
            assertThat( joinTime.getTime( 1 ), is( java.sql.Time.valueOf( "00:01:02" ) ) );

            // timestamp
            final String sql3 = "select JOINTIMES from \"DATE\"\n" + "where JOINTIMES > {ts '1990-01-01 00:00:00'}";
            final ResultSet joinTimes = statement.executeQuery( sql3 );
            assertThat( joinTimes.next(), is( true ) );
            assertThat( joinTimes.getTimestamp( 1 ).getClass(), equalTo( java.sql.Timestamp.class ) );
            assertThat( joinTimes.getTimestamp( 1 ), is( java.sql.Timestamp.valueOf( "1996-08-03 00:01:02" ) ) );
        }
    }


    /**
     * Creates a command that appends a line to the CSV file.
     */
    private Callable<Void> writeLine( final PrintWriter pw, final String line ) {
        return () -> {
            pw.println( line );
            pw.flush();
            return null;
        };
    }


    /**
     * Creates a command that sleeps.
     */
    private Callable<Void> sleep( final long millis ) {
        return () -> {
            Thread.sleep( millis );
            return null;
        };
    }


    /**
     * Creates a command that cancels a statement.
     */
    private Callable<Void> cancel( final Statement statement ) {
        return () -> {
            statement.cancel();
            return null;
        };
    }


    private Void output( ResultSet resultSet ) {
        try {
            output( resultSet, System.out );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
        return null;
    }


    /**
     * Fluent API to perform test actions.
     */
    private class Fluent {

        private final String model;
        private final String sql;
        private final Consumer<ResultSet> expect;


        Fluent( String model, String sql, Consumer<ResultSet> expect ) {
            this.model = model;
            this.sql = sql;
            this.expect = expect;
        }


        /**
         * Runs the test.
         */
        Fluent ok() {
            try {
                checkSql( sql, model, expect );
                return this;
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        }


        /**
         * Assigns a function to call to test whether output is correct.
         */
        Fluent checking( Consumer<ResultSet> expect ) {
            return new Fluent( model, sql, expect );
        }


        /**
         * Sets the rows that are expected to be returned from the SQL query.
         */
        Fluent returns( String... expectedLines ) {
            return checking( expect( expectedLines ) );
        }


        /**
         * Sets the rows that are expected to be returned from the SQL query,
         * in no particular order.
         */
        Fluent returnsUnordered( String... expectedLines ) {
            return checking( expectUnordered( expectedLines ) );
        }

    }

}

