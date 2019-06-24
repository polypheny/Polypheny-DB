/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.test;


import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionProperty;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbEmbeddedConnection;
import ch.unibas.dmi.dbis.polyphenydb.schema.ScannableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.TableFunction;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.TableFunctionImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformanceEnum;
import ch.unibas.dmi.dbis.polyphenydb.util.Smalls;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;


/**
 * Tests for user-defined table functions.
 *
 * @see UdfTest
 * @see Smalls
 */
public class TableFunctionTest {

    private PolyphenyDbAssert.AssertThat with() {
        final String c = Smalls.class.getName();
        final String m = Smalls.MULTIPLICATION_TABLE_METHOD.getName();
        final String m2 = Smalls.FIBONACCI_TABLE_METHOD.getName();
        final String m3 = Smalls.FIBONACCI2_TABLE_METHOD.getName();
        return PolyphenyDbAssert.model( "{\n"
                + "  version: '1.0',\n"
                + "   schemas: [\n"
                + "     {\n"
                + "       name: 's',\n"
                + "       functions: [\n"
                + "         {\n"
                + "           name: 'multiplication',\n"
                + "           className: '" + c + "',\n"
                + "           methodName: '" + m + "'\n"
                + "         }, {\n"
                + "           name: 'fibonacci',\n"
                + "           className: '" + c + "',\n"
                + "           methodName: '" + m2 + "'\n"
                + "         }, {\n"
                + "           name: 'fibonacci2',\n"
                + "           className: '" + c + "',\n"
                + "           methodName: '" + m3 + "'\n"
                + "         }\n"
                + "       ]\n"
                + "     }\n"
                + "   ]\n"
                + "}" )
                .withDefaultSchema( "s" );
    }


    /**
     * Tests a table function with literal arguments.
     */
    @Test
    public void testTableFunction() throws SQLException {
        try ( Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" ) ) {
            PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
            SchemaPlus rootSchema = polyphenyDbEmbeddedConnection.getRootSchema();
            SchemaPlus schema = rootSchema.add( "s", new AbstractSchema() );
            final TableFunction table = TableFunctionImpl.create( Smalls.GENERATE_STRINGS_METHOD );
            schema.add( "GenerateStrings", table );
            final String sql = "select *\n" + "from table(\"s\".\"GenerateStrings\"(5)) as t(n, c)\n" + "where char_length(c) > 3";
            ResultSet resultSet = connection.createStatement().executeQuery( sql );
            assertThat( PolyphenyDbAssert.toString( resultSet ), equalTo( "N=4; C=abcd\n" ) );
        }
    }


    /**
     * Tests a table function that implements {@link ScannableTable} and returns a single column.
     */
    @Test
    public void testScannableTableFunction() throws SQLException, ClassNotFoundException {
        Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" );
        PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
        SchemaPlus rootSchema = polyphenyDbEmbeddedConnection.getRootSchema();
        SchemaPlus schema = rootSchema.add( "s", new AbstractSchema() );
        final TableFunction table = TableFunctionImpl.create( Smalls.MAZE_METHOD );
        schema.add( "Maze", table );
        final String sql = "select *\n" + "from table(\"s\".\"Maze\"(5, 3, 1))";
        ResultSet resultSet = connection.createStatement().executeQuery( sql );
        final String result = "S=abcde\n" + "S=xyz\n" + "S=generate(w=5, h=3, s=1)\n";
        assertThat( PolyphenyDbAssert.toString( resultSet ), is( result ) );
    }


    /**
     * As {@link #testScannableTableFunction()} but with named parameters.
     */
    @Test
    public void testScannableTableFunctionWithNamedParameters() throws SQLException, ClassNotFoundException {
        Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" );
        PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
        SchemaPlus rootSchema = polyphenyDbEmbeddedConnection.getRootSchema();
        SchemaPlus schema = rootSchema.add( "s", new AbstractSchema() );
        final TableFunction table = TableFunctionImpl.create( Smalls.MAZE2_METHOD );
        schema.add( "Maze", table );
        final String sql = "select *\n" + "from table(\"s\".\"Maze\"(5, 3, 1))";
        final Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery( sql );
        final String result = "S=abcde\n" + "S=xyz\n";
        assertThat( PolyphenyDbAssert.toString( resultSet ), is( result + "S=generate2(w=5, h=3, s=1)\n" ) );

        final String sql2 = "select *\n" + "from table(\"s\".\"Maze\"(WIDTH => 5, HEIGHT => 3, SEED => 1))";
        resultSet = statement.executeQuery( sql2 );
        assertThat( PolyphenyDbAssert.toString( resultSet ), is( result + "S=generate2(w=5, h=3, s=1)\n" ) );

        final String sql3 = "select *\n" + "from table(\"s\".\"Maze\"(HEIGHT => 3, WIDTH => 5))";
        resultSet = statement.executeQuery( sql3 );
        assertThat( PolyphenyDbAssert.toString( resultSet ), is( result + "S=generate2(w=5, h=3, s=null)\n" ) );
        connection.close();
    }


    /**
     * As {@link #testScannableTableFunction()} but with named parameters.
     */
    @Test
    public void testMultipleScannableTableFunctionWithNamedParameters() throws SQLException, ClassNotFoundException {
        try (
                Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" );
                Statement statement = connection.createStatement()
        ) {
            PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
            SchemaPlus rootSchema = polyphenyDbEmbeddedConnection.getRootSchema();
            SchemaPlus schema = rootSchema.add( "s", new AbstractSchema() );
            final TableFunction table1 = TableFunctionImpl.create( Smalls.MAZE_METHOD );
            schema.add( "Maze", table1 );
            final TableFunction table2 = TableFunctionImpl.create( Smalls.MAZE2_METHOD );
            schema.add( "Maze", table2 );
            final TableFunction table3 = TableFunctionImpl.create( Smalls.MAZE3_METHOD );
            schema.add( "Maze", table3 );
            final String sql = "select *\n" + "from table(\"s\".\"Maze\"(5, 3, 1))";
            ResultSet resultSet = statement.executeQuery( sql );
            final String result = "S=abcde\n" + "S=xyz\n";
            assertThat( PolyphenyDbAssert.toString( resultSet ), is( result + "S=generate(w=5, h=3, s=1)\n" ) );

            final String sql2 = "select *\n" + "from table(\"s\".\"Maze\"(WIDTH => 5, HEIGHT => 3, SEED => 1))";
            resultSet = statement.executeQuery( sql2 );
            assertThat( PolyphenyDbAssert.toString( resultSet ), is( result + "S=generate2(w=5, h=3, s=1)\n" ) );

            final String sql3 = "select *\n" + "from table(\"s\".\"Maze\"(HEIGHT => 3, WIDTH => 5))";
            resultSet = statement.executeQuery( sql3 );
            assertThat( PolyphenyDbAssert.toString( resultSet ), is( result + "S=generate2(w=5, h=3, s=null)\n" ) );

            final String sql4 = "select *\n" + "from table(\"s\".\"Maze\"(FOO => 'a'))";
            resultSet = statement.executeQuery( sql4 );
            assertThat( PolyphenyDbAssert.toString( resultSet ), is( result + "S=generate3(foo=a)\n" ) );
        }
    }


    /**
     * Tests a table function that returns different row type based on actual call arguments.
     */
    @Test
    public void testTableFunctionDynamicStructure() throws SQLException, ClassNotFoundException {
        Connection connection = getConnectionWithMultiplyFunction();
        final PreparedStatement ps = connection.prepareStatement( "select *\n" + "from table(\"s\".\"multiplication\"(4, 3, ?))\n" );
        ps.setInt( 1, 100 );
        ResultSet resultSet = ps.executeQuery();
        assertThat(
                PolyphenyDbAssert.toString( resultSet ),
                equalTo( "row_name=row 0; c1=101; c2=102; c3=103; c4=104\n" + "row_name=row 1; c1=102; c2=104; c3=106; c4=108\n" + "row_name=row 2; c1=103; c2=106; c3=109; c4=112\n" ) );
    }


    /**
     * Tests that non-nullable arguments of a table function must be provided as literals.
     */
    @Ignore("SQLException does not include message from nested exception")
    @Test
    public void testTableFunctionNonNullableMustBeLiterals() throws SQLException, ClassNotFoundException {
        Connection connection = getConnectionWithMultiplyFunction();
        try {
            final PreparedStatement ps = connection.prepareStatement( "select *\n" + "from table(\"s\".\"multiplication\"(?, 3, 100))\n" );
            ps.setInt( 1, 100 );
            ResultSet resultSet = ps.executeQuery();
            fail( "Should fail, got " + resultSet );
        } catch ( SQLException e ) {
            assertThat( e.getMessage(),
                    containsString( "Wrong arguments for table function 'public static ch.unibas.dmi.dbis.polyphenydb.schema.QueryableTable ch.unibas.dmi.dbis.polyphenydb.test.JdbcTest.multiplicationTable(int,int,java.lang.Integer)'"
                            + " call. Expected '[int, int, class java.lang.Integer]', actual '[null, 3, 100]'" ) );
        }
    }


    private Connection getConnectionWithMultiplyFunction() throws ClassNotFoundException, SQLException {
        Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" );
        PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
        SchemaPlus rootSchema = polyphenyDbEmbeddedConnection.getRootSchema();
        SchemaPlus schema = rootSchema.add( "s", new AbstractSchema() );
        final TableFunction table = TableFunctionImpl.create( Smalls.MULTIPLICATION_TABLE_METHOD );
        schema.add( "multiplication", table );
        return connection;
    }


    /**
     * Tests a table function that takes cursor input.
     */
    @Ignore("CannotPlanException: Node [rel#18:Subset#4.ENUMERABLE.[]] could not be implemented")
    @Test
    public void testTableFunctionCursorInputs() throws SQLException, ClassNotFoundException {
        try (
                Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" )
        ) {
            PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
            SchemaPlus rootSchema = polyphenyDbEmbeddedConnection.getRootSchema();
            SchemaPlus schema = rootSchema.add( "s", new AbstractSchema() );
            final TableFunction table = TableFunctionImpl.create( Smalls.GENERATE_STRINGS_METHOD );
            schema.add( "GenerateStrings", table );
            final TableFunction add = TableFunctionImpl.create( Smalls.PROCESS_CURSOR_METHOD );
            schema.add( "process", add );
            final PreparedStatement ps = connection.prepareStatement( "select *\n"
                    + "from table(\"s\".\"process\"(2,\n"
                    + "cursor(select * from table(\"s\".\"GenerateStrings\"(?)))\n"
                    + ")) as t(u)\n"
                    + "where u > 3" );
            ps.setInt( 1, 5 );
            ResultSet resultSet = ps.executeQuery();
            // GenerateStrings returns 0..4, then 2 is added (process function), thus 2..6, finally where u > 3 leaves just 4..6
            assertThat( PolyphenyDbAssert.toString( resultSet ), equalTo( "u=4\n" + "u=5\n" + "u=6\n" ) );
        }
    }


    /**
     * Tests a table function that takes multiple cursor inputs.
     */
    @Ignore("CannotPlanException: Node [rel#24:Subset#6.ENUMERABLE.[]] could not be implemented")
    @Test
    public void testTableFunctionCursorsInputs() throws SQLException, ClassNotFoundException {
        try (
                Connection connection = getConnectionWithMultiplyFunction()
        ) {
            PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
            SchemaPlus rootSchema = polyphenyDbEmbeddedConnection.getRootSchema();
            SchemaPlus schema = rootSchema.getSubSchema( "s" );
            final TableFunction table = TableFunctionImpl.create( Smalls.GENERATE_STRINGS_METHOD );
            schema.add( "GenerateStrings", table );
            final TableFunction add = TableFunctionImpl.create( Smalls.PROCESS_CURSORS_METHOD );
            schema.add( "process", add );
            final PreparedStatement ps = connection.prepareStatement( "select *\n"
                    + "from table(\"s\".\"process\"(2,\n"
                    + "cursor(select * from table(\"s\".\"multiplication\"(5,5,0))),\n"
                    + "cursor(select * from table(\"s\".\"GenerateStrings\"(?)))\n"
                    + ")) as t(u)\n"
                    + "where u > 3" );
            ps.setInt( 1, 5 );
            ResultSet resultSet = ps.executeQuery();
            // GenerateStrings produce 0..4
            // multiplication produce 1..5
            // process sums and adds 2
            // sum is 2 + 1..9 == 3..9
            assertThat( PolyphenyDbAssert.toString( resultSet ), equalTo( "u=4\n" + "u=5\n" + "u=6\n" + "u=7\n" + "u=8\n" + "u=9\n" ) );
        }
    }


    @Test
    public void testUserDefinedTableFunction() {
        final String q = "select *\nfrom table(\"s\".\"multiplication\"(2, 3, 100))\n";
        with().query( q ).returnsUnordered( "row_name=row 0; c1=101; c2=102", "row_name=row 1; c1=102; c2=104", "row_name=row 2; c1=103; c2=106" );
    }


    @Test
    public void testUserDefinedTableFunction2() {
        final String q = "select c1\n"
                + "from table(\"s\".\"multiplication\"(2, 3, 100))\n"
                + "where c1 + 2 < c2";
        final boolean oldCaseSensitiveValue = RuntimeConfig.CASE_SENSITIVE.getBoolean();
        try {
            RuntimeConfig.CASE_SENSITIVE.setBoolean( true );
            with().query( q ).throws_( "Column 'C1' not found in any table; did you mean 'c1'?" );
        } finally {
            RuntimeConfig.CASE_SENSITIVE.setBoolean( oldCaseSensitiveValue );
        }
    }


    @Test
    public void testUserDefinedTableFunction3() {
        final String q = "select \"c1\"\n"
                + "from table(\"s\".\"multiplication\"(2, 3, 100))\n"
                + "where \"c1\" + 2 < \"c2\"";
        with().query( q ).returnsUnordered( "c1=103" );
    }


    @Test
    public void testUserDefinedTableFunction4() {
        final String q = "select *\n"
                + "from table(\"s\".\"multiplication\"('2', 3, 100))\n"
                + "where c1 + 2 < c2";
        final String e = "No match found for function signature multiplication(<CHARACTER>, <NUMERIC>, <NUMERIC>)";
        with().query( q ).throws_( e );
    }


    @Test
    public void testUserDefinedTableFunction5() {
        final String q = "select *\n"
                + "from table(\"s\".\"multiplication\"(3, 100))\n"
                + "where c1 + 2 < c2";
        final String e = "No match found for function signature multiplication(<NUMERIC>, <NUMERIC>)";
        with().query( q ).throws_( e );
    }


    @Test
    public void testUserDefinedTableFunction6() {
        final String q = "select *\n"
                + "from table(\"s\".\"fibonacci\"())";
        with().query( q )
                .returns( r -> {
                    try {
                        final List<Long> numbers = new ArrayList<>();
                        while ( r.next() && numbers.size() < 13 ) {
                            numbers.add( r.getLong( 1 ) );
                        }
                        assertThat( numbers.toString(), is( "[1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233]" ) );
                    } catch ( SQLException e ) {
                        throw new RuntimeException( e );
                    }
                } );
    }


    @Test
    public void testUserDefinedTableFunction7() {
        final String q = "select *\n"
                + "from table(\"s\".\"fibonacci2\"(20))\n"
                + "where n > 7";
        with().query( q ).returnsUnordered( "N=13", "N=8" );
    }


    @Test
    public void testUserDefinedTableFunction8() {
        final String q = "select count(*) as c\n"
                + "from table(\"s\".\"fibonacci2\"(20))";
        with().query( q ).returnsUnordered( "C=7" );
    }


    @Test
    public void testCrossApply() {
        final String q1 = "select *\n"
                + "from (values 2, 5) as t (c)\n"
                + "cross apply table(\"s\".\"fibonacci2\"(c))";
        final String q2 = "select *\n"
                + "from (values 2, 5) as t (c)\n"
                + "cross apply table(\"s\".\"fibonacci2\"(t.c))";
        for ( String q : new String[]{ q1, q2 } ) {
            with()
                    .with( PolyphenyDbConnectionProperty.CONFORMANCE, SqlConformanceEnum.LENIENT )
                    .query( q )
                    .returnsUnordered( "C=2; N=1", "C=2; N=1", "C=2; N=2", "C=5; N=1", "C=5; N=1", "C=5; N=2", "C=5; N=3", "C=5; N=5" );
        }
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-2382">[POLYPHENYDB-2382] Sub-query lateral joined to table function</a>.
     */
    @Test
    public void testInlineViewLateralTableFunction() throws SQLException {
        try (
                Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" )
        ) {
            PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
            SchemaPlus rootSchema = polyphenyDbEmbeddedConnection.getRootSchema();
            SchemaPlus schema = rootSchema.add( "s", new AbstractSchema() );
            final TableFunction table = TableFunctionImpl.create( Smalls.GENERATE_STRINGS_METHOD );
            schema.add( "GenerateStrings", table );
            Table tbl = new ScannableTableTest.SimpleTable();
            schema.add( "t", tbl );

            final String sql = "select *\n"
                    + "from (select 5 as f0 from \"s\".\"t\") \"a\",\n"
                    + "  lateral table(\"s\".\"GenerateStrings\"(f0)) as t(n, c)\n"
                    + "where char_length(c) > 3";
            ResultSet resultSet = connection.createStatement().executeQuery( sql );
            final String expected = "F0=5; N=4; C=abcd\n"
                    + "F0=5; N=4; C=abcd\n"
                    + "F0=5; N=4; C=abcd\n"
                    + "F0=5; N=4; C=abcd\n";
            assertThat( PolyphenyDbAssert.toString( resultSet ), equalTo( expected ) );
        }
    }
}

