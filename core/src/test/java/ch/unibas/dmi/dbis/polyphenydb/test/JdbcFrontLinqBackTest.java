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


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTableQueryable;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.junit.Ignore;
import org.junit.Test;


/**
 * Tests for a JDBC front-end (with some quite complex SQL) and Linq4j back-end (based on in-memory collections).
 */
public class JdbcFrontLinqBackTest {

    /**
     * Runs a simple query that reads from a table in an in-memory schema.
     */
    @Test
    public void testSelect() {
        PolyphenyDbAssert.hr()
                .query( "select *\n"
                        + "from \"foodmart\".\"sales_fact_1997\" as s\n"
                        + "where s.\"cust_id\" = 100" )
                .returns( "cust_id=100; prod_id=10\n" );
    }


    /**
     * Runs a simple query that joins between two in-memory schemas.
     */
    @Test
    public void testJoin() {
        PolyphenyDbAssert.hr()
                .query( "select *\n"
                        + "from \"foodmart\".\"sales_fact_1997\" as s\n"
                        + "join \"hr\".\"emps\" as e\n"
                        + "on e.\"empid\" = s.\"cust_id\"" )
                .returnsUnordered(
                        "cust_id=100; prod_id=10; empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000",
                        "cust_id=150; prod_id=20; empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null" );
    }


    /**
     * Simple GROUP BY.
     */
    @Test
    public void testGroupBy() {
        PolyphenyDbAssert.hr()
                .query( "select \"deptno\", sum(\"empid\") as s, count(*) as c\n"
                        + "from \"hr\".\"emps\" as e\n"
                        + "group by \"deptno\"" )
                .returns( "deptno=20; S=200; C=1\n"
                        + "deptno=10; S=360; C=3\n" );
    }


    /**
     * Simple ORDER BY.
     */
    @Test
    public void testOrderBy() {
        PolyphenyDbAssert.hr()
                .query( "select upper(\"name\") as un, \"deptno\"\n"
                        + "from \"hr\".\"emps\" as e\n"
                        + "order by \"deptno\", \"name\" desc" )
                .explainContains( ""
                        + "EnumerableSort(sort0=[$1], sort1=[$2], dir0=[ASC], dir1=[DESC])\n"
                        + "  EnumerableCalc(expr#0..4=[{inputs}], expr#5=[UPPER($t2)], UN=[$t5], deptno=[$t1], name=[$t2])\n"
                        + "    EnumerableTableScan(table=[[hr, emps]])" )
                .returns( "UN=THEODORE; deptno=10\n"
                        + "UN=SEBASTIAN; deptno=10\n"
                        + "UN=BILL; deptno=10\n"
                        + "UN=ERIC; deptno=20\n" );
    }


    /**
     * Simple UNION, plus ORDER BY.
     *
     * Also tests a query that returns a single column. We optimize this case internally, using non-array representations for rows.
     */
    @Test
    public void testUnionAllOrderBy() {
        PolyphenyDbAssert.hr()
                .query( "select \"name\"\n"
                        + "from \"hr\".\"emps\" as e\n"
                        + "union all\n"
                        + "select \"name\"\n"
                        + "from \"hr\".\"depts\"\n"
                        + "order by 1 desc" )
                .returns( "name=Theodore\n"
                        + "name=Sebastian\n"
                        + "name=Sales\n"
                        + "name=Marketing\n"
                        + "name=HR\n"
                        + "name=Eric\n"
                        + "name=Bill\n" );
    }


    /**
     * Tests UNION.
     */
    @Test
    public void testUnion() {
        PolyphenyDbAssert.hr()
                .query( "select substring(\"name\" from 1 for 1) as x\n"
                        + "from \"hr\".\"emps\" as e\n"
                        + "union\n"
                        + "select substring(\"name\" from 1 for 1) as y\n"
                        + "from \"hr\".\"depts\"" )
                .returnsUnordered(
                        "X=T",
                        "X=E",
                        "X=S",
                        "X=B",
                        "X=M",
                        "X=H" );
    }


    /**
     * Tests INTERSECT.
     */
    @Test
    public void testIntersect() {
        PolyphenyDbAssert.hr()
                .query( "select substring(\"name\" from 1 for 1) as x\n"
                        + "from \"hr\".\"emps\" as e\n"
                        + "intersect\n"
                        + "select substring(\"name\" from 1 for 1) as y\n"
                        + "from \"hr\".\"depts\"" )
                .returns( "X=S\n" );
    }


    /**
     * Tests EXCEPT.
     */
    @Ignore
    @Test
    public void testExcept() {
        PolyphenyDbAssert.hr()
                .query( "select substring(\"name\" from 1 for 1) as x\n"
                        + "from \"hr\".\"emps\" as e\n"
                        + "except\n"
                        + "select substring(\"name\" from 1 for 1) as y\n"
                        + "from \"hr\".\"depts\"" )
                .returnsUnordered(
                        "X=T",
                        "X=E",
                        "X=B" );
    }


    @Test
    public void testWhereBad() {
        PolyphenyDbAssert.hr()
                .query( "select *\n"
                        + "from \"foodmart\".\"sales_fact_1997\" as s\n"
                        + "where empid > 120" )
                .throws_( "Column 'EMPID' not found in any table" );
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-9">[POLYPHENYDB-9] RexToLixTranslator not incrementing local variable name counter</a>.
     */
    @Test
    public void testWhereOr() {
        PolyphenyDbAssert.hr()
                .query( "select * from \"hr\".\"emps\"\n"
                        + "where (\"empid\" = 100 or \"empid\" = 200)\n"
                        + "and \"deptno\" = 10" )
                .returns(
                        "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n" );
    }


    @Test
    public void testWhereLike() {
        PolyphenyDbAssert.hr()
                .query( "select *\n"
                        + "from \"hr\".\"emps\" as e\n"
                        + "where e.\"empid\" < 120 or e.\"name\" like 'S%'" )
                .returns( ""
                        + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
                        + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
                        + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n" );
    }


    @Test
    public void testInsert() {
        final List<JdbcTest.Employee> employees = new ArrayList<>();
        PolyphenyDbAssert.AssertThat with = mutable( employees );
        with.query( "select * from \"foo\".\"bar\"" )
                .returns( "empid=0; deptno=0; name=first; salary=0.0; commission=null\n" );
        with.query( "insert into \"foo\".\"bar\" select * from \"hr\".\"emps\"" )
                .updates( 4 );
        with.query( "select count(*) as c from \"foo\".\"bar\"" )
                .returns( "C=5\n" );
        with.query( "insert into \"foo\".\"bar\" select * from \"hr\".\"emps\" where \"deptno\" = 10" )
                .updates( 3 );
        with.query( "select \"name\", count(*) as c from \"foo\".\"bar\" "
                + "group by \"name\"" )
                .returnsUnordered(
                        "name=Bill; C=2",
                        "name=Eric; C=1",
                        "name=Theodore; C=2",
                        "name=first; C=1",
                        "name=Sebastian; C=2" );
    }


    @Test
    public void testInsertBind() throws Exception {
        final List<JdbcTest.Employee> employees = new ArrayList<>();
        PolyphenyDbAssert.AssertThat with = mutable( employees );
        with.query( "select count(*) as c from \"foo\".\"bar\"" ).returns( "C=1\n" );
        with.doWithConnection( c -> {
            try {
                final String sql = "insert into \"foo\".\"bar\"\n" + "values (?, 0, ?, 10.0, null)";
                try ( PreparedStatement p = c.prepareStatement( sql ) ) {
                    p.setInt( 1, 1 );
                    p.setString( 2, "foo" );
                    final int count = p.executeUpdate();
                    assertThat( count, is( 1 ) );
                }
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        } );
        with.query( "select count(*) as c from \"foo\".\"bar\"" )
                .returns( "C=2\n" );
        with.query( "select * from \"foo\".\"bar\"" )
                .returnsUnordered( "empid=0; deptno=0; name=first; salary=0.0; commission=null", "empid=1; deptno=0; name=foo; salary=10.0; commission=null" );
    }


    @Test
    public void testDelete() {
        final List<JdbcTest.Employee> employees = new ArrayList<>();
        PolyphenyDbAssert.AssertThat with = mutable( employees );
        with.query( "select * from \"foo\".\"bar\"" )
                .returnsUnordered( "empid=0; deptno=0; name=first; salary=0.0; commission=null" );
        with.query( "insert into \"foo\".\"bar\" select * from \"hr\".\"emps\"" )
                .updates( 4 );
        with.query( "select count(*) as c from \"foo\".\"bar\"" )
                .returnsUnordered( "C=5" );
        final String deleteSql = "delete from \"foo\".\"bar\" "
                + "where \"deptno\" = 10";
        with.query( deleteSql )
                .updates( 3 );
        final String sql = "select \"name\", count(*) as c\n"
                + "from \"foo\".\"bar\"\n"
                + "group by \"name\"";
        with.query( sql )
                .returnsUnordered( "name=Eric; C=1", "name=first; C=1" );
    }


    /**
     * Creates the post processor routine to be applied against a Connection.
     *
     * Table schema is based on JdbcTest#Employee (refer to {@link JdbcFrontLinqBackTest#mutable}).
     *
     * @param initialData records to be presented in table
     * @return a connection post-processor
     */
    private static PolyphenyDbAssert.ConnectionPostProcessor makePostProcessor( final List<JdbcTest.Employee> initialData ) {
        return connection -> {
            PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
            SchemaPlus rootSchema = polyphenyDbEmbeddedConnection.getRootSchema();
            SchemaPlus mapSchema = rootSchema.add( "foo", new AbstractSchema() );
            final String tableName = "bar";
            final JdbcTest.AbstractModifiableTable table = mutable( tableName, initialData );
            mapSchema.add( tableName, table );
            return polyphenyDbEmbeddedConnection;
        };
    }


    /**
     * Method to be shared with {@code RemoteDriverTest}.
     *
     * @param initialData record to be presented in table
     */
    public static Connection makeConnection( final List<JdbcTest.Employee> initialData ) throws Exception {
        Properties info = new Properties();
        Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info );
        connection = makePostProcessor( initialData ).apply( connection );
        return connection;
    }


    /**
     * Creates a connection with an empty modifiable table with {@link JdbcTest.Employee} schema.
     */
    public static Connection makeConnection() throws Exception {
        return makeConnection( new ArrayList<JdbcTest.Employee>() );
    }


    private PolyphenyDbAssert.AssertThat mutable( final List<JdbcTest.Employee> employees ) {
        employees.add( new JdbcTest.Employee( 0, 0, "first", 0f, null ) );
        return PolyphenyDbAssert.that()
                .with( PolyphenyDbAssert.Config.REGULAR )
                .with( makePostProcessor( employees ) );
    }


    static JdbcTest.AbstractModifiableTable mutable( String tableName, final List<JdbcTest.Employee> employees ) {
        return new JdbcTest.AbstractModifiableTable( tableName ) {
            public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
                return ((JavaTypeFactory) typeFactory).createType( JdbcTest.Employee.class );
            }


            @Override
            public <T> Queryable<T> asQueryable( QueryProvider queryProvider, SchemaPlus schema, String tableName ) {
                return new AbstractTableQueryable<T>( queryProvider, schema, this, tableName ) {
                    public Enumerator<T> enumerator() {
                        //noinspection unchecked
                        return (Enumerator<T>) Linq4j.enumerator( employees );
                    }
                };
            }


            public Type getElementType() {
                return JdbcTest.Employee.class;
            }


            @Override
            public Expression getExpression( SchemaPlus schema, String tableName, Class clazz ) {
                return Schemas.tableExpression( schema, getElementType(), tableName, clazz );
            }


            public Collection getModifiableCollection() {
                return employees;
            }
        };
    }


    @Test
    public void testInsert2() {
        final List<JdbcTest.Employee> employees = new ArrayList<>();
        PolyphenyDbAssert.AssertThat with = mutable( employees );
        with.query( "insert into \"foo\".\"bar\" values (1, 1, 'second', 2, 2)" )
                .updates( 1 );
        with.query( "insert into \"foo\".\"bar\"\n" + "values (1, 3, 'third', 0, 3), (1, 4, 'fourth', 0, 4), (1, 5, 'fifth ', 0, 3)" )
                .updates( 3 );
        with.query( "select count(*) as c from \"foo\".\"bar\"" )
                .returns( "C=5\n" );
        with.query( "insert into \"foo\".\"bar\" values (1, 6, null, 0, null)" )
                .updates( 1 );
        with.query( "select count(*) as c from \"foo\".\"bar\"" )
                .returns( "C=6\n" );
    }


    /**
     * Local Statement insert
     */
    @Test
    public void testInsert3() throws Exception {
        Connection connection = makeConnection( new ArrayList<JdbcTest.Employee>() );
        String sql = "insert into \"foo\".\"bar\" values (1, 1, 'second', 2, 2)";

        Statement statement = connection.createStatement();
        boolean status = statement.execute( sql );
        assertFalse( status );
        ResultSet resultSet = statement.getResultSet();
        assertNull( resultSet );
        int updateCount = statement.getUpdateCount();
        assertEquals( 1, updateCount );
    }


    /**
     * Local PreparedStatement insert WITHOUT bind variables
     */
    @Test
    public void testPreparedStatementInsert() throws Exception {
        Connection connection = makeConnection( new ArrayList<JdbcTest.Employee>() );
        assertFalse( connection.isClosed() );

        String sql = "insert into \"foo\".\"bar\" values (1, 1, 'second', 2, 2)";
        PreparedStatement preparedStatement = connection.prepareStatement( sql );
        assertFalse( preparedStatement.isClosed() );

        boolean status = preparedStatement.execute();
        assertFalse( status );
        ResultSet resultSet = preparedStatement.getResultSet();
        assertNull( resultSet );
        int updateCount = preparedStatement.getUpdateCount();
        assertEquals( 1, updateCount );
    }


    /**
     * Local PreparedStatement insert WITH bind variables
     */
    @Test
    public void testPreparedStatementInsert2() throws Exception {
    }


    /**
     * Some of the rows have the wrong number of columns.
     */
    @Test
    public void testInsertMultipleRowMismatch() {
        final List<JdbcTest.Employee> employees = new ArrayList<>();
        PolyphenyDbAssert.AssertThat with = mutable( employees );
        with.query( "insert into \"foo\".\"bar\" values\n"
                + " (1, 3, 'third'),\n"
                + " (1, 4, 'fourth'),\n"
                + " (1, 5, 'fifth ', 3)" )
                .throws_( "Incompatible types" );
    }
}

