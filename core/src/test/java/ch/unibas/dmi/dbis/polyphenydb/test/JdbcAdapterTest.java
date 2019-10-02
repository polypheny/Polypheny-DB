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

package ch.unibas.dmi.dbis.polyphenydb.test;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import ch.unibas.dmi.dbis.polyphenydb.sql.Lex;
import ch.unibas.dmi.dbis.polyphenydb.test.PolyphenyDbAssert.AssertThat;
import ch.unibas.dmi.dbis.polyphenydb.test.PolyphenyDbAssert.DatabaseInstance;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.hsqldb.jdbcDriver;
import org.junit.Ignore;
import org.junit.Test;


/**
 * Tests for the {@code ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc} package.
 */
public class JdbcAdapterTest {

    /**
     * Ensures that tests that are modifying data (doing DML) do not run at the same time.
     */
    private static final ReentrantLock LOCK = new ReentrantLock();


    /**
     * VALUES is not pushed down, currently.
     */
    @Test
    @Ignore
    public void testValuesPlan() {
        final String sql = "select * from \"days\", (values 1, 2) as t(c)";
        final String explain = "PLAN="
                + "EnumerableCalc(expr#0..2=[{inputs}], day=[$t1], week_day=[$t2], C=[$t0])\n"
                + "  EnumerableJoin(condition=[true], joinType=[inner])\n"
                + "    EnumerableValues(tuples=[[{ 1 }, { 2 }]])\n"
                + "    JdbcToEnumerableConverter\n"
                + "      JdbcTableScan(table=[[foodmart, days]])";
        final String jdbcSql = "SELECT *\n" + "FROM \"foodmart\".\"days\"";
        PolyphenyDbAssert.model( JdbcTest.FOODMART_MODEL )
                .query( sql )
                .explainContains( explain )
                .runs()
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB || PolyphenyDbAssert.DB == DatabaseInstance.POSTGRESQL )
                .planHasSql( jdbcSql );
    }


    @Test
    @Ignore
    public void testUnionPlan() {
        PolyphenyDbAssert.model( JdbcTest.FOODMART_MODEL )
                .query( "select * from \"sales_fact_1997\"\n"
                        + "union all\n"
                        + "select * from \"sales_fact_1998\"" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcUnion(all=[true])\n"
                        + "    JdbcTableScan(table=[[foodmart, sales_fact_1997]])\n"
                        + "    JdbcTableScan(table=[[foodmart, sales_fact_1998]])" )
                .runs()
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .planHasSql( "SELECT *\n"
                        + "FROM \"foodmart\".\"sales_fact_1997\"\n"
                        + "UNION ALL\n"
                        + "SELECT *\n"
                        + "FROM \"foodmart\".\"sales_fact_1998\"" );
    }


    @Test
    @Ignore
    public void testFilterUnionPlan() {
        PolyphenyDbAssert.model( JdbcTest.FOODMART_MODEL )
                .query( "select * from (\n"
                        + "  select * from \"sales_fact_1997\"\n"
                        + "  union all\n"
                        + "  select * from \"sales_fact_1998\")\n"
                        + "where \"product_id\" = 1" )
                .runs()
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .planHasSql( "SELECT *\n"
                        + "FROM \"foodmart\".\"sales_fact_1997\"\n"
                        + "WHERE \"product_id\" = 1\n"
                        + "UNION ALL\n"
                        + "SELECT *\n"
                        + "FROM \"foodmart\".\"sales_fact_1998\"\n"
                        + "WHERE \"product_id\" = 1" );
    }


    @Test
    @Ignore
    public void testInPlan() {
        PolyphenyDbAssert.model( JdbcTest.FOODMART_MODEL )
                .query( "select \"store_id\", \"store_name\" from \"store\"\n" + "where \"store_name\" in ('Store 1', 'Store 10', 'Store 11', 'Store 15', 'Store 16', 'Store 24', 'Store 3', 'Store 7')" )
                .runs()
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .planHasSql( "SELECT \"store_id\", \"store_name\"\n"
                        + "FROM \"foodmart\".\"store\"\n"
                        + "WHERE \"store_name\" = 'Store 1' OR \"store_name\" = 'Store 10' OR \"store_name\" = 'Store 11' OR \"store_name\" = 'Store 15' OR \"store_name\" = 'Store 16' OR \"store_name\" = 'Store 24' OR \"store_name\" = 'Store 3' OR \"store_name\" = 'Store 7'" )
                .returns( "store_id=1; store_name=Store 1\n"
                        + "store_id=3; store_name=Store 3\n"
                        + "store_id=7; store_name=Store 7\n"
                        + "store_id=10; store_name=Store 10\n"
                        + "store_id=11; store_name=Store 11\n"
                        + "store_id=15; store_name=Store 15\n"
                        + "store_id=16; store_name=Store 16\n"
                        + "store_id=24; store_name=Store 24\n" );
    }


    @Test
    @Ignore
    public void testEquiJoinPlan() {
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( "select empno, ename, e.deptno, dname \n"
                        + "from scott.emp e inner join scott.dept d \n"
                        + "on e.deptno = d.deptno" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcProject(EMPNO=[$2], ENAME=[$3], DEPTNO=[$4], DNAME=[$1])\n"
                        + "    JdbcJoin(condition=[=($4, $0)], joinType=[inner])\n"
                        + "      JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
                        + "        JdbcTableScan(table=[[SCOTT, DEPT]])\n"
                        + "      JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7])\n"
                        + "        JdbcTableScan(table=[[SCOTT, EMP]])" )
                .runs()
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .planHasSql( "SELECT \"t0\".\"EMPNO\", \"t0\".\"ENAME\", "
                        + "\"t0\".\"DEPTNO\", \"t\".\"DNAME\"\n"
                        + "FROM (SELECT \"DEPTNO\", \"DNAME\"\n"
                        + "FROM \"SCOTT\".\"DEPT\") AS \"t\"\n"
                        + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\", \"DEPTNO\"\n"
                        + "FROM \"SCOTT\".\"EMP\") AS \"t0\" "
                        + "ON \"t\".\"DEPTNO\" = \"t0\".\"DEPTNO\"" );
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-631">[POLYPHENYDB-631] Push theta joins down to JDBC adapter</a>.
     */
    @Test
    @Ignore
    public void testNonEquiJoinPlan() {
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( "select empno, ename, grade \n"
                        + "from scott.emp e inner join scott.salgrade s \n"
                        + "on e.sal > s.losal and e.sal < s.hisal" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcProject(EMPNO=[$3], ENAME=[$4], GRADE=[$0])\n"
                        + "    JdbcJoin(condition=[AND(>($5, $1), <($5, $2))], joinType=[inner])\n"
                        + "      JdbcTableScan(table=[[SCOTT, SALGRADE]])\n"
                        + "      JdbcProject(EMPNO=[$0], ENAME=[$1], SAL=[$5])\n"
                        + "        JdbcTableScan(table=[[SCOTT, EMP]])" )
                .runs()
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .planHasSql( "SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", "
                        + "\"SALGRADE\".\"GRADE\"\nFROM \"SCOTT\".\"SALGRADE\"\n"
                        + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\", \"SAL\"\n"
                        + "FROM \"SCOTT\".\"EMP\") AS \"t\" "
                        + "ON \"SALGRADE\".\"LOSAL\" < \"t\".\"SAL\" "
                        + "AND \"SALGRADE\".\"HISAL\" > \"t\".\"SAL\"" );
    }


    @Test
    @Ignore
    public void testNonEquiJoinReverseConditionPlan() {
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( "select empno, ename, grade \n"
                        + "from scott.emp e inner join scott.salgrade s \n"
                        + "on s.losal <= e.sal and s.hisal >= e.sal" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcProject(EMPNO=[$3], ENAME=[$4], GRADE=[$0])\n"
                        + "    JdbcJoin(condition=[AND(<=($1, $5), >=($2, $5))], joinType=[inner])\n"
                        + "      JdbcTableScan(table=[[SCOTT, SALGRADE]])\n"
                        + "      JdbcProject(EMPNO=[$0], ENAME=[$1], SAL=[$5])\n"
                        + "        JdbcTableScan(table=[[SCOTT, EMP]])" )
                .runs()
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .planHasSql( "SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", "
                        + "\"SALGRADE\".\"GRADE\"\nFROM \"SCOTT\".\"SALGRADE\"\n"
                        + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\", \"SAL\"\n"
                        + "FROM \"SCOTT\".\"EMP\") AS \"t\" "
                        + "ON \"SALGRADE\".\"LOSAL\" <= \"t\".\"SAL\" AND \"SALGRADE\".\"HISAL\" >= \"t\".\"SAL\"" );
    }


    @Test
    @Ignore
    public void testMixedJoinPlan() {
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( "select e.empno, e.ename, e.empno, e.ename  \n"
                        + "from scott.emp e inner join scott.emp m on  \n"
                        + "e.mgr = m.empno and e.sal > m.sal" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcProject(EMPNO=[$2], ENAME=[$3], EMPNO0=[$2], ENAME0=[$3])\n"
                        + "    JdbcJoin(condition=[AND(=($4, $0), >($5, $1))], joinType=[inner])\n"
                        + "      JdbcProject(EMPNO=[$0], SAL=[$5])\n"
                        + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
                        + "      JdbcProject(EMPNO=[$0], ENAME=[$1], MGR=[$3], SAL=[$5])\n"
                        + "        JdbcTableScan(table=[[SCOTT, EMP]])" )
                .runs()
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .planHasSql( "SELECT \"t0\".\"EMPNO\", \"t0\".\"ENAME\", "
                        + "\"t0\".\"EMPNO\" AS \"EMPNO0\", \"t0\".\"ENAME\" AS \"ENAME0\"\n"
                        + "FROM (SELECT \"EMPNO\", \"SAL\"\n"
                        + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
                        + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\", \"MGR\", \"SAL\"\n"
                        + "FROM \"SCOTT\".\"EMP\") AS \"t0\" "
                        + "ON \"t\".\"EMPNO\" = \"t0\".\"MGR\" AND \"t\".\"SAL\" < \"t0\".\"SAL\"" );
    }


    @Test
    @Ignore
    public void testMixedJoinWithOrPlan() {
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( "select e.empno, e.ename, e.empno, e.ename  \n"
                        + "from scott.emp e inner join scott.emp m on  \n"
                        + "e.mgr = m.empno and (e.sal > m.sal or m.hiredate > e.hiredate)" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcProject(EMPNO=[$3], ENAME=[$4], EMPNO0=[$3], ENAME0=[$4])\n"
                        + "    JdbcJoin(condition=[AND(=($5, $0), OR(>($7, $2), >($1, $6)))], joinType=[inner])\n"
                        + "      JdbcProject(EMPNO=[$0], HIREDATE=[$4], SAL=[$5])\n"
                        + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
                        + "      JdbcProject(EMPNO=[$0], ENAME=[$1], MGR=[$3], HIREDATE=[$4], SAL=[$5])\n"
                        + "        JdbcTableScan(table=[[SCOTT, EMP]])" )
                .runs()
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .planHasSql( "SELECT \"t0\".\"EMPNO\", \"t0\".\"ENAME\", "
                        + "\"t0\".\"EMPNO\" AS \"EMPNO0\", \"t0\".\"ENAME\" AS \"ENAME0\"\n"
                        + "FROM (SELECT \"EMPNO\", \"HIREDATE\", \"SAL\"\n"
                        + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
                        + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\", \"MGR\", \"HIREDATE\", \"SAL\"\n"
                        + "FROM \"SCOTT\".\"EMP\") AS \"t0\" "
                        + "ON \"t\".\"EMPNO\" = \"t0\".\"MGR\" "
                        + "AND (\"t\".\"SAL\" < \"t0\".\"SAL\" OR \"t\".\"HIREDATE\" > \"t0\".\"HIREDATE\")" );
    }


    @Test
    @Ignore
    public void testJoin3TablesPlan() {
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( "select  empno, ename, dname, grade \n"
                        + "from scott.emp e inner join scott.dept d \n"
                        + "on e.deptno = d.deptno \n"
                        + "inner join scott.salgrade s \n"
                        + "on e.sal > s.losal and e.sal < s.hisal" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcProject(EMPNO=[$3], ENAME=[$4], DNAME=[$8], GRADE=[$0])\n"
                        + "    JdbcJoin(condition=[AND(>($5, $1), <($5, $2))], joinType=[inner])\n"
                        + "      JdbcTableScan(table=[[SCOTT, SALGRADE]])\n"
                        + "      JdbcJoin(condition=[=($3, $4)], joinType=[inner])\n"
                        + "        JdbcProject(EMPNO=[$0], ENAME=[$1], SAL=[$5], DEPTNO=[$7])\n"
                        + "          JdbcTableScan(table=[[SCOTT, EMP]])\n"
                        + "        JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
                        + "          JdbcTableScan(table=[[SCOTT, DEPT]])" )
                .runs()
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .planHasSql( "SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", "
                        + "\"t0\".\"DNAME\", \"SALGRADE\".\"GRADE\"\n"
                        + "FROM \"SCOTT\".\"SALGRADE\"\n"
                        + "INNER JOIN ((SELECT \"EMPNO\", \"ENAME\", \"SAL\", \"DEPTNO\"\n"
                        + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
                        + "INNER JOIN (SELECT \"DEPTNO\", \"DNAME\"\n"
                        + "FROM \"SCOTT\".\"DEPT\") AS \"t0\" ON \"t\".\"DEPTNO\" = \"t0\".\"DEPTNO\")"
                        + " ON \"SALGRADE\".\"LOSAL\" < \"t\".\"SAL\" AND \"SALGRADE\".\"HISAL\" > \"t\".\"SAL\"" );
    }


    @Test
    @Ignore
    public void testCrossJoinWithJoinKeyPlan() {
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( "select empno, ename, d.deptno, dname \n"
                        + "from scott.emp e,scott.dept d \n"
                        + "where e.deptno = d.deptno" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$3], DNAME=[$4])\n"
                        + "    JdbcJoin(condition=[=($2, $3)], joinType=[inner])\n"
                        + "      JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7])\n"
                        + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
                        + "      JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
                        + "        JdbcTableScan(table=[[SCOTT, DEPT]])" )
                .runs()
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .planHasSql( "SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", "
                        + "\"t0\".\"DEPTNO\", \"t0\".\"DNAME\"\n"
                        + "FROM (SELECT \"EMPNO\", \"ENAME\", \"DEPTNO\"\nFROM \"SCOTT\".\"EMP\") AS \"t\"\n"
                        + "INNER JOIN (SELECT \"DEPTNO\", \"DNAME\"\n"
                        + "FROM \"SCOTT\".\"DEPT\") AS \"t0\" ON \"t\".\"DEPTNO\" = \"t0\".\"DEPTNO\"" );
    }


    // JdbcJoin not used for this
    @Test
    @Ignore
    public void testCartesianJoinWithoutKeyPlan() {
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( "select empno, ename, d.deptno, dname \n"
                        + "from scott.emp e,scott.dept d" )
                .explainContains( "PLAN=EnumerableJoin(condition=[true], "
                        + "joinType=[inner])\n"
                        + "  JdbcToEnumerableConverter\n"
                        + "    JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
                        + "      JdbcTableScan(table=[[SCOTT, EMP]])\n"
                        + "  JdbcToEnumerableConverter\n"
                        + "    JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
                        + "      JdbcTableScan(table=[[SCOTT, DEPT]])" )
                .runs()
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB );
    }


    @Test
    @Ignore
    public void testCrossJoinWithJoinKeyAndFilterPlan() {
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( "select empno, ename, d.deptno, dname \n"
                        + "from scott.emp e,scott.dept d \n"
                        + "where e.deptno = d.deptno \n"
                        + "and e.deptno=20" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$3], DNAME=[$4])\n"
                        + "    JdbcJoin(condition=[=($2, $3)], joinType=[inner])\n"
                        + "      JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7])\n"
                        + "        JdbcFilter(condition=[=(CAST($7):INTEGER, 20)])\n"
                        + "          JdbcTableScan(table=[[SCOTT, EMP]])\n"
                        + "      JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
                        + "        JdbcTableScan(table=[[SCOTT, DEPT]])" )
                .runs()
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .planHasSql( "SELECT \"t0\".\"EMPNO\", \"t0\".\"ENAME\", "
                        + "\"t1\".\"DEPTNO\", \"t1\".\"DNAME\"\n"
                        + "FROM (SELECT \"EMPNO\", \"ENAME\", \"DEPTNO\"\n"
                        + "FROM \"SCOTT\".\"EMP\"\n"
                        + "WHERE CAST(\"DEPTNO\" AS INTEGER) = 20) AS \"t0\"\n"
                        + "INNER JOIN (SELECT \"DEPTNO\", \"DNAME\"\n"
                        + "FROM \"SCOTT\".\"DEPT\") AS \"t1\" ON \"t0\".\"DEPTNO\" = \"t1\".\"DEPTNO\"" );
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-893">[POLYPHENYDB-893] Theta join in JdbcAdapter</a>.
     */
    @Test
    @Ignore
    public void testJoinPlan() {
        final String sql = "SELECT T1.\"brand_name\"\n"
                + "FROM \"foodmart\".\"product\" AS T1\n"
                + " INNER JOIN \"foodmart\".\"product_class\" AS T2\n"
                + " ON T1.\"product_class_id\" = T2.\"product_class_id\"\n"
                + "WHERE T2.\"product_department\" = 'Frozen Foods'\n"
                + " OR T2.\"product_department\" = 'Baking Goods'\n"
                + " AND T1.\"brand_name\" <> 'King'";
        PolyphenyDbAssert.model( JdbcTest.FOODMART_MODEL )
                .query( sql ).runs()
                .returnsCount( 275 );
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-1372">[POLYPHENYDB-1372] JDBC adapter generates SQL with wrong field names</a>.
     */
    @Test
    @Ignore
    public void testJoinPlan2() {
        final String sql = "SELECT v1.deptno, v2.deptno\n"
                + "FROM Scott.dept v1 LEFT JOIN Scott.emp v2 ON v1.deptno = v2.deptno\n"
                + "WHERE v2.job LIKE 'PRESIDENT'";
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .with( Lex.MYSQL )
                .query( sql ).runs()
                .returnsCount( 1 );
    }


    @Test
    @Ignore
    public void testJoinCartesian() {
        final String sql = "SELECT *\n" + "FROM Scott.dept, Scott.emp";
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL ).query( sql ).returnsCount( 56 );
    }


    @Test
    @Ignore
    public void testJoinCartesianCount() {
        final String sql = "SELECT count(*) as c\n" + "FROM Scott.dept, Scott.emp";
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL ).query( sql ).returns( "C=56\n" );
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-657">[POLYPHENYDB-657] NullPointerException when executing JdbcAggregate implement method</a>.
     */
    @Test
    @Ignore
    public void testJdbcAggregate() throws Exception {
        final String url = MultiJdbcSchemaJoinTest.TempDb.INSTANCE.getUrl();
        Connection baseConnection = DriverManager.getConnection( url );
        Statement baseStmt = baseConnection.createStatement();
        baseStmt.execute( "CREATE TABLE T2 (\n" + "ID INTEGER,\n" + "VALS INTEGER)" );
        baseStmt.execute( "INSERT INTO T2 VALUES (1, 1)" );
        baseStmt.execute( "INSERT INTO T2 VALUES (2, null)" );
        baseStmt.close();
        baseConnection.commit();

        Properties info = new Properties();
        info.put( "model",
                "inline:"
                        + "{\n"
                        + "  version: '1.0',\n"
                        + "  defaultSchema: 'BASEJDBC',\n"
                        + "  schemas: [\n"
                        + "     {\n"
                        + "       type: 'jdbc',\n"
                        + "       name: 'BASEJDBC',\n"
                        + "       jdbcDriver: '" + jdbcDriver.class.getName() + "',\n"
                        + "       jdbcUrl: '" + url + "',\n"
                        + "       jdbcCatalog: null,\n"
                        + "       jdbcSchema: null\n"
                        + "     }\n"
                        + "  ]\n"
                        + "}" );

        final Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info );
        ResultSet rs = connection.prepareStatement( "select 10 * count(ID) from t2" ).executeQuery();

        assertThat( rs.next(), is( true ) );
        assertThat( (Long) rs.getObject( 1 ), equalTo( 20L ) );
        assertThat( rs.next(), is( false ) );

        rs.close();
        connection.close();
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-2206">[POLYPHENYDB-2206] JDBC adapter incorrectly pushes windowed aggregates down to HSQLDB</a>.
     */
    @Test
    @Ignore
    public void testOverNonSupportedDialect() {
        final String sql = "select \"store_id\", \"account_id\", \"exp_date\",\n"
                + " \"time_id\", \"category_id\", \"currency_id\", \"amount\",\n"
                + " last_value(\"time_id\") over () as \"last_version\"\n"
                + "from \"expense_fact\"";
        final String explain = "PLAN="
                + "EnumerableWindow(window#0=[window(partition {} "
                + "order by [] range between UNBOUNDED PRECEDING and "
                + "UNBOUNDED FOLLOWING aggs [LAST_VALUE($3)])])\n"
                + "  JdbcToEnumerableConverter\n"
                + "    JdbcTableScan(table=[[foodmart, expense_fact]])\n";
        PolyphenyDbAssert
                .model( JdbcTest.FOODMART_MODEL )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .query( sql )
                .explainContains( explain )
                .runs()
                .planHasSql( "SELECT *\n" + "FROM \"foodmart\".\"expense_fact\"" );
    }


    @Test
    public void testTablesNoCatalogSchema() {
        final String model =
                JdbcTest.FOODMART_MODEL
                        .replace( "jdbcCatalog: 'foodmart'", "jdbcCatalog: null" )
                        .replace( "jdbcSchema: 'foodmart'", "jdbcSchema: null" );
        // Since Polypheny-DB uses PostgreSQL JDBC driver version >= 4.1, catalog/schema can be retrieved from JDBC connection and this test succeeds
        PolyphenyDbAssert.model( model )
                // Polypheny-DB uses PostgreSQL JDBC driver version >= 4.1
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.POSTGRESQL )
                .query( "select \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
                        + " last_value(\"time_id\") over ()"
                        + " as \"last_version\" from \"expense_fact\"" )
                .runs();
        // Since Polypheny-DB uses HSQLDB JDBC driver version < 4.1, catalog/schema cannot be retrieved from JDBC connection and this test fails
        PolyphenyDbAssert.model( model )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .query( "select \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
                        + " last_value(\"time_id\") over ()"
                        + " as \"last_version\" from \"expense_fact\"" )
                .throws_( "'expense_fact' not found" );
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-1506">[POLYPHENYDB-1506] Push OVER Clause to underlying SQL via JDBC adapter</a>.
     *
     * Test runs only on Postgres; the default database, Hsqldb, does not support OVER.
     */
    @Test
    public void testOverDefault() {
        PolyphenyDbAssert
                .model( JdbcTest.FOODMART_MODEL )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.POSTGRESQL )
                .query( "select \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
                        + " last_value(\"time_id\") over ()"
                        + " as \"last_version\" from \"expense_fact\"" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2], "
                        + "time_id=[$3], category_id=[$4], currency_id=[$5], amount=[$6],"
                        + " last_version=[LAST_VALUE($3) OVER (RANGE BETWEEN UNBOUNDED"
                        + " PRECEDING AND UNBOUNDED FOLLOWING)])\n"
                        + "    JdbcTableScan(table=[[foodmart, expense_fact]])\n" )
                .runs()
                .planHasSql( "SELECT \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
                        + " LAST_VALUE(\"time_id\") OVER (RANGE BETWEEN UNBOUNDED"
                        + " PRECEDING AND UNBOUNDED FOLLOWING) AS \"last_version\"\n"
                        + "FROM \"foodmart\".\"expense_fact\"" );
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-2305">[POLYPHENYDB-2305] JDBC adapter generates invalid casts on PostgreSQL, because PostgreSQL does not have TINYINT and DOUBLE types</a>.
     */
    @Test
    public void testCast() {
        PolyphenyDbAssert
                .model( JdbcTest.FOODMART_MODEL )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.POSTGRESQL )
                .query( "select cast(\"store_id\" as TINYINT),"
                        + "cast(\"store_id\" as DOUBLE)"
                        + " from \"expense_fact\"" )
                .runs()
                .planHasSql( "SELECT CAST(\"store_id\" AS SMALLINT),"
                        + " CAST(\"store_id\" AS DOUBLE PRECISION)\n"
                        + "FROM \"foodmart\".\"expense_fact\"" );
    }


    @Test
    public void testOverRowsBetweenBoundFollowingAndFollowing() {
        PolyphenyDbAssert
                .model( JdbcTest.FOODMART_MODEL )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.POSTGRESQL )
                .query( "select \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
                        + " last_value(\"time_id\") over (partition by \"account_id\""
                        + " order by \"time_id\" rows between 1 following and 10 following)"
                        + " as \"last_version\" from \"expense_fact\"" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2], "
                        + "time_id=[$3], category_id=[$4], currency_id=[$5], amount=[$6],"
                        + " last_version=[LAST_VALUE($3) OVER (PARTITION BY $1"
                        + " ORDER BY $3 ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING)])\n"
                        + "    JdbcTableScan(table=[[foodmart, expense_fact]])\n" )
                .runs()
                .planHasSql( "SELECT \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
                        + " LAST_VALUE(\"time_id\") OVER (PARTITION BY \"account_id\""
                        + " ORDER BY \"time_id\" ROWS BETWEEN 1 FOLLOWING"
                        + " AND 10 FOLLOWING) AS \"last_version\"\n"
                        + "FROM \"foodmart\".\"expense_fact\"" );
    }


    @Test
    public void testOverRowsBetweenBoundPrecedingAndCurrent() {
        PolyphenyDbAssert
                .model( JdbcTest.FOODMART_MODEL )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.POSTGRESQL )
                .query( "select \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
                        + " last_value(\"time_id\") over (partition by \"account_id\""
                        + " order by \"time_id\" rows between 3 preceding and current row)"
                        + " as \"last_version\" from \"expense_fact\"" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2], "
                        + "time_id=[$3], category_id=[$4], currency_id=[$5], amount=[$6],"
                        + " last_version=[LAST_VALUE($3) OVER (PARTITION BY $1"
                        + " ORDER BY $3 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)])\n"
                        + "    JdbcTableScan(table=[[foodmart, expense_fact]])\n" )
                .runs()
                .planHasSql( "SELECT \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
                        + " LAST_VALUE(\"time_id\") OVER (PARTITION BY \"account_id\""
                        + " ORDER BY \"time_id\" ROWS BETWEEN 3 PRECEDING"
                        + " AND CURRENT ROW) AS \"last_version\"\n"
                        + "FROM \"foodmart\".\"expense_fact\"" );
    }


    @Test
    public void testOverDisallowPartial() {
        PolyphenyDbAssert
                .model( JdbcTest.FOODMART_MODEL )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.POSTGRESQL )
                .query( "select \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
                        + " last_value(\"time_id\") over (partition by \"account_id\""
                        + " order by \"time_id\" rows 3 preceding disallow partial)"
                        + " as \"last_version\" from \"expense_fact\"" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2],"
                        + " time_id=[$3], category_id=[$4], currency_id=[$5],"
                        + " amount=[$6], last_version=[CASE(>=(COUNT() OVER"
                        + " (PARTITION BY $1 ORDER BY $3 ROWS BETWEEN 3 PRECEDING AND"
                        + " CURRENT ROW), 2), LAST_VALUE($3) OVER (PARTITION BY $1"
                        + " ORDER BY $3 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW),"
                        + " null)])\n    JdbcTableScan(table=[[foodmart,"
                        + " expense_fact]])\n" )
                .runs()
                .planHasSql( "SELECT \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
                        + " CASE WHEN (COUNT(*) OVER (PARTITION BY \"account_id\""
                        + " ORDER BY \"time_id\" ROWS BETWEEN 3 PRECEDING"
                        + " AND CURRENT ROW)) >= 2 THEN LAST_VALUE(\"time_id\")"
                        + " OVER (PARTITION BY \"account_id\" ORDER BY \"time_id\""
                        + " ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)"
                        + " ELSE NULL END AS \"last_version\"\n"
                        + "FROM \"foodmart\".\"expense_fact\"" );
    }


    @Test
    public void testLastValueOver() {
        PolyphenyDbAssert
                .model( JdbcTest.FOODMART_MODEL )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.POSTGRESQL )
                .query( "select \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
                        + " last_value(\"time_id\") over (partition by \"account_id\""
                        + " order by \"time_id\") as \"last_version\""
                        + " from \"expense_fact\"" )
                .explainContains( "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2],"
                        + " time_id=[$3], category_id=[$4], currency_id=[$5], amount=[$6],"
                        + " last_version=[LAST_VALUE($3) OVER (PARTITION BY $1 ORDER BY $3"
                        + " RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)])\n"
                        + "    JdbcTableScan(table=[[foodmart, expense_fact]])\n" )
                .runs()
                .planHasSql( "SELECT \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
                        + " LAST_VALUE(\"time_id\") OVER (PARTITION BY \"account_id\""
                        + " ORDER BY \"time_id\" RANGE BETWEEN UNBOUNDED PRECEDING AND"
                        + " CURRENT ROW) AS \"last_version\""
                        + "\nFROM \"foodmart\".\"expense_fact\"" );
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-259">[POLYPHENYDB-259] Using sub-queries in CASE statement against JDBC tables generates invalid Oracle SQL</a>.
     */
    @Test
    @Ignore
    public void testSubQueryWithSingleValue() {
        final String expected;
        switch ( PolyphenyDbAssert.DB ) {
            case MYSQL:
                expected = "Sub query returns more than 1 row";
                break;
            default:
                expected = "more than one value in agg SINGLE_VALUE";
        }
        PolyphenyDbAssert.model( JdbcTest.FOODMART_MODEL )
                .query( "SELECT \"full_name\" FROM \"employee\" WHERE \"employee_id\" = (SELECT \"employee_id\" FROM \"salary\")" )
                .explainContains( "SINGLE_VALUE" )
                .throws_( expected );
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-666">[POLYPHENYDB-666] Anti-semi-joins against JDBC adapter give wrong results</a>.
     */
    @Test
    @Ignore
    public void testScalarSubQuery() {
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( "SELECT COUNT(empno) AS cEmpNo FROM \"SCOTT\".\"EMP\" WHERE DEPTNO <> (SELECT * FROM (VALUES 1))" )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .returns( "CEMPNO=14\n" );

        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( "SELECT ename FROM \"SCOTT\".\"EMP\" WHERE DEPTNO = (SELECT deptno FROM \"SCOTT\".\"DEPT\" WHERE dname = 'ACCOUNTING')" )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .returns( "ENAME=CLARK\nENAME=KING\nENAME=MILLER\n" );

        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( "SELECT COUNT(ename) AS cEname FROM \"SCOTT\".\"EMP\" WHERE DEPTNO > (SELECT deptno FROM \"SCOTT\".\"DEPT\" WHERE dname = 'ACCOUNTING')" )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .returns( "CENAME=11\n" );

        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( "SELECT COUNT(ename) AS cEname FROM \"SCOTT\".\"EMP\" WHERE DEPTNO < (SELECT deptno FROM \"SCOTT\".\"DEPT\" WHERE dname = 'ACCOUNTING')" )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB )
                .returns( "CENAME=0\n" );
    }


    /**
     * Acquires an exclusive connection to a test database, and cleans it.
     *
     * Cleans any previous TableModify states and creates one expense_fact instance with store_id = 666.
     *
     * Caller must close the returned wrapper, so that the next test can acquire the lock and use the database.
     *
     * @param c JDBC connection
     */
    private LockWrapper exclusiveCleanDb( Connection c ) throws SQLException {
        final LockWrapper wrapper = LockWrapper.lock( LOCK );
        try ( Statement statement = c.createStatement() ) {
            final String dSql = "DELETE FROM \"foodmart\".\"expense_fact\" WHERE \"store_id\"=666\n";
            final String iSql = "INSERT INTO \"foodmart\".\"expense_fact\"(\n"
                    + " \"store_id\", \"account_id\", \"exp_date\", \"time_id\","
                    + " \"category_id\", \"currency_id\", \"amount\")\n"
                    + " VALUES (666, 666, TIMESTAMP '1997-01-01 00:00:00',"
                    + " 666, '666', 666, 666)";
            statement.executeUpdate( dSql );
            int rowCount = statement.executeUpdate( iSql );
            assertThat( rowCount, is( 1 ) );
            return wrapper;
        } catch ( SQLException | RuntimeException | Error e ) {
            wrapper.close();
            throw e;
        }
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-1527">[POLYPHENYDB-1527] Support DML in the JDBC adapter</a>.
     */
    @Test
    @Ignore
    public void testTableModifyInsert() throws Exception {
        final String sql = "INSERT INTO \"foodmart\".\"expense_fact\"(\n"
                + " \"store_id\", \"account_id\", \"exp_date\", \"time_id\","
                + " \"category_id\", \"currency_id\", \"amount\")\n"
                + "VALUES (666, 666, TIMESTAMP '1997-01-01 00:00:00',"
                + " 666, '666', 666, 666)";
        final String explain = "PLAN=JdbcToEnumerableConverter\n"
                + "  JdbcTableModify(table=[[foodmart, expense_fact]], operation=[INSERT], flattened=[false])\n"
                + "    JdbcValues(tuples=[[{ 666, 666, 1997-01-01 00:00:00, 666, '666', 666, 666.0000 }]])\n";
        final String jdbcSql = "INSERT INTO \"foodmart\".\"expense_fact\""
                + " (\"store_id\", \"account_id\", \"exp_date\", \"time_id\","
                + " \"category_id\", \"currency_id\", \"amount\")\n"
                + "VALUES  (666, 666, TIMESTAMP '1997-01-01 00:00:00', 666, '666', 666, 666.0000)";
        final AssertThat that = PolyphenyDbAssert.model( JdbcTest.FOODMART_MODEL ).enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB || PolyphenyDbAssert.DB == DatabaseInstance.POSTGRESQL );
        that.doWithConnection( connection -> {
            try ( LockWrapper ignore = exclusiveCleanDb( connection ) ) {
                that.query( sql )
                        .explainContains( explain )
                        .planUpdateHasSql( jdbcSql, 1 );
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        } );
    }


    @Test
    @Ignore
    public void testTableModifyInsertMultiValues() throws Exception {
        final String sql = "INSERT INTO \"foodmart\".\"expense_fact\"(\n"
                + " \"store_id\", \"account_id\", \"exp_date\", \"time_id\","
                + " \"category_id\", \"currency_id\", \"amount\")\n"
                + "VALUES (666, 666, TIMESTAMP '1997-01-01 00:00:00',"
                + "   666, '666', 666, 666),\n"
                + " (666, 777, TIMESTAMP '1997-01-01 00:00:00',"
                + "   666, '666', 666, 666)";
        final String explain = "PLAN=JdbcToEnumerableConverter\n"
                + "  JdbcTableModify(table=[[foodmart, expense_fact]], operation=[INSERT], flattened=[false])\n"
                + "    JdbcValues(tuples=[[{ 666, 666, 1997-01-01 00:00:00, 666, '666', 666, 666.0000 },"
                + " { 666, 777, 1997-01-01 00:00:00, 666, '666', 666, 666.0000 }]])\n";
        final String jdbcSql = "INSERT INTO \"foodmart\".\"expense_fact\""
                + " (\"store_id\", \"account_id\", \"exp_date\", \"time_id\","
                + " \"category_id\", \"currency_id\", \"amount\")\n"
                + "VALUES  (666, 666, TIMESTAMP '1997-01-01 00:00:00', 666, '666', 666, 666.0000),\n"
                + " (666, 777, TIMESTAMP '1997-01-01 00:00:00', 666, '666', 666, 666.0000)";
        final AssertThat that =
                PolyphenyDbAssert.model( JdbcTest.FOODMART_MODEL ).enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB || PolyphenyDbAssert.DB == DatabaseInstance.POSTGRESQL );
        that.doWithConnection( connection -> {
            try ( LockWrapper ignore = exclusiveCleanDb( connection ) ) {
                that.query( sql )
                        .explainContains( explain )
                        .planUpdateHasSql( jdbcSql, 2 );
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        } );
    }


    @Test
    @Ignore
    public void testTableModifyInsertWithSubQuery() throws Exception {
        final AssertThat that = PolyphenyDbAssert
                .model( JdbcTest.FOODMART_MODEL )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB );

        that.doWithConnection( connection -> {
            try ( LockWrapper ignore = exclusiveCleanDb( connection ) ) {
                final String sql = "INSERT INTO \"foodmart\".\"expense_fact\"(\n"
                        + " \"store_id\", \"account_id\", \"exp_date\", \"time_id\","
                        + " \"category_id\", \"currency_id\", \"amount\")\n"
                        + "SELECT  \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\" + 1, \"category_id\", \"currency_id\","
                        + " \"amount\"\n"
                        + "FROM \"foodmart\".\"expense_fact\"\n"
                        + "WHERE \"store_id\" = 666";
                final String explain = "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcTableModify(table=[[foodmart, expense_fact]], operation=[INSERT], flattened=[false])\n"
                        + "    JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2], time_id=[+($3, 1)], category_id=[$4], currency_id=[$5], amount=[$6])\n"
                        + "      JdbcFilter(condition=[=($0, 666)])\n"
                        + "        JdbcTableScan(table=[[foodmart, expense_fact]])\n";
                final String jdbcSql = "INSERT INTO \"foodmart\".\"expense_fact\""
                        + " (\"store_id\", \"account_id\", \"exp_date\", \"time_id\","
                        + " \"category_id\", \"currency_id\", \"amount\")\n"
                        + "(SELECT \"store_id\", \"account_id\", \"exp_date\","
                        + " \"time_id\" + 1 AS \"time_id\", \"category_id\","
                        + " \"currency_id\", \"amount\"\n"
                        + "FROM \"foodmart\".\"expense_fact\"\n"
                        + "WHERE \"store_id\" = 666)";
                that.query( sql )
                        .explainContains( explain )
                        .planUpdateHasSql( jdbcSql, 1 );
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        } );
    }


    @Test
    @Ignore
    public void testTableModifyUpdate() throws Exception {
        final AssertThat that = PolyphenyDbAssert
                .model( JdbcTest.FOODMART_MODEL )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB );

        that.doWithConnection( connection -> {
            try ( LockWrapper ignore = exclusiveCleanDb( connection ) ) {
                final String sql = "UPDATE \"foodmart\".\"expense_fact\"\n"
                        + " SET \"account_id\"=888\n"
                        + " WHERE \"store_id\"=666\n";
                final String explain = "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcTableModify(table=[[foodmart, expense_fact]], operation=[UPDATE], updateColumnList=[[account_id]], sourceExpressionList=[[888]], flattened=[false])\n"
                        + "    JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2], time_id=[$3], category_id=[$4], currency_id=[$5], amount=[$6], EXPR$0=[888])\n"
                        + "      JdbcFilter(condition=[=($0, 666)])\n"
                        + "        JdbcTableScan(table=[[foodmart, expense_fact]])";
                final String jdbcSql = "UPDATE \"foodmart\".\"expense_fact\""
                        + " SET \"account_id\" = 888\n"
                        + "WHERE \"store_id\" = 666";
                that.query( sql )
                        .explainContains( explain )
                        .planUpdateHasSql( jdbcSql, 1 );
                return null;
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        } );
    }


    @Test
    @Ignore
    public void testTableModifyDelete() throws Exception {
        final AssertThat that = PolyphenyDbAssert
                .model( JdbcTest.FOODMART_MODEL )
                .enable( PolyphenyDbAssert.DB == DatabaseInstance.HSQLDB );

        that.doWithConnection( connection -> {
            try ( LockWrapper ignore = exclusiveCleanDb( connection ) ) {
                final String sql = "DELETE FROM \"foodmart\".\"expense_fact\"\n"
                        + "WHERE \"store_id\"=666\n";
                final String explain = "PLAN=JdbcToEnumerableConverter\n"
                        + "  JdbcTableModify(table=[[foodmart, expense_fact]], operation=[DELETE], flattened=[false])\n"
                        + "    JdbcFilter(condition=[=($0, 666)])\n"
                        + "      JdbcTableScan(table=[[foodmart, expense_fact]]";
                final String jdbcSql = "DELETE FROM \"foodmart\".\"expense_fact\"\n"
                        + "WHERE \"store_id\" = 666";
                that.query( sql )
                        .explainContains( explain )
                        .planUpdateHasSql( jdbcSql, 1 );
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        } );
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-1572">[POLYPHENYDB-1572] JdbcSchema throws exception when detecting nullable columns</a>.
     */
    @Test
    @Ignore
    public void testColumnNullability() throws Exception {
        final String sql = "select \"employee_id\", \"position_id\"\n"
                + "from \"foodmart\".\"employee\" limit 10";
        PolyphenyDbAssert.model( JdbcTest.FOODMART_MODEL )
                .query( sql )
                .runs()
                .returnsCount( 10 )
                .typeIs( "[employee_id INTEGER NOT NULL, position_id INTEGER]" );
    }


    @Test
    @Ignore
    public void pushBindParameters() throws Exception {
        final String sql = "select empno, ename from emp where empno = ?";
        PolyphenyDbAssert.model( JdbcTest.SCOTT_MODEL )
                .query( sql )
                .consumesPreparedStatement( p -> p.setInt( 1, 7566 ) )
                .returnsCount( 1 )
                .planHasSql( "SELECT \"EMPNO\", \"ENAME\"\nFROM \"SCOTT\".\"EMP\"\nWHERE \"EMPNO\" = ?" );
    }


    /**
     * Acquires a lock, and releases it when closed.
     */
    static class LockWrapper implements AutoCloseable {

        private final Lock lock;


        LockWrapper( Lock lock ) {
            this.lock = lock;
        }


        /**
         * Acquires a lock and returns a closeable wrapper.
         */
        static LockWrapper lock( Lock lock ) {
            lock.lock();
            return new LockWrapper( lock );
        }


        @Override
        public void close() {
            lock.unlock();
        }
    }
}

