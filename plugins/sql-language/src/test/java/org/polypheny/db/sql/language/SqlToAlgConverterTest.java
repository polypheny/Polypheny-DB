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

package org.polypheny.db.sql.language;


import com.google.common.collect.ImmutableSet;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.constant.ConformanceEnum;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.externalize.AlgXmlWriter;
import org.polypheny.db.config.PolyphenyDbConnectionConfigImpl;
import org.polypheny.db.config.PolyphenyDbConnectionProperty;
import org.polypheny.db.languages.NodeToAlgConverter;
import org.polypheny.db.languages.NodeToAlgConverter.Config;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.sql.DiffRepository;
import org.polypheny.db.sql.language.fun.SqlCaseOperator;
import org.polypheny.db.sql.language.validate.SqlDelegatingConformance;
import org.polypheny.db.sql.sql2alg.SqlToAlgConverter;
import org.polypheny.db.util.Bug;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.TestUtil;
import org.polypheny.db.util.Util;


/**
 * Unit test for {@link SqlToAlgConverter}.
 */
@Disabled // refactor
public class SqlToAlgConverterTest extends SqlToAlgTestBase {


    public SqlToAlgConverterTest() {
        super();
    }


    @Override
    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup( SqlToAlgConverterTest.class );
    }


    /**
     * Sets the SQL statement for a test.
     */
    public final Sql sql( String sql ) {
        return new Sql( sql, true, true, tester, false, Config.DEFAULT, tester.getConformance() );
    }


    protected final void check( String sql, String plan ) {
        sql( sql ).convertsTo( plan );
    }


    @Test
    @Disabled // refactor
    public void testDotLiteralAfterNestedRow() {
        final String sql = "select ((1,2),(3,4,5)).\"EXPR$1\".\"EXPR$2\" from emp";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testDotLiteralAfterRow() {
        final String sql = "select row(1,2).\"EXPR$1\" from emp";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testIntegerLiteral() {
        final String sql = "select 1 from emp";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testIntervalLiteralYearToMonth() {
        final String sql = """
                select
                  cast(empno as Integer) * (INTERVAL '1-1' YEAR TO MONTH)
                from emp""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testIntervalLiteralHourToMinute() {
        final String sql = """
                select
                 cast(empno as Integer) * (INTERVAL '1:1' HOUR TO MINUTE)
                from emp""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testAliasList() {
        final String sql = """
                select a + b from (
                  select deptno, 1 as uno, name from dept
                ) as d(a, b, c)
                where c like 'X%'""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testAliasList2() {
        final String sql = """
                select * from (
                  select a, b, c from (values (1, 2, 3)) as t (c, b, a)
                ) join dept on dept.deptno = c
                order by c + a""";
        sql( sql ).ok();
    }


    /**
     * Test case for "struct type alias should not cause IOOBE".
     */
    @Test
    @Disabled // refactor
    public void testStructTypeAlias() {
        final String sql = "select t.r AS myRow \n"
                + "from (select row(row(1)) r from dept) t";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinUsingDynamicTable() {
        final String sql = """
                select * from SALES.NATION t1
                join SALES.NATION t2
                using (n_nationkey)""";
        sql( sql ).ok();
    }


    /**
     * Tests that AND(x, AND(y, z)) gets flattened to AND(x, y, z).
     */
    @Test
    @Disabled // refactor
    public void testMultiAnd() {
        final String sql = """
                select * from emp
                where deptno < 10
                and deptno > 5
                and (deptno = 8 or empno < 100)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinOn() {
        final String sql = "SELECT * FROM emp\n"
                + "JOIN dept on emp.deptno = dept.deptno";
        sql( sql ).ok();
    }


    /**
     * Test case for "Off-by-one translation of ON clause of JOIN".
     */
    @Test
    @Disabled // refactor
    public void testConditionOffByOne() {
        // Bug causes the plan to contain
        //   LogicalJoin(condition=[=($9, $9)], joinType=[inner])
        final String sql = "SELECT * FROM emp\n"
                + "JOIN dept on emp.deptno + 0 = dept.deptno";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testConditionOffByOneReversed() {
        final String sql = "SELECT * FROM emp\n"
                + "JOIN dept on dept.deptno = emp.deptno + 0";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinOnExpression() {
        final String sql = "SELECT * FROM emp\n"
                + "JOIN dept on emp.deptno + 1 = dept.deptno - 2";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinOnIn() {
        final String sql = "select * from emp join dept\n"
                + " on emp.deptno = dept.deptno and emp.empno in (1, 3)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinOnInSubQuery() {
        final String sql = """
                select * from emp left join dept
                on emp.empno = 1
                or dept.deptno in (select deptno from emp where empno > 5)""";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinOnExists() {
        final String sql = """
                select * from emp left join dept
                on emp.empno = 1
                or exists (select deptno from emp where empno > dept.deptno + 5)""";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinUsing() {
        sql( "SELECT * FROM emp JOIN dept USING (deptno)" ).ok();
    }


    /**
     * Test case for "JOIN ... USING fails in 3-way join with UnsupportedOperationException".
     */
    @Test
    @Disabled // refactor
    public void testJoinUsingThreeWay() {
        final String sql = """
                select *
                from emp as e
                join dept as d using (deptno)
                join emp as e2 using (empno)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinUsingCompound() {
        final String sql = "SELECT * FROM emp LEFT JOIN (SELECT *, deptno * 5 as empno FROM dept) USING (deptno,empno)";
        sql( sql ).ok();
    }


    /**
     * Test case for "NullPointerException using USING on table alias with column aliases".
     */
    @Test
    @Disabled // refactor
    public void testValuesUsing() {
        final String sql = """
                select d.deptno, min(e.empid) as empid
                from (values (100, 'Bill', 1)) as e(empid, name, deptno)
                join (values (1, 'LeaderShip')) as d(deptno, name)
                  using (deptno)
                group by d.deptno""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinNatural() {
        sql( "SELECT * FROM emp NATURAL JOIN dept" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinNaturalNoCommonColumn() {
        final String sql = "SELECT *\n"
                + "FROM emp NATURAL JOIN (SELECT deptno AS foo, name FROM dept) AS d";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinNaturalMultipleCommonColumn() {
        final String sql = """
                SELECT *
                FROM emp
                NATURAL JOIN (SELECT deptno, name AS ename FROM dept) AS d""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinWithUnion() {
        final String sql = """
                select grade
                from (select empno from emp union select deptno from dept),
                  salgrade""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroup() {
        sql( "select deptno from emp group by deptno" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupByAlias() {
        sql( "select empno as d from emp group by d" ).conformance( ConformanceEnum.LENIENT ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupByAliasOfSubExpressionsInProject() {
        final String sql = "select deptno+empno as d, deptno+empno+mgr\n"
                + "from emp group by d,mgr";
        sql( sql ).conformance( ConformanceEnum.LENIENT ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupByAliasEqualToColumnName() {
        sql( "select empno, ename as deptno from emp group by empno, deptno" ).conformance( ConformanceEnum.LENIENT ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupByOrdinal() {
        sql( "select empno from emp group by 1" ).conformance( ConformanceEnum.LENIENT ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupByContainsLiterals() {
        final String sql = "select count(*) from (\n"
                + "  select 1 from emp group by substring(ename from 2 for 3))";
        sql( sql ).conformance( ConformanceEnum.LENIENT ).ok();
    }


    @Test
    @Disabled // refactor
    public void testAliasInHaving() {
        sql( "select count(empno) as e from emp having e > 1" ).conformance( ConformanceEnum.LENIENT ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupJustOneAgg() {
        // just one agg
        final String sql = "select deptno, sum(sal) as sum_sal from emp group by deptno";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupExpressionsInsideAndOut() {
        // Expressions inside and outside aggs. Common sub-expressions should be eliminated: 'sal' always translates to expression #2.
        final String sql = """
                select
                  deptno + 4, sum(sal), sum(3 + sal), 2 * count(sal)
                from emp group by deptno""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testAggregateNoGroup() {
        sql( "select sum(deptno) from emp" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupEmpty() {
        sql( "select sum(deptno) from emp group by ()" ).ok();
    }


    // Same effect as writing "GROUP BY deptno"
    @Test
    @Disabled // refactor
    public void testSingletonGroupingSet() {
        sql( "select sum(sal) from emp group by grouping sets (deptno)" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupingSets() {
        final String sql = """
                select deptno, ename, sum(sal) from emp
                group by grouping sets ((deptno), (ename, deptno))
                order by 2""";
        sql( sql ).ok();
    }


    /**
     * Test case for "Incorrect plan in with with ROLLUP inside GROUPING SETS".
     * <p>
     * Equivalence example:
     * <code>GROUP BY GROUPING SETS (ROLLUP(A, B), CUBE(C,D))</code>
     * is equal to
     * <code>GROUP BY GROUPING SETS ((A,B), (A), (), (C,D), (C), (D) )</code>
     */
    @Test
    @Disabled // refactor
    public void testGroupingSetsWithRollup() {
        final String sql = """
                select deptno, ename, sum(sal) from emp
                group by grouping sets ( rollup(deptno), (ename, deptno))
                order by 2""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupingSetsWithCube() {
        final String sql = """
                select deptno, ename, sum(sal) from emp
                group by grouping sets ( (deptno), CUBE(ename, deptno))
                order by 2""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupingSetsWithRollupCube() {
        final String sql = """
                select deptno, ename, sum(sal) from emp
                group by grouping sets ( CUBE(deptno), ROLLUP(ename, deptno))
                order by 2""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupingSetsProduct() {
        // Example in SQL:2011:
        //   GROUP BY GROUPING SETS ((A, B), (C)), GROUPING SETS ((X, Y), ())
        // is transformed to
        //   GROUP BY GROUPING SETS ((A, B, X, Y), (A, B), (C, X, Y), (C))
        final String sql = """
                select 1
                from (values (0, 1, 2, 3, 4)) as t(a, b, c, x, y)
                group by grouping sets ((a, b), c), grouping sets ((x, y), ())""";
        sql( sql ).ok();
    }


    /**
     * When the GROUPING function occurs with GROUP BY (effectively just one grouping set), we can translate it directly to 1.
     */
    @Test
    @Disabled // refactor
    public void testGroupingFunctionWithGroupBy() {
        final String sql = """
                select
                  deptno, grouping(deptno), count(*), grouping(empno)
                from emp
                group by empno, deptno
                order by 2""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupingFunction() {
        final String sql = """
                select
                  deptno, grouping(deptno), count(*), grouping(empno)
                from emp
                group by rollup(empno, deptno)""";
        sql( sql ).ok();
    }


    /**
     * GROUP BY with duplicates.
     * <p>
     * From SQL spec:
     * <blockquote>NOTE 190 &mdash; That is, a simple <em>group by clause</em> that is not primitive may be transformed into a primitive <em>group by clause</em> by deleting all parentheses, and deleting extra commas as
     * necessary for correct syntax. If there are no grouping columns at all (for example, GROUP BY (), ()), this is transformed to the canonical form GROUP BY ().</blockquote>
     */
    // Same effect as writing "GROUP BY ()"
    @Test
    @Disabled // refactor
    public void testGroupByWithDuplicates() {
        sql( "select sum(sal) from emp group by (), ()" ).ok();
    }


    /**
     * GROUP BY with duplicate (and heavily nested) GROUPING SETS.
     */
    @Test
    @Disabled // refactor
    public void testDuplicateGroupingSets() {
        final String sql = """
                select sum(sal) from emp
                group by sal,
                  grouping sets (deptno,
                    grouping sets ((deptno, ename), ename),
                      (ename)),
                  ()""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupingSetsCartesianProduct() {
        // Equivalent to (a, c), (a, d), (b, c), (b, d)
        final String sql = """
                select 1
                from (values (1, 2, 3, 4)) as t(a, b, c, d)
                group by grouping sets (a, b), grouping sets (c, d)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupingSetsCartesianProduct2() {
        final String sql = """
                select 1
                from (values (1, 2, 3, 4)) as t(a, b, c, d)
                group by grouping sets (a, (a, b)), grouping sets (c), d""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testRollupSimple() {
        // a is nullable so is translated as just "a" b is not null, so is represented as 0 inside Aggregate, then+ using "CASE WHEN i$b THEN NULL ELSE b END"
        final String sql = """
                select a, b, count(*) as c
                from (values (cast(null as integer), 2)) as t(a, b)
                group by rollup(a, b)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testRollup() {
        // Equivalent to {(a, b), (a), ()}  * {(c, d), (c), ()}
        final String sql = """
                select 1
                from (values (1, 2, 3, 4)) as t(a, b, c, d)
                group by rollup(a, b), rollup(c, d)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testRollupTuples() {
        // rollup(b, (a, d)) is (b, a, d), (b), ()
        final String sql = """
                select 1
                from (values (1, 2, 3, 4)) as t(a, b, c, d)
                group by rollup(b, (a, d))""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCube() {
        // cube(a, b) is {(a, b), (a), (b), ()}
        final String sql = """
                select 1
                from (values (1, 2, 3, 4)) as t(a, b, c, d)
                group by cube(a, b)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupingSetsWith() {
        final String sql = """
                with t(a, b, c, d) as (values (1, 2, 3, 4))
                select 1 from t
                group by rollup(a, b), rollup(c, d)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testHaving() {
        // empty group-by clause, having
        final String sql = "select sum(sal + sal) from emp having sum(sal) > 10";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupBug281() {
        // Dtbug 281 gives:
        //   Internal error:
        //   Type 'RecordType(VARCHAR(128) $f0)' has no field 'NAME'
        final String sql = "select name from (select name from dept group by name)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupBug281b() {
        // Try to confuse it with spurious columns.
        final String sql = """
                select name, foo from (
                select deptno, name, count(deptno) as foo
                from dept
                group by name, deptno, name)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testGroupByExpression() {
        // This used to cause an infinite loop, SqlValidatorImpl.getValidatedNodeType calling getValidatedNodeTypeIfKnown calling getValidatedNodeType.
        final String sql = """
                select count(*)
                from emp
                group by substring(ename FROM 1 FOR 1)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testAggDistinct() {
        final String sql = """
                select deptno, sum(sal), sum(distinct sal), count(*)
                from emp
                group by deptno""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testAggFilter() {
        final String sql = """
                select
                  deptno, sum(sal * 2) filter (where empno < 10), count(*)
                from emp
                group by deptno""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testAggFilterWithIn() {
        final String sql = """
                select
                  deptno, sum(sal * 2) filter (where empno not in (1, 2)), count(*)
                from emp
                group by deptno""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testFakeStar() {
        sql( "SELECT * FROM (VALUES (0, 0)) AS T(A, \"*\")" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testSelectDistinct() {
        sql( "select distinct sal + 5 from emp" ).ok();
    }


    /**
     * Test case for "DISTINCT flag in windowed aggregates".
     */
    @Test
    @Disabled // refactor
    public void testSelectOverDistinct() {
        // Checks to see if <aggregate>(DISTINCT x) is set and preserved as a flag for the aggregate call.
        final String sql = """
                select SUM(DISTINCT deptno)
                over (ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
                from emp
                """;
        sql( sql ).ok();
    }


    /**
     * As {@link #testSelectOverDistinct()} but for streaming queries.
     */
    @Test
    @Disabled // refactor
    public void testSelectStreamPartitionDistinct() {
        final String sql = """
                select stream
                  count(distinct orderId) over (partition by productId
                    order by rowtime
                    range interval '1' second preceding) as c,
                  count(distinct orderId) over w as c2,
                  count(orderId) over w as c3
                from orders
                window w as (partition by productId)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testSelectDistinctGroup() {
        sql( "select distinct sum(sal) from emp group by deptno" ).ok();
    }


    /**
     * Tests that if the clause of SELECT DISTINCT contains duplicate expressions, they are only aggregated once.
     */
    @Test
    @Disabled // refactor
    public void testSelectDistinctDup() {
        final String sql = "select distinct sal + 5, deptno, sal + 5 from emp where deptno < 10";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testSelectWithoutFrom() {
        final String sql = "select 2+2";
        sql( sql ).ok();
    }


    /**
     * Tests referencing columns from a sub-query that has duplicate column names. I think the standard says that this is illegal. We roll with it, and rename the second column to "e0".
     */
    @Test
    @Disabled // refactor
    public void testDuplicateColumnsInSubQuery() {
        String sql = "select \"e\" from (\n"
                + "select empno as \"e\", deptno as d, 1 as \"e\" from EMP)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrder() {
        final String sql = "select empno from emp order by empno";
        sql( sql ).ok();

        // duplicate field is dropped, so plan is same
        final String sql2 = "select empno from emp order by empno, empno asc";
        sql( sql2 ).ok();

        // ditto
        final String sql3 = "select empno from emp order by empno, empno desc";
        sql( sql3 ).ok();
    }


    /**
     * Tests that if a column occurs twice in ORDER BY, only the first key is kept.
     */
    @Test
    @Disabled // refactor
    public void testOrderBasedRepeatFields() {
        final String sql = "select empno from emp order by empno DESC, empno ASC";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderDescNullsLast() {
        final String sql = "select empno from emp order by empno desc nulls last";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderByOrdinalDesc() {
        // FRG-98
        if ( !tester.getConformance().isSortByOrdinal() ) {
            return;
        }
        final String sql = "select empno + 1, deptno, empno from emp order by 2 desc";
        sql( sql ).ok();

        // ordinals rounded down, so 2.5 should have same effect as 2, and generate identical plan
        final String sql2 =
                "select empno + 1, deptno, empno from emp order by 2.5 desc";
        sql( sql2 ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderDistinct() {
        // The relexp aggregates by 3 expressions - the 2 select expressions plus the one to sort on. A little inefficient, but acceptable.
        final String sql = "select distinct empno, deptno + 1\n"
                + "from emp order by deptno + 1 + empno";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderByNegativeOrdinal() {
        // Regardless of whether sort-by-ordinals is enabled, negative ordinals are treated like ordinary numbers.
        final String sql = "select empno + 1, deptno, empno from emp order by -1 desc";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderByOrdinalInExpr() {
        // Regardless of whether sort-by-ordinals is enabled, ordinals inside expressions are treated like integers.
        final String sql = "select empno + 1, deptno, empno from emp order by 1 + 2 desc";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderByIdenticalExpr() {
        // Expression in ORDER BY clause is identical to expression in SELECT clause, so plan should not need an extra project.
        final String sql = "select empno + 1 from emp order by deptno asc, empno + 1 desc";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderByAlias() {
        final String sql = "select empno + 1 as x, empno - 2 as y from emp order by y";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderByAliasInExpr() {
        final String sql = "select empno + 1 as x, empno - 2 as y\n"
                + "from emp order by y + 3";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderByAliasOverrides() {
        if ( !tester.getConformance().isSortByAlias() ) {
            return;
        }

        // plan should contain '(empno + 1) + 3'
        final String sql = "select empno + 1 as empno, empno - 2 as y\n"
                + "from emp order by empno + 3";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderByAliasDoesNotOverride() {
        if ( tester.getConformance().isSortByAlias() ) {
            return;
        }

        // plan should contain 'empno + 3', not '(empno + 1) + 3'
        final String sql = "select empno + 1 as empno, empno - 2 as y\n"
                + "from emp order by empno + 3";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderBySameExpr() {
        final String sql = "select empno from emp, dept\n"
                + "order by sal + empno desc, sal * empno, sal + empno desc";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderUnion() {
        final String sql = """
                select empno, sal from emp
                union all
                select deptno, deptno from dept
                order by sal desc, empno asc""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderUnionOrdinal() {
        if ( !tester.getConformance().isSortByOrdinal() ) {
            return;
        }
        final String sql = """
                select empno, sal from emp
                union all
                select deptno, deptno from dept
                order by 2""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderUnionExprs() {
        final String sql = """
                select empno, sal from emp
                union all
                select deptno, deptno from dept
                order by empno * sal + 2""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderOffsetFetch() {
        final String sql = "select empno from emp\n"
                + "order by empno offset 10 rows fetch next 5 rows only";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderOffsetFetchWithDynamicParameter() {
        final String sql = "select empno from emp\n"
                + "order by empno offset ? rows fetch next ? rows only";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOffsetFetch() {
        final String sql = "select empno from emp\n"
                + "offset 10 rows fetch next 5 rows only";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOffsetFetchWithDynamicParameter() {
        final String sql = "select empno from emp\n"
                + "offset ? rows fetch next ? rows only";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOffset() {
        final String sql = "select empno from emp offset 10 rows";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOffsetWithDynamicParameter() {
        final String sql = "select empno from emp offset ? rows";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testFetch() {
        final String sql = "select empno from emp fetch next 5 rows only";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testFetchWithDynamicParameter() {
        final String sql = "select empno from emp fetch next ? rows only";
        sql( sql ).ok();
    }


    /**
     * Test case for "SqlValidatorUtil.uniquify() may not terminate under some conditions".
     */
    @Test
    @Disabled // refactor
    public void testGroupAlias() {
        final String sql = """
                select "$f2", max(x), max(x + 1)
                from (values (1, 2)) as t("$f2", x)
                group by "$f2\"""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderGroup() {
        final String sql = """
                select deptno, count(*)
                from emp
                group by deptno
                order by deptno * sum(sal) desc, min(empno)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCountNoGroup() {
        final String sql = """
                select count(*), sum(sal)
                from emp
                where empno > 10""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWith() {
        final String sql = "with emp2 as (select * from emp)\n"
                + "select * from emp2";
        sql( sql ).ok();
    }


    /**
     * Test case for "WITH ... ORDER BY query gives AssertionError".
     */
    @Test
    @Disabled // refactor
    public void testWithOrder() {
        final String sql = "with emp2 as (select * from emp)\n"
                + "select * from emp2 order by deptno";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWithUnionOrder() {
        final String sql = """
                with emp2 as (select empno, deptno as x from emp)
                select * from emp2
                union all
                select * from emp2
                order by empno + x""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWithUnion() {
        final String sql = """
                with emp2 as (select * from emp where deptno > 10)
                select empno from emp2 where deptno < 30
                union all
                select deptno from emp""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWithAlias() {
        final String sql = """
                with w(x, y) as
                  (select * from dept where deptno > 10)
                select x from w where x < 30 union all select deptno from dept""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWithInsideWhereExists() {
        final String sql = """
                select * from emp
                where exists (
                  with dept2 as (select * from dept where dept.deptno >= emp.deptno)
                  select 1 from dept2 where deptno <= emp.deptno)""";
        sql( sql ).decorrelate( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWithInsideWhereExistsRex() {
        final String sql = """
                select * from emp
                where exists (
                  with dept2 as (select * from dept where dept.deptno >= emp.deptno)
                  select 1 from dept2 where deptno <= emp.deptno)""";
        sql( sql ).decorrelate( false ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWithInsideWhereExistsDecorrelate() {
        final String sql = """
                select * from emp
                where exists (
                  with dept2 as (select * from dept where dept.deptno >= emp.deptno)
                  select 1 from dept2 where deptno <= emp.deptno)""";
        sql( sql ).decorrelate( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWithInsideWhereExistsDecorrelateRex() {
        final String sql = """
                select * from emp
                where exists (
                  with dept2 as (select * from dept where dept.deptno >= emp.deptno)
                  select 1 from dept2 where deptno <= emp.deptno)""";
        sql( sql ).decorrelate( true ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWithInsideScalarSubQuery() {
        final String sql = """
                select (
                 with dept2 as (select * from dept where deptno > 10) select count(*) from dept2) as c
                from emp""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWithInsideScalarSubQueryRex() {
        final String sql = """
                select (
                 with dept2 as (select * from dept where deptno > 10) select count(*) from dept2) as c
                from emp""";
        sql( sql ).expand( false ).ok();
    }


    /**
     * Test case for "AssertionError while translating query with WITH and correlated sub-query".
     */
    @Test
    @Disabled // refactor
    public void testWithExists() {
        final String sql = """
                with t (a, b) as (select * from (values (1, 2)))
                select * from t where exists (
                  select 1 from emp where deptno = t.a)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testTableSubset() {
        final String sql = "select deptno, name from dept";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testTableExpression() {
        final String sql = "select deptno + deptno from dept";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testTableExtend() {
        final String sql = "select * from dept extend (x varchar(5) not null)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testTableExtendSubset() {
        final String sql = "select deptno, x from dept extend (x int)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testTableExtendExpression() {
        final String sql = "select deptno + x from dept extend (x int not null)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUpdateExtendedColumnCollision() {
        sql( "update empdefaults(empno INTEGER NOT NULL, deptno INTEGER)"
                + " set deptno = 1, empno = 20, ename = 'Bob'"
                + " where deptno = 10" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUpdateExtendedColumnCaseSensitiveCollision() {
        sql( "update empdefaults(\"slacker\" INTEGER, deptno INTEGER)"
                + " set deptno = 1, \"slacker\" = 100"
                + " where ename = 'Bob'" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testExplicitTable() {
        sql( "table emp" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCollectionTable() {
        sql( "select * from table(ramp(3))" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCollectionTableWithLateral() {
        sql( "select * from dept, lateral table(ramp(dept.deptno))" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCollectionTableWithLateral2() {
        sql( "select * from dept, lateral table(ramp(deptno))" ).ok();
    }


    /**
     * Test case for "IndexOutOfBoundsException when using LATERAL TABLE with more than one field".
     */
    @Test
    @Disabled // refactor
    public void testCollectionTableWithLateral3() {
        sql( "select * from dept, lateral table(DEDUP(dept.deptno, dept.name))" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testSample() {
        final String sql = "select * from emp tablesample substitute('DATASET1') where empno > 5";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testSampleQuery() {
        final String sql = """
                select * from (
                 select * from emp as e tablesample substitute('DATASET1')
                 join dept on e.deptno = dept.deptno
                ) tablesample substitute('DATASET2')
                where empno > 5""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testSampleBernoulli() {
        final String sql = "select * from emp tablesample bernoulli(50) where empno > 5";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testSampleBernoulliQuery() {
        final String sql = """
                select * from (
                 select * from emp as e tablesample bernoulli(10) repeatable(1)
                 join dept on e.deptno = dept.deptno
                ) tablesample bernoulli(50) repeatable(99)
                where empno > 5""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testSampleSystem() {
        final String sql = "select * from emp tablesample system(50) where empno > 5";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testSampleSystemQuery() {
        final String sql = """
                select * from (
                 select * from emp as e tablesample system(10) repeatable(1)
                 join dept on e.deptno = dept.deptno
                ) tablesample system(50) repeatable(99)
                where empno > 5""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCollectionTableWithCursorParam() {
        final String sql = "select * from table(dedup(cursor(select ename from emp), cursor(select name from dept), 'NAME'))";
        sql( sql ).decorrelate( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUnnest() {
        final String sql = "select*from unnest(multiset[1,2])";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUnnestSubQuery() {
        final String sql = "select*from unnest(multiset(select*from dept))";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUnnestArrayAggPlan() {
        final String sql = """
                select d.deptno, e2.empno_avg
                from dept_nested as d outer apply
                 (select avg(e.empno) as empno_avg from UNNEST(d.employees) as e) e2""";
        sql( sql ).conformance( ConformanceEnum.LENIENT ).ok();
    }


    @Test
    @Disabled
    public void testUnnestArrayPlan() {
        final String sql = """
                select d.deptno, e2.empno
                from dept_nested as d,
                 UNNEST(d.employees) e2""";
        sql( sql ).ok();
    }


    @Test
    @Disabled
    public void testUnnestArrayPlanAs() {
        final String sql = """
                select d.deptno, e2.empno
                from dept_nested as d,
                 UNNEST(d.employees) as e2(empno, y, z)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testArrayOfRecord() {
        sql( "select employees[1].detail.skills[2+3].desc from dept_nested" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testFlattenRecords() {
        sql( "select employees[1] from dept_nested" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUnnestArray() {
        sql( "select*from unnest(array(select*from dept))" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUnnestWithOrdinality() {
        final String sql = "select*from unnest(array(select*from dept)) with ordinality";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testMultisetSubQuery() {
        final String sql = "select multiset(select deptno from dept) from (values(true))";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testMultiset() {
        final String sql = "select 'a',multiset[10] from dept";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testMultisetOfColumns() {
        final String sql = "select 'abc',multiset[deptno,sal] from emp";
        sql( sql ).expand( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testMultisetOfColumnsRex() {
        sql( "select 'abc',multiset[deptno,sal] from emp" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCorrelationJoin() {
        final String sql = """
                select *,
                  multiset(select * from emp where deptno=dept.deptno) as empset
                from dept""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCorrelationJoinRex() {
        final String sql = """
                select *,
                  multiset(select * from emp where deptno=dept.deptno) as empset
                from dept""";
        sql( sql ).expand( false ).ok();
    }


    /**
     * Test case for "Correlation variable has incorrect row type if it is populated by right side of a Join".
     */
    @Test
    @Disabled // refactor
    public void testCorrelatedSubQueryInJoin() {
        final String sql = """
                select *
                from emp as e
                join dept as d using (deptno)
                where d.name = (
                  select max(name)
                  from dept as d2
                  where d2.deptno = d.deptno)""";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testExists() {
        final String sql = "select*from emp\n"
                + "where exists (select 1 from dept where deptno=55)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testExistsCorrelated() {
        final String sql = "select*from emp where exists (\n"
                + "  select 1 from dept where emp.deptno=dept.deptno)";
        sql( sql ).decorrelate( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testNotExistsCorrelated() {
        final String sql = "select * from emp where not exists (\n"
                + "  select 1 from dept where emp.deptno=dept.deptno)";
        sql( sql ).decorrelate( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testExistsCorrelatedDecorrelate() {
        final String sql = "select*from emp where exists (\n"
                + "  select 1 from dept where emp.deptno=dept.deptno)";
        sql( sql ).decorrelate( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testExistsCorrelatedDecorrelateRex() {
        final String sql = "select*from emp where exists (\n"
                + "  select 1 from dept where emp.deptno=dept.deptno)";
        sql( sql ).decorrelate( true ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testExistsCorrelatedLimit() {
        final String sql = "select*from emp where exists (\n"
                + "  select 1 from dept where emp.deptno=dept.deptno limit 1)";
        sql( sql ).decorrelate( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testExistsCorrelatedLimitDecorrelate() {
        final String sql = "select*from emp where exists (\n"
                + "  select 1 from dept where emp.deptno=dept.deptno limit 1)";
        sql( sql ).decorrelate( true ).expand( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testExistsCorrelatedLimitDecorrelateRex() {
        final String sql = "select*from emp where exists (\n"
                + "  select 1 from dept where emp.deptno=dept.deptno limit 1)";
        sql( sql ).decorrelate( true ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInValueListShort() {
        final String sql = "select empno from emp where deptno in (10, 20)";
        sql( sql ).ok();
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInValueListLong() {
        // Go over the default threshold of 20 to force a sub-query.
        final String sql = "select empno from emp where deptno in (10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInUncorrelatedSubQuery() {
        final String sql = "select empno from emp where deptno in (select deptno from dept)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInUncorrelatedSubQueryRex() {
        final String sql = "select empno from emp where deptno in (select deptno from dept)";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCompositeInUncorrelatedSubQueryRex() {
        final String sql = "select empno from emp where (empno, deptno) in (select deptno - 10, deptno from dept)";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testNotInUncorrelatedSubQuery() {
        final String sql = "select empno from emp where deptno not in (select deptno from dept)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testAllValueList() {
        final String sql = "select empno from emp where deptno > all (10, 20)";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testSomeValueList() {
        final String sql = "select empno from emp where deptno > some (10, 20)";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testSome() {
        final String sql = "select empno from emp where deptno > some (\n"
                + "  select deptno from dept)";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testNotInUncorrelatedSubQueryRex() {
        final String sql = "select empno from emp where deptno not in (select deptno from dept)";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testNotCaseInThreeClause() {
        final String sql = "select empno from emp where not case when true then deptno in (10,20) else true end";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testNotCaseInMoreClause() {
        final String sql = "select empno from emp where not case when true then deptno in (10,20) when false then false else deptno in (30,40) end";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testNotCaseInWithoutElse() {
        final String sql = "select empno from emp where not case when true then deptno in (10,20)  end";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWhereInCorrelated() {
        final String sql = """
                select empno from emp as e
                join dept as d using (deptno)
                where e.sal in (
                  select e2.sal from emp as e2 where e2.deptno > e.deptno)""";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInUncorrelatedSubQueryInSelect() {
        // In the SELECT clause, the value of IN remains in 3-valued logic -- it's not forced into 2-valued by the "... IS TRUE" wrapper as in the WHERE clause -- so the translation is more complicated.
        final String sql = """
                select name, deptno in (
                  select case when true then deptno else null end from emp)
                from dept""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInUncorrelatedSubQueryInSelectRex() {
        // In the SELECT clause, the value of IN remains in 3-valued logic -- it's not forced into 2-valued by the "... IS TRUE" wrapper as in the WHERE clause -- so the translation is more complicated.
        final String sql = """
                select name, deptno in (
                  select case when true then deptno else null end from emp)
                from dept""";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInUncorrelatedSubQueryInHavingRex() {
        final String sql = """
                select sum(sal) as s
                from emp
                group by deptno
                having count(*) > 2
                and deptno in (
                  select case when true then deptno else null end from emp)""";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUncorrelatedScalarSubQueryInOrderRex() {
        final String sql = """
                select ename
                from emp
                order by (select case when true then deptno else null end from emp) desc,
                  ename""";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUncorrelatedScalarSubQueryInGroupOrderRex() {
        final String sql = """
                select sum(sal) as s
                from emp
                group by deptno
                order by (select case when true then deptno else null end from emp) desc,
                  count(*)""";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUncorrelatedScalarSubQueryInAggregateRex() {
        final String sql = """
                select sum((select min(deptno) from emp)) as s
                from emp
                group by deptno
                """;
        sql( sql ).expand( false ).ok();
    }


    /**
     * Plan should be as {@link #testInUncorrelatedSubQueryInSelect}, but with an extra NOT. Both queries require 3-valued logic.
     */
    @Test
    @Disabled // refactor
    public void testNotInUncorrelatedSubQueryInSelect() {
        final String sql = """
                select empno, deptno not in (
                  select case when true then deptno else null end from dept)
                from emp""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testNotInUncorrelatedSubQueryInSelectRex() {
        final String sql = """
                select empno, deptno not in (
                  select case when true then deptno else null end from dept)
                from emp""";
        sql( sql ).expand( false ).ok();
    }


    /**
     * Since 'deptno NOT IN (SELECT deptno FROM dept)' can not be null, we generate a simpler plan.
     */
    @Test
    @Disabled // refactor
    public void testNotInUncorrelatedSubQueryInSelectNotNull() {
        final String sql = """
                select empno, deptno not in (
                  select deptno from dept)
                from emp""";
        sql( sql ).ok();
    }


    /**
     * Since 'deptno NOT IN (SELECT mgr FROM emp)' can be null, we need a more complex plan, including counts of null and not-null keys.
     */
    @Test
    @Disabled // refactor
    public void testNotInUncorrelatedSubQueryInSelectMayBeNull() {
        final String sql = """
                select empno, deptno not in (
                  select mgr from emp)
                from emp""";
        sql( sql ).ok();
    }


    /**
     * Even though "mgr" allows nulls, we can deduce from the WHERE clause that it will never be null. Therefore we can generate a simpler plan.
     */
    @Test
    @Disabled // refactor
    public void testNotInUncorrelatedSubQueryInSelectDeduceNotNull() {
        final String sql = """
                select empno, deptno not in (
                  select mgr from emp where mgr > 5)
                from emp""";
        sql( sql ).ok();
    }


    /**
     * Similar to {@link #testNotInUncorrelatedSubQueryInSelectDeduceNotNull()}, using {@code IS NOT NULL}.
     */
    @Test
    @Disabled // refactor
    public void testNotInUncorrelatedSubQueryInSelectDeduceNotNull2() {
        final String sql = """
                select empno, deptno not in (
                  select mgr from emp where mgr is not null)
                from emp""";
        sql( sql ).ok();
    }


    /**
     * Similar to {@link #testNotInUncorrelatedSubQueryInSelectDeduceNotNull()}, using {@code IN}.
     */
    @Test
    @Disabled // refactor
    public void testNotInUncorrelatedSubQueryInSelectDeduceNotNull3() {
        final String sql = """
                select empno, deptno not in (
                  select mgr from emp where mgr in (
                    select mgr from emp where deptno = 10))
                from emp""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testNotInUncorrelatedSubQueryInSelectNotNullRex() {
        final String sql = """
                select empno, deptno not in (
                  select deptno from dept)
                from emp""";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUnnestSelect() {
        final String sql = "select*from unnest(select multiset[deptno] from dept)";
        sql( sql ).expand( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUnnestSelectRex() {
        final String sql = "select*from unnest(select multiset[deptno] from dept)";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinUnnest() {
        final String sql = "select*from dept as d, unnest(multiset[d.deptno * 2])";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJoinUnnestRex() {
        final String sql = "select*from dept as d, unnest(multiset[d.deptno * 2])";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testLateral() {
        final String sql = "select * from emp,\n"
                + "  LATERAL (select * from dept where emp.deptno=dept.deptno)";
        sql( sql ).decorrelate( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testLateralDecorrelate() {
        final String sql = "select * from emp,\n"
                + " LATERAL (select * from dept where emp.deptno=dept.deptno)";
        sql( sql ).decorrelate( true ).expand( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testLateralDecorrelateRex() {
        final String sql = "select * from emp,\n"
                + " LATERAL (select * from dept where emp.deptno=dept.deptno)";
        sql( sql ).decorrelate( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testLateralDecorrelateThetaRex() {
        final String sql = "select * from emp,\n"
                + " LATERAL (select * from dept where emp.deptno < dept.deptno)";
        sql( sql ).decorrelate( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testNestedCorrelations() {
        final String sql = """
                select *
                from (select 2+deptno d2, 3+deptno d3 from emp) e
                 where exists (select 1 from (select deptno+1 d1 from dept) d
                 where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)
                 where d4=d.d1 and d5=d.d1 and d6=e.d3))""";
        sql( sql ).decorrelate( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testNestedCorrelationsDecorrelated() {
        final String sql = """
                select *
                from (select 2+deptno d2, 3+deptno d3 from emp) e
                 where exists (select 1 from (select deptno+1 d1 from dept) d
                 where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)
                 where d4=d.d1 and d5=d.d1 and d6=e.d3))""";
        sql( sql ).decorrelate( true ).expand( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testNestedCorrelationsDecorrelatedRex() {
        final String sql = """
                select *
                from (select 2+deptno d2, 3+deptno d3 from emp) e
                 where exists (select 1 from (select deptno+1 d1 from dept) d
                 where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)
                 where d4=d.d1 and d5=d.d1 and d6=e.d3))""";
        sql( sql ).decorrelate( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testElement() {
        sql( "select element(multiset[5]) from emp" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testElementInValues() {
        sql( "values element(multiset[5])" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUnionAll() {
        final String sql = "select empno from emp union all select deptno from dept";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUnion() {
        final String sql = "select empno from emp union select deptno from dept";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUnionValues() {
        // union with values
        final String sql = """
                values (10), (20)
                union all
                select 34 from emp
                union all values (30), (45 + 10)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUnionSubQuery() {
        // union of sub-query, inside from list, also values
        final String sql = """
                select deptno from emp as emp0 cross join
                 (select empno from emp union all
                  select deptno from dept where deptno > 20 union all
                  values (45), (67))""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testIsDistinctFrom() {
        final String sql = """
                select empno is distinct from deptno
                from (values (cast(null as int), 1),
                             (2, cast(null as int))) as emp(empno, deptno)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testIsNotDistinctFrom() {
        final String sql = """
                select empno is not distinct from deptno
                from (values (cast(null as int), 1),
                             (2, cast(null as int))) as emp(empno, deptno)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testNotLike() {
        // note that 'x not like y' becomes 'not(x like y)'
        final String sql = "values ('a' not like 'b' escape 'c')";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testTumble() {
        final String sql = """
                select STREAM
                  TUMBLE_START(rowtime, INTERVAL '1' MINUTE) AS s,
                  TUMBLE_END(rowtime, INTERVAL '1' MINUTE) AS e
                from Shipments
                GROUP BY TUMBLE(rowtime, INTERVAL '1' MINUTE)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testNotNotIn() {
        final String sql = "select * from EMP where not (ename not in ('Fred') )";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOverMultiple() {
        final String sql = """
                select sum(sal) over w1,
                  sum(deptno) over w1,
                  sum(deptno) over w2
                from emp
                where deptno - sal > 999
                window w1 as (partition by job order by hiredate rows 2 preceding),
                  w2 as (partition by job order by hiredate rows 3 preceding disallow partial),
                  w3 as (partition by job order by hiredate range interval '1' second preceding)""";
        sql( sql ).ok();
    }


    /**
     * Test case for "Allow windowed aggregate on top of regular aggregate".
     */
    @Test
    @Disabled // refactor
    public void testNestedAggregates() {
        final String sql = """
                SELECT
                  avg(sum(sal) + 2 * min(empno) + 3 * avg(empno))
                  over (partition by deptno)
                from emp
                group by deptno""";
        sql( sql ).ok();
    }


    /**
     * Test one of the custom conversions which is recognized by the class of the operator (in this case, {@link SqlCaseOperator}).
     */
    @Test
    @Disabled // refactor
    public void testCase() {
        sql( "values (case 'a' when 'a' then 1 end)" ).ok();
    }


    /**
     * Tests one of the custom conversions which is recognized by the identity of the operator (in this case, {@link OperatorRegistry #CHARACTER_LENGTH}).
     */
    @Test
    @Disabled // refactor
    public void testCharLength() {
        // Note that CHARACTER_LENGTH becomes CHAR_LENGTH.
        sql( "values (character_length('foo'))" ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOverAvg() {
        // AVG(x) gets translated to SUM(x)/COUNT(x).  Because COUNT controls the return type there usually needs to be a final CAST to get the result back to match the type of x.
        final String sql = """
                select sum(sal) over w1,
                  avg(sal) over w1
                from emp
                window w1 as (partition by job order by hiredate rows 2 preceding)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOverAvg2() {
        // Check to see if extra CAST is present.  Because CAST is nested inside AVG it passed to both SUM and COUNT so the outer final CAST isn't needed.
        final String sql = """
                select sum(sal) over w1,
                  avg(CAST(sal as real)) over w1
                from emp
                window w1 as (partition by job order by hiredate rows 2 preceding)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOverCountStar() {
        final String sql = """
                select count(sal) over w1,
                  count(*) over w1
                from emp
                window w1 as (partition by job order by hiredate rows 2 preceding)""";
        sql( sql ).ok();
    }


    /**
     * Tests that a window containing only ORDER BY is implicitly CURRENT ROW.
     */
    @Test
    @Disabled // refactor
    public void testOverOrderWindow() {
        final String sql = """
                select last_value(deptno) over w
                from emp
                window w as (order by empno)""";
        sql( sql ).ok();

        // Same query using inline window
        final String sql2 = """
                select last_value(deptno) over (order by empno)
                from emp
                """;
        sql( sql2 ).ok();
    }


    /**
     * Tests that a window with a FOLLOWING bound becomes BETWEEN CURRENT ROW AND FOLLOWING.
     */
    @Test
    @Disabled // refactor
    public void testOverOrderFollowingWindow() {
        // Window contains only ORDER BY (implicitly CURRENT ROW).
        final String sql = """
                select last_value(deptno) over w
                from emp
                window w as (order by empno rows 2 following)""";
        sql( sql ).ok();

        // Same query using inline window
        final String sql2 = """
                select
                  last_value(deptno) over (order by empno rows 2 following)
                from emp
                """;
        sql( sql2 ).ok();
    }


    @Test
    @Disabled // refactor
    public void testTumbleTable() {
        final String sql = """
                select stream tumble_end(rowtime, interval '2' hour) as rowtime, productId
                from orders
                group by tumble(rowtime, interval '2' hour), productId""";
        sql( sql ).ok();
    }


    /**
     * As {@link #testTumbleTable()} but on a table where "rowtime" is at position 1 not 0.
     */
    @Test
    @Disabled // refactor
    public void testTumbleTableRowtimeNotFirstColumn() {
        final String sql = """
                select stream
                   tumble_end(rowtime, interval '2' hour) as rowtime, orderId
                from shipments
                group by tumble(rowtime, interval '2' hour), orderId""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testHopTable() {
        final String sql = """
                select stream hop_start(rowtime, interval '1' hour, interval '3' hour) as rowtime,
                  count(*) as c
                from orders
                group by hop(rowtime, interval '1' hour, interval '3' hour)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testSessionTable() {
        final String sql = """
                select stream session_start(rowtime, interval '1' hour) as rowtime,
                  session_end(rowtime, interval '1' hour),
                  count(*) as c
                from orders
                group by session(rowtime, interval '1' hour)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInterval() {
        // temporarily disabled per DTbug 1212
        if ( !Bug.DT785_FIXED ) {
            return;
        }
        final String sql = "values(cast(interval '1' hour as interval hour to second))";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testStream() {
        final String sql = "select stream productId from orders where productId = 10";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testStreamGroupBy() {
        final String sql = """
                select stream
                 floor(rowtime to second) as rowtime, count(*) as c
                from orders
                group by floor(rowtime to second)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testStreamWindowedAggregation() {
        final String sql = """
                select stream *,
                  count(*) over (partition by productId
                    order by rowtime
                    range interval '1' second preceding) as c
                from orders""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testExplainAsXml() {
        String sql = "select 1 + 2, 3 from (values (true))";
        final AlgNode alg = tester.convertSqlToAlg( sql ).alg;
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw );
        AlgXmlWriter planWriter = new AlgXmlWriter( pw, ExplainLevel.EXPPLAN_ATTRIBUTES );
        alg.explain( planWriter );
        pw.flush();
        TestUtil.assertEqualsVerbose(
                """
                        <AlgNode type="LogicalProject">
                        \t<Property name="EXPR$0">
                        \t\t+(1, 2)\t</Property>
                        \t<Property name="EXPR$1">
                        \t\t3\t</Property>
                        \t<Inputs>
                        \t\t<AlgNode type="LogicalValues">
                        \t\t\t<Property name="tuples">
                        \t\t\t\t[{ true }]\t\t\t</Property>
                        \t\t\t<Inputs/>
                        \t\t</AlgNode>
                        \t</Inputs>
                        </AlgNode>
                        """,
                Util.toLinux( sw.toString() ) );
    }


    /**
     * Test case for "AlgFieldTrimmer: when trimming Sort, the collation and trait set don't match".
     */
    @Test
    @Disabled // refactor
    public void testSortWithTrim() {
        final String sql = "select ename from (select * from emp order by sal) a";
        sql( sql ).trim( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOffset0() {
        final String sql = "select * from emp offset 0";
        sql( sql ).ok();
    }


    /**
     * Test group-by CASE expression involving a non-query IN
     */
    @Test
    @Disabled // refactor
    public void testGroupByCaseSubQuery() {
        final String sql = """
                SELECT CASE WHEN emp.empno IN (3) THEN 0 ELSE 1 END
                FROM emp
                GROUP BY (CASE WHEN emp.empno IN (3) THEN 0 ELSE 1 END)""";
        sql( sql ).ok();
    }


    /**
     * Test aggregate function on a CASE expression involving a non-query IN
     */
    @Test
    @Disabled // refactor
    public void testAggCaseSubQuery() {
        final String sql = "SELECT SUM(CASE WHEN empno IN (3) THEN 0 ELSE 1 END) FROM emp";
        sql( sql ).ok();
    }


    /**
     * Test case for "Test aggregate operators do not derive row types with duplicate column names".
     */
    @Test
    @Disabled // refactor
    public void testAggNoDuplicateColumnNames() {
        final String sql = """
                SELECT  empno, EXPR$2, COUNT(empno) FROM (
                    SELECT empno, deptno AS EXPR$2
                    FROM emp)
                GROUP BY empno, EXPR$2""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testAggScalarSubQuery() {
        final String sql = "SELECT SUM(SELECT min(deptno) FROM dept) FROM emp";
        sql( sql ).ok();
    }


    /**
     * Test aggregate function on a CASE expression involving IN with a sub-query.
     * <p>
     * Test case for "Sub-query inside aggregate function".
     */
    @Test
    @Disabled // refactor
    public void testAggCaseInSubQuery() {
        final String sql = """
                SELECT SUM(
                  CASE WHEN deptno IN (SELECT deptno FROM dept) THEN 1 ELSE 0 END)
                FROM emp""";
        sql( sql ).expand( false ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCorrelatedSubQueryInAggregate() {
        final String sql = """
                SELECT SUM(
                  (select char_length(name) from dept
                   where dept.deptno = emp.empno))
                FROM emp""";
        sql( sql ).expand( false ).ok();
    }


    /**
     * Test case for "IN within CASE within GROUP BY gives AssertionError".
     */
    @Test
    @Disabled // refactor
    public void testGroupByCaseIn() {
        final String sql = """
                select
                 (CASE WHEN (deptno IN (10, 20)) THEN 0 ELSE deptno END),
                 min(empno) from EMP
                group by (CASE WHEN (deptno IN (10, 20)) THEN 0 ELSE deptno END)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInsert() {
        final String sql = "insert into empnullables (deptno, empno, ename)\n"
                + "values (10, 150, 'Fred')";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInsertSubset() {
        final String sql = "insert into empnullables\n"
                + "values (50, 'Fred')";
        sql( sql ).conformance( ConformanceEnum.PRAGMATIC_2003 ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInsertWithCustomInitializerExpressionFactory() {
        final String sql = "insert into empdefaults (deptno) values (300)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInsertSubsetWithCustomInitializerExpressionFactory() {
        final String sql = "insert into empdefaults values (100)";
        sql( sql ).conformance( ConformanceEnum.PRAGMATIC_2003 ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInsertBind() {
        final String sql = "insert into empnullables (deptno, empno, ename)\n"
                + "values (?, ?, ?)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInsertBindSubset() {
        final String sql = "insert into empnullables\n"
                + "values (?, ?)";
        sql( sql ).conformance( ConformanceEnum.PRAGMATIC_2003 ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInsertBindWithCustomInitializerExpressionFactory() {
        final String sql = "insert into empdefaults (deptno) values (?)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInsertBindSubsetWithCustomInitializerExpressionFactory() {
        final String sql = "insert into empdefaults values (?)";
        sql( sql ).conformance( ConformanceEnum.PRAGMATIC_2003 ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInsertExtendedColumn() {
        final String sql = "insert into empdefaults(updated TIMESTAMP) (ename, deptno, empno, updated, sal) values ('Fred', 456, 44, timestamp '2017-03-12 13:03:05', 999999)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInsertBindExtendedColumn() {
        final String sql = "insert into empdefaults(updated TIMESTAMP) (ename, deptno, empno, updated, sal) values ('Fred', 456, 44, ?, 999999)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testDelete() {
        final String sql = "delete from emp";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testDeleteWhere() {
        final String sql = "delete from emp where deptno = 10";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testDeleteBind() {
        final String sql = "delete from emp where deptno = ?";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testDeleteBindExtendedColumn() {
        final String sql = "delete from emp(enddate TIMESTAMP) where enddate < ?";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUpdate() {
        final String sql = "update emp set empno = empno + 1";
        sql( sql ).ok();
    }


    @Disabled("POLYPHENYDB-1527")
    @Test
    public void testUpdateSubQuery() {
        final String sql = """
                update emp
                set empno = (
                  select min(empno) from emp as e where e.deptno = emp.deptno)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUpdateWhere() {
        final String sql = "update emp set empno = empno + 1 where deptno = 10";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUpdateExtendedColumn() {
        final String sql = "update empdefaults(updated TIMESTAMP) set deptno = 1, updated = timestamp '2017-03-12 13:03:05', empno = 20, ename = 'Bob' where deptno = 10";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUpdateBind() {
        final String sql = "update emp set sal = sal + ? where slacker = false";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUpdateBind2() {
        final String sql = "update emp set sal = ? where slacker = false";
        sql( sql ).ok();
    }


    @Disabled("POLYPHENYDB-1708")
    @Test
    public void testUpdateBindExtendedColumn() {
        final String sql = "update emp(test INT) set test = ?, sal = sal + 5000 where slacker = false";
        sql( sql ).ok();
    }


    @Disabled("POLYPHENYDB-985")
    @Test
    public void testMerge() {
        final String sql = """
                merge into emp as target
                using (select * from emp where deptno = 30) as source
                on target.empno = source.empno
                when matched then
                  update set sal = sal + source.sal
                when not matched then
                  insert (empno, deptno, sal)
                  values (source.empno, source.deptno, source.sal)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInsertWithCustomColumnResolving() {
        final String sql = "insert into struct.t values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testInsertWithCustomColumnResolving2() {
        final String sql = "insert into struct.t_nullables (f0.c0, f1.c2, c1)\n"
                + "values (?, ?, ?)";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testUpdateWithCustomColumnResolving() {
        final String sql = "update struct.t set c0 = c0 + 1";
        sql( sql ).ok();
    }


    /**
     * Test case for "SqlSingleValueAggFunction is created when it may not be needed".
     */
    @Test
    @Disabled // refactor
    public void testSubQueryAggregateFunctionFollowedBySimpleOperation() {
        final String sql = """
                select deptno
                from EMP
                where deptno > (select min(deptno) * 2 + 10 from EMP)""";
        sql( sql ).ok();
    }


    /**
     * Test case for ""OR .. IN" sub-query conversion wrong".
     * <p>
     * The problem is only fixed if you have {@code expand = false}.
     */
    @Test
    @Disabled // refactor
    public void testSubQueryOr() {
        final String sql = """
                select * from emp where deptno = 10 or deptno in (
                    select dept.deptno from dept where deptno < 5)
                """;
        sql( sql ).expand( false ).ok();
    }


    /**
     * Test case for "SqlSingleValueAggFunction is created when it may not be needed".
     */
    @Test
    @Disabled // refactor
    public void testSubQueryValues() {
        final String sql = """
                select deptno
                from EMP
                where deptno > (values 10)""";
        sql( sql ).ok();
    }


    /**
     * Test case for "SqlSingleValueAggFunction is created when it may not be needed".
     */
    @Test
    @Disabled // refactor
    public void testSubQueryLimitOne() {
        final String sql = """
                select deptno
                from EMP
                where deptno > (select deptno
                from EMP order by deptno limit 1)""";
        sql( sql ).ok();
    }


    /**
     * Test case for "When look up sub-queries, perform the same logic as the way when ones were registered".
     */
    @Test
    @Disabled // refactor
    public void testIdenticalExpressionInSubQuery() {
        final String sql = """
                select deptno
                from EMP
                where deptno in (1, 2) or deptno in (1, 2)""";
        sql( sql ).ok();
    }


    /**
     * Test case for "Scan HAVING clause for sub-queries and IN-lists" relating to IN.
     */
    @Test
    @Disabled // refactor
    public void testHavingAggrFunctionIn() {
        final String sql = """
                select deptno
                from emp
                group by deptno
                having sum(case when deptno in (1, 2) then 0 else 1 end) +
                sum(case when deptno in (3, 4) then 0 else 1 end) > 10""";
        sql( sql ).ok();
    }


    /**
     * Test case for "Scan HAVING clause for sub-queries and IN-lists", with a sub-query in the HAVING clause.
     */
    @Test
    @Disabled // refactor
    public void testHavingInSubQueryWithAggrFunction() {
        final String sql = """
                select sal
                from emp
                group by sal
                having sal in (
                  select deptno
                  from dept
                  group by deptno
                  having sum(deptno) > 0)""";
        sql( sql ).ok();
    }


    /**
     * Test case for "Scalar sub-query and aggregate function in SELECT or HAVING clause gives AssertionError"; variant involving HAVING clause.
     */
    @Test
    @Disabled // refactor
    public void testAggregateAndScalarSubQueryInHaving() {
        final String sql = """
                select deptno
                from emp
                group by deptno
                having max(emp.empno) > (SELECT min(emp.empno) FROM emp)
                """;
        sql( sql ).ok();
    }


    /**
     * Test case for "Scalar sub-query and aggregate function in SELECT or HAVING clause gives AssertionError"; variant involving SELECT clause.
     */
    @Test
    @Disabled // refactor
    public void testAggregateAndScalarSubQueryInSelect() {
        final String sql = """
                select deptno,
                  max(emp.empno) > (SELECT min(emp.empno) FROM emp) as b
                from emp
                group by deptno
                """;
        sql( sql ).ok();
    }


    /**
     * Test case for "window aggregate and ranking functions with grouped aggregates".
     */
    @Test
    @Disabled // refactor
    public void testWindowAggWithGroupBy() {
        final String sql = """
                select min(deptno), rank() over (order by empno),
                max(empno) over (partition by deptno)
                from emp group by deptno, empno
                """;
        sql( sql ).ok();
    }


    /**
     * Test case for "AVG window function in GROUP BY gives AssertionError".
     */
    @Test
    @Disabled // refactor
    public void testWindowAverageWithGroupBy() {
        final String sql = """
                select avg(deptno) over ()
                from emp
                group by deptno""";
        sql( sql ).ok();
    }


    /**
     * Test case for "variant involving joins".
     */
    @Test
    @Disabled // refactor
    public void testWindowAggWithGroupByAndJoin() {
        final String sql = """
                select min(d.deptno), rank() over (order by e.empno),
                 max(e.empno) over (partition by e.deptno)
                from emp e, dept d
                where e.deptno = d.deptno
                group by d.deptno, e.empno, e.deptno
                """;
        sql( sql ).ok();
    }


    /**
     * Test case for "variant involving HAVING clause".
     */
    @Test
    @Disabled // refactor
    public void testWindowAggWithGroupByAndHaving() {
        final String sql = """
                select min(deptno), rank() over (order by empno),
                max(empno) over (partition by deptno)
                from emp group by deptno, empno
                having empno < 10 and min(deptno) < 20
                """;
        sql( sql ).ok();
    }


    /**
     * Test case for "variant involving join with sub-query that contains window function and GROUP BY".
     */
    @Test
    @Disabled // refactor
    public void testWindowAggInSubQueryJoin() {
        final String sql = """
                select T.x, T.y, T.z, emp.empno
                from (select min(deptno) as x,
                   rank() over (order by empno) as y,
                   max(empno) over (partition by deptno) as z
                   from emp group by deptno, empno) as T
                 inner join emp on T.x = emp.deptno
                 and T.y = emp.empno
                """;
        sql( sql ).ok();
    }


    /**
     * Test case for "Validator should derive type of expression in ORDER BY".
     */
    @Test
    @Disabled // refactor
    public void testOrderByOver() {
        String sql = "select deptno, rank() over(partition by empno order by deptno)\n"
                + "from emp order by row_number() over(partition by empno order by deptno)";
        sql( sql ).ok();
    }


    /**
     * Test case (correlated scalar aggregate sub-query) for "When de-correlating, push join condition into sub-query".
     */
    @Test
    @Disabled // refactor
    public void testCorrelationScalarAggAndFilter() {
        final String sql = """
                SELECT e1.empno
                FROM emp e1, dept d1 where e1.deptno = d1.deptno
                and e1.deptno < 10 and d1.deptno < 15
                and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)""";
        sql( sql ).decorrelate( true ).expand( true ).ok();
    }


    /**
     * Test case for "Correlated scalar sub-query with multiple aggregates gives AssertionError".
     */
    @Test
    @Disabled // refactor
    public void testCorrelationMultiScalarAggregate() {
        final String sql = """
                select sum(e1.empno)
                from emp e1, dept d1
                where e1.deptno = d1.deptno
                and e1.sal > (select avg(e2.sal) from emp e2
                  where e2.deptno = d1.deptno)""";
        sql( sql ).decorrelate( true ).expand( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCorrelationScalarAggAndFilterRex() {
        final String sql = """
                SELECT e1.empno
                FROM emp e1, dept d1 where e1.deptno = d1.deptno
                and e1.deptno < 10 and d1.deptno < 15
                and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)""";
        sql( sql ).decorrelate( true ).expand( false ).ok();
    }


    /**
     * Test case (correlated EXISTS sub-query) for "When de-correlating, push join condition into sub-query".
     */
    @Test
    @Disabled // refactor
    public void testCorrelationExistsAndFilter() {
        final String sql = """
                SELECT e1.empno
                FROM emp e1, dept d1 where e1.deptno = d1.deptno
                and e1.deptno < 10 and d1.deptno < 15
                and exists (select * from emp e2 where e1.empno = e2.empno)""";
        sql( sql ).decorrelate( true ).expand( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCorrelationExistsAndFilterRex() {
        final String sql = """
                SELECT e1.empno
                FROM emp e1, dept d1 where e1.deptno = d1.deptno
                and e1.deptno < 10 and d1.deptno < 15
                and exists (select * from emp e2 where e1.empno = e2.empno)""";
        sql( sql ).decorrelate( true ).ok();
    }


    /**
     * A theta join condition, unlike the equi-join condition in {@link #testCorrelationExistsAndFilterRex()}, requires a value generator.
     */
    @Test
    @Disabled // refactor
    public void testCorrelationExistsAndFilterThetaRex() {
        final String sql = """
                SELECT e1.empno
                FROM emp e1, dept d1 where e1.deptno = d1.deptno
                and e1.deptno < 10 and d1.deptno < 15
                and exists (select * from emp e2 where e1.empno < e2.empno)""";
        sql( sql ).decorrelate( true ).ok();
    }


    /**
     * Test case (correlated NOT EXISTS sub-query) for "When de-correlating, push join condition into sub-query".
     */
    @Test
    @Disabled // refactor
    public void testCorrelationNotExistsAndFilter() {
        final String sql = """
                SELECT e1.empno
                FROM emp e1, dept d1 where e1.deptno = d1.deptno
                and e1.deptno < 10 and d1.deptno < 15
                and not exists (select * from emp e2 where e1.empno = e2.empno)""";
        sql( sql ).decorrelate( true ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCustomColumnResolving() {
        final String sql = "select k0 from struct.t";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCustomColumnResolving2() {
        final String sql = "select c2 from struct.t";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCustomColumnResolving3() {
        final String sql = "select f1.c2 from struct.t";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCustomColumnResolving4() {
        final String sql = "select c1 from struct.t order by f0.c1";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCustomColumnResolving5() {
        final String sql = "select count(c1) from struct.t group by f0.c1";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCustomColumnResolvingWithSelectStar() {
        final String sql = "select * from struct.t";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testCustomColumnResolvingWithSelectFieldNameDotStar() {
        final String sql = "select f1.* from struct.t";
        sql( sql ).ok();
    }


    /**
     * Test case for "Dynamic Table / Dynamic Star support".
     */
    @Test
    @Disabled // refactor
    public void testSelectFromDynamicTable() throws Exception {
        final String sql = "select n_nationkey, n_name from SALES.NATION";
        sql( sql ).ok();
    }


    /**
     * Test case for Dynamic Table / Dynamic Star support.
     */
    @Test
    @Disabled // refactor
    public void testSelectStarFromDynamicTable() throws Exception {
        final String sql = "select * from SALES.NATION";
        sql( sql ).ok();
    }


    /**
     * Test case for "Query with NOT IN operator and literal fails throws AssertionError: 'Cast for just nullability not allowed'".
     */
    @Test
    @Disabled // refactor
    public void testNotInWithLiteral() {
        final String sql = """
                SELECT *
                FROM SALES.NATION
                WHERE n_name NOT IN
                    (SELECT ''
                     FROM SALES.NATION)""";
        sql( sql ).ok();
    }


    /**
     * Test case for Dynamic Table / Dynamic Star support.
     */
    @Test
    @Disabled // refactor
    public void testReferDynamicStarInSelectOB() throws Exception {
        final String sql = """
                select n_nationkey, n_name
                from (select * from SALES.NATION)
                order by n_regionkey""";
        sql( sql ).ok();
    }


    /**
     * Test case for Dynamic Table / Dynamic Star support.
     */
    @Test
    @Disabled // refactor
    public void testDynamicStarInTableJoin() throws Exception {
        final String sql = "select * from "
                + " (select * from SALES.NATION) T1, "
                + " (SELECT * from SALES.CUSTOMER) T2 "
                + " where T1.n_nationkey = T2.c_nationkey";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testDynamicNestedColumn() {
        final String sql = """
                select t3.fake_q1['fake_col2'] as fake2
                from (
                  select t2.fake_col as fake_q1
                  from SALES.CUSTOMER as t2) as t3""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testDynamicSchemaUnnest() {
        final String sql3 = """
                select t1.c_nationkey, t3.fake_col3
                from SALES.CUSTOMER as t1,
                lateral (select t2."$unnest" as fake_col3
                         from unnest(t1.fake_col) as t2) as t3""";
        sql( sql3 ).ok();
    }


    @Test
    @Disabled // refactor
    public void testStarDynamicSchemaUnnest() {
        final String sql3 = """
                select *
                from SALES.CUSTOMER as t1,
                lateral (select t2."$unnest" as fake_col3
                         from unnest(t1.fake_col) as t2) as t3""";
        sql( sql3 ).ok();
    }


    @Test
    @Disabled // refactor
    public void testStarDynamicSchemaUnnest2() {
        final String sql3 = """
                select *
                from SALES.CUSTOMER as t1,
                unnest(t1.fake_col) as t2""";
        sql( sql3 ).ok();
    }


    @Test
    @Disabled // refactor
    public void testStarDynamicSchemaUnnestNestedSubQuery() {
        String sql3 = """
                select t2.c1
                from (select * from SALES.CUSTOMER) as t1,
                unnest(t1.fake_col) as t2(c1)""";
        sql( sql3 ).ok();
    }


    /**
     * Test case for Dynamic Table / Dynamic Star support.
     */
    @Test
    @Disabled // refactor
    public void testReferDynamicStarInSelectWhereGB() throws Exception {
        final String sql = "select n_regionkey, count(*) as cnt from (select * from SALES.NATION) where n_nationkey > 5 group by n_regionkey";
        sql( sql ).ok();
    }


    /**
     * Test case for Dynamic Table / Dynamic Star support.
     */
    @Test
    @Disabled // refactor
    public void testDynamicStarInJoinAndSubQ() throws Exception {
        final String sql = "select * from (select * from SALES.NATION T1, SALES.CUSTOMER T2 where T1.n_nationkey = T2.c_nationkey)";
        sql( sql ).ok();
    }


    /**
     * Test case for Dynamic Table / Dynamic Star support.
     */
    @Test
    @Disabled // refactor
    public void testStarJoinStaticDynTable() throws Exception {
        final String sql = "select * from SALES.NATION N, SALES.REGION as R where N.n_regionkey = R.r_regionkey";
        sql( sql ).ok();
    }


    /**
     * Test case for Dynamic Table / Dynamic Star support.
     */
    @Test
    @Disabled // refactor
    public void testGrpByColFromStarInSubQuery() throws Exception {
        final String sql = "SELECT n.n_nationkey AS col from (SELECT * FROM SALES.NATION) as n group by n.n_nationkey";
        sql( sql ).ok();
    }


    /**
     * Test case for Dynamic Table / Dynamic Star support.
     */
    @Test
    @Disabled // refactor
    public void testDynStarInExistSubQ() throws Exception {
        final String sql = "select *\n"
                + "from SALES.REGION where exists (select * from SALES.NATION)";
        sql( sql ).ok();
    }


    /**
     * Test case for "Create the a new DynamicRecordType, avoiding star expansion when working with this type".
     */
    @Test
    @Disabled // refactor
    public void testSelectDynamicStarOrderBy() throws Exception {
        final String sql = "SELECT * from SALES.NATION order by n_nationkey";
        sql( sql ).ok();
    }


    /**
     * Test case for "Configurable IN list size when converting IN clause to join".
     */
    @Test
    @Disabled // refactor
    public void testInToSemiJoin() {
        final String sql = """
                SELECT empno
                FROM emp AS e
                WHERE cast(e.empno as bigint) in (130, 131, 132, 133, 134)""";
        // No conversion to join since less than IN-list size threshold 10
        SqlToAlgConverter.Config noConvertConfig = NodeToAlgConverter.configBuilder().inSubQueryThreshold( 10 ).build();
        sql( sql ).withConfig( noConvertConfig ).convertsTo( "${planNotConverted}" );
        // Conversion to join since greater than IN-list size threshold 2
        SqlToAlgConverter.Config convertConfig = NodeToAlgConverter.configBuilder().inSubQueryThreshold( 2 ).build();
        sql( sql ).withConfig( convertConfig ).convertsTo( "${planConverted}" );
    }


    /**
     * Test case for "Window function applied to sub-query with dynamic star gets wrong plan".
     */
    @Test
    @Disabled // refactor
    public void testWindowOnDynamicStar() throws Exception {
        final String sql = """
                SELECT SUM(n_nationkey) OVER w
                FROM (SELECT * FROM SALES.NATION) subQry
                WINDOW w AS (PARTITION BY REGION ORDER BY n_nationkey)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWindowAndGroupByWithDynamicStar() {
        final String sql = """
                SELECT
                n_regionkey,
                MAX(MIN(n_nationkey)) OVER (PARTITION BY n_regionkey)
                FROM (SELECT * FROM SALES.NATION)
                GROUP BY n_regionkey""";

        sql( sql ).conformance( new SqlDelegatingConformance( ConformanceEnum.DEFAULT ) {
            @Override
            public boolean isGroupByAlias() {
                return true;
            }
        } ).ok();
    }


    /**
     * Test case for "Add support for ANY_VALUE aggregate function".
     */
    @Test
    @Disabled // refactor
    public void testAnyValueAggregateFunctionNoGroupBy() throws Exception {
        final String sql = "SELECT any_value(empno) as anyempno FROM emp AS e";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testAnyValueAggregateFunctionGroupBy() throws Exception {
        final String sql = "SELECT any_value(empno) as anyempno FROM emp AS e group by e.sal";
        sql( sql ).ok();
    }



    /* // TODO MV: FIX
    @Test @Disabled // refactor
    public void testLarge() {
        // Size factor used to be 400, but lambdas use a lot of stack
        final int x = 300;
        SqlValidatorTest.checkLarge( x, input -> {
            final AlgRoot root = tester.convertSqlToRel( input );
            final String s = RelOptUtil.toString( root.project() );
            assertThat( s, notNullValue() );
        } );
    }
*/


    @Test
    @Disabled // refactor
    public void testUnionInFrom() {
        final String sql = """
                select x0, x1 from (
                  select 'a' as x0, 'a' as x1, 'a' as x2 from emp
                  union all
                  select 'bb' as x0, 'bb' as x1, 'bb' as x2 from dept)""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testMatchRecognize1() {
        final String sql = """
                select *
                  from emp match_recognize
                  (
                    partition by job, sal
                    order by job asc, sal desc, empno
                    pattern (strt down+ up+)
                    define
                      down as down.mgr < PREV(down.mgr),
                      up as up.mgr > prev(up.mgr)) as mr""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testMatchRecognizeMeasures1() {
        final String sql = """
                select *
                from emp match_recognize (
                  partition by job, sal
                  order by job asc, sal desc
                  measures MATCH_NUMBER() as match_num,
                    CLASSIFIER() as var_match,
                    STRT.mgr as start_nw,
                    LAST(DOWN.mgr) as bottom_nw,
                    LAST(up.mgr) as end_nw
                  pattern (strt down+ up+)
                  define
                    down as down.mgr < PREV(down.mgr),
                    up as up.mgr > prev(up.mgr)) as mr""";
        sql( sql ).ok();
    }


    /**
     * Test case for "Output rowType of Match should include PARTITION BY and ORDER BY columns".
     */
    @Test
    @Disabled // refactor
    public void testMatchRecognizeMeasures2() {
        final String sql = """
                select *
                from emp match_recognize (
                  partition by job
                  order by sal
                  measures MATCH_NUMBER() as match_num,
                    CLASSIFIER() as var_match,
                    STRT.mgr as start_nw,
                    LAST(DOWN.mgr) as bottom_nw,
                    LAST(up.mgr) as end_nw
                  pattern (strt down+ up+)
                  define
                    down as down.mgr < PREV(down.mgr),
                    up as up.mgr > prev(up.mgr)) as mr""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testMatchRecognizeMeasures3() {
        final String sql = """
                select *
                from emp match_recognize (
                  partition by job
                  order by sal
                  measures MATCH_NUMBER() as match_num,
                    CLASSIFIER() as var_match,
                    STRT.mgr as start_nw,
                    LAST(DOWN.mgr) as bottom_nw,
                    LAST(up.mgr) as end_nw
                  ALL ROWS PER MATCH
                  pattern (strt down+ up+)
                  define
                    down as down.mgr < PREV(down.mgr),
                    up as up.mgr > prev(up.mgr)) as mr""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testMatchRecognizePatternSkip1() {
        final String sql = """
                select *
                  from emp match_recognize
                  (
                    after match skip to next row
                    pattern (strt down+ up+)
                    define
                      down as down.mgr < PREV(down.mgr),
                      up as up.mgr > NEXT(up.mgr)
                  ) mr""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testMatchRecognizeSubset1() {
        final String sql = """
                select *
                  from emp match_recognize
                  (
                    after match skip to down
                    pattern (strt down+ up+)
                    subset stdn = (strt, down)
                    define
                      down as down.mgr < PREV(down.mgr),
                      up as up.mgr > NEXT(up.mgr)
                  ) mr""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testMatchRecognizePrevLast() {
        final String sql = """
                SELECT *
                FROM emp
                MATCH_RECOGNIZE (
                  MEASURES
                    STRT.mgr AS start_mgr,
                    LAST(DOWN.mgr) AS bottom_mgr,
                    LAST(UP.mgr) AS end_mgr
                  ONE ROW PER MATCH
                  PATTERN (STRT DOWN+ UP+)
                  DEFINE
                    DOWN AS DOWN.mgr < PREV(DOWN.mgr),
                    UP AS UP.mgr > PREV(LAST(DOWN.mgr, 1), 1)
                ) AS T""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testMatchRecognizePrevDown() {
        final String sql = """
                SELECT *
                FROM emp
                MATCH_RECOGNIZE (
                  MEASURES
                    STRT.mgr AS start_mgr,
                    LAST(DOWN.mgr) AS up_days,
                    LAST(UP.mgr) AS total_days
                  PATTERN (STRT DOWN+ UP+)
                  DEFINE
                    DOWN AS DOWN.mgr < PREV(DOWN.mgr),
                    UP AS UP.mgr > PREV(DOWN.mgr)
                ) AS T""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testPrevClassifier() {
        final String sql = """
                SELECT *
                FROM emp
                MATCH_RECOGNIZE (
                  MEASURES
                    STRT.mgr AS start_mgr,
                    LAST(DOWN.mgr) AS up_days,
                    LAST(UP.mgr) AS total_days
                  PATTERN (STRT DOWN? UP+)
                  DEFINE
                    DOWN AS DOWN.mgr < PREV(DOWN.mgr),
                    UP AS CASE
                            WHEN PREV(CLASSIFIER()) = 'STRT'
                              THEN UP.mgr > 15
                            ELSE
                              UP.mgr > 20
                            END
                ) AS T""";
        sql( sql ).ok();
    }


    /**
     * Test case for "Validator should allow alternative nullCollations for ORDER BY in OVER".
     */
    @Test
    @Disabled // refactor
    public void testUserDefinedOrderByOver() {
        String sql = """
                select deptno,
                  rank() over(partition by empno order by deptno)
                from emp
                order by row_number() over(partition by empno order by deptno)""";
        Properties properties = new Properties();
        properties.setProperty( PolyphenyDbConnectionProperty.DEFAULT_NULL_COLLATION.camelName(), NullCollation.LOW.name() );
        PolyphenyDbConnectionConfigImpl connectionConfig = new PolyphenyDbConnectionConfigImpl( properties );
        TesterImpl tester = new TesterImpl( getDiffRepos(), false, false, true, false, null, SqlToAlgConverter.Config.DEFAULT, ConformanceEnum.DEFAULT, Contexts.of( connectionConfig ) );
        sql( sql ).with( tester ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJsonExists() {
        final String sql = "select json_exists(ename, 'lax $')\n" + "from emp";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJsonValue() {
        final String sql = "select json_value(ename, 'lax $')\n" + "from emp";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJsonQuery() {
        final String sql = "select json_query(ename, 'lax $')\n" + "from emp";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJsonArray() {
        final String sql = "select json_array(ename, ename)\n" + "from emp";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJsonArrayAgg() {
        final String sql = "select json_arrayagg(ename)\n" + "from emp";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJsonObject() {
        final String sql = "select json_object(ename: deptno, ename: deptno)\n" + "from emp";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJsonObjectAgg() {
        final String sql = "select json_objectagg(ename: deptno)\n" + "from emp";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testJsonPredicate() {
        final String sql = """
                select
                ename is json,
                ename is json value,
                ename is json object,
                ename is json array,
                ename is json scalar,
                ename is not json,
                ename is not json value,
                ename is not json object,
                ename is not json array,
                ename is not json scalar
                from emp""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWithinGroup1() {
        final String sql = """
                select deptno,
                 collect(empno) within group (order by deptno, hiredate desc)
                from emp
                group by deptno""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWithinGroup2() {
        final String sql = """
                select dept.deptno,
                 collect(sal) within group (order by sal desc) as s,
                 collect(sal) within group (order by 1)as s1,
                 collect(sal) within group (order by sal)
                  filter (where sal > 2000) as s2
                from emp
                join dept using (deptno)
                group by dept.deptno""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testWithinGroup3() {
        final String sql = """
                select deptno,
                 collect(empno) within group (order by empno not in (1, 2)), count(*)
                from emp
                group by deptno""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderByRemoval1() {
        final String sql = """
                select * from (
                  select empno from emp order by deptno offset 0) t
                order by empno desc""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderByRemoval2() {
        final String sql = """
                select * from (
                  select empno from emp order by deptno offset 1) t
                order by empno desc""";
        sql( sql ).ok();
    }


    @Test
    @Disabled // refactor
    public void testOrderByRemoval3() {
        final String sql = """
                select * from (
                  select empno from emp order by deptno limit 10) t
                order by empno""";
        sql( sql ).ok();
    }


    /**
     * Visitor that checks that every {@link AlgNode} in a tree is valid.
     *
     * @see AlgNode#isValid(Litmus, AlgNode.Context)
     */
    public static class RelValidityChecker extends AlgVisitor implements AlgNode.Context {

        int invalidCount;
        final Deque<AlgNode> stack = new ArrayDeque<>();


        @Override
        public Set<CorrelationId> correlationIds() {
            final ImmutableSet.Builder<CorrelationId> builder = ImmutableSet.builder();
            for ( AlgNode r : stack ) {
                builder.addAll( r.getVariablesSet() );
            }
            return builder.build();
        }


        @Override
        public void visit( AlgNode node, int ordinal, AlgNode parent ) {
            try {
                stack.push( node );
                if ( !node.isValid( Litmus.THROW, this ) ) {
                    ++invalidCount;
                }
                super.visit( node, ordinal, parent );
            } finally {
                stack.pop();
            }
        }

    }


    /**
     * Allows fluent testing.
     */
    public static class Sql {

        private final String sql;
        private final boolean expand;
        private final boolean decorrelate;
        private final Tester tester;
        private final boolean trim;
        private final SqlToAlgConverter.Config config;
        private final Conformance conformance;


        Sql( String sql, boolean expand, boolean decorrelate, Tester tester, boolean trim, SqlToAlgConverter.Config config, Conformance conformance ) {
            this.sql = sql;
            this.expand = expand;
            this.decorrelate = decorrelate;
            this.tester = tester;
            this.trim = trim;
            this.config = config;
            this.conformance = conformance;
        }


        public void ok() {
            convertsTo( "${plan}" );
        }


        public void convertsTo( String plan ) {
            tester.withExpand( expand )
                    .withDecorrelation( decorrelate )
                    .withConformance( conformance )
                    .withConfig( config )
                    .assertConvertsTo( sql, plan, trim );
        }


        public Sql withConfig( SqlToAlgConverter.Config config ) {
            return new Sql( sql, expand, decorrelate, tester, trim, config, conformance );
        }


        public Sql expand( boolean expand ) {
            return new Sql( sql, expand, decorrelate, tester, trim, config, conformance );
        }


        public Sql decorrelate( boolean decorrelate ) {
            return new Sql( sql, expand, decorrelate, tester, trim, config, conformance );
        }


        public Sql with( Tester tester ) {
            return new Sql( sql, expand, decorrelate, tester, trim, config, conformance );
        }


        public Sql trim( boolean trim ) {
            return new Sql( sql, expand, decorrelate, tester, trim, config, conformance );
        }


        public Sql conformance( Conformance conformance ) {
            return new Sql( sql, expand, decorrelate, tester, trim, config, conformance );
        }

    }

}

