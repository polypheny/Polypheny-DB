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

package org.polypheny.db.sql;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.polypheny.db.algebra.AlgDecorrelator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.mutable.MutableAlg;
import org.polypheny.db.algebra.mutable.MutableAlgs;
import org.polypheny.db.algebra.rules.FilterJoinRule;
import org.polypheny.db.algebra.rules.FilterProjectTransposeRule;
import org.polypheny.db.algebra.rules.FilterToCalcRule;
import org.polypheny.db.algebra.rules.ProjectMergeRule;
import org.polypheny.db.algebra.rules.ProjectToWindowRules;
import org.polypheny.db.algebra.rules.SemiJoinRules;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.hep.HepPlanner;
import org.polypheny.db.plan.hep.HepProgram;
import org.polypheny.db.plan.hep.HepProgramBuilder;
import org.polypheny.db.sql.language.SqlToAlgTestBase;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.Litmus;


/**
 * Tests for {@link MutableAlg} sub-classes.
 */
public class MutableRelTest {

    @Test
    public void testConvertAggregate() {
        checkConvertMutableAlg(
                "Aggregate",
                "select empno, sum(sal) from emp group by empno" );
    }


    @Test
    public void testConvertFilter() {
        checkConvertMutableAlg(
                "Filter",
                "select * from emp where ename = 'DUMMY'" );
    }


    @Test
    public void testConvertProject() {
        checkConvertMutableAlg(
                "Project",
                "select ename from emp" );
    }


    @Test
    public void testConvertSort() {
        checkConvertMutableAlg(
                "Sort",
                "select * from emp order by ename" );
    }


    @Test
    public void testConvertCalc() {
        checkConvertMutableAlg(
                "Calc",
                "select * from emp where ename = 'DUMMY'",
                false,
                ImmutableList.of( FilterToCalcRule.INSTANCE ) );
    }


    @Test
    public void testConvertWindow() {
        checkConvertMutableAlg(
                "Window",
                "select sal, avg(sal) over (partition by deptno) from emp",
                false,
                ImmutableList.of( ProjectToWindowRules.PROJECT ) );
    }


    @Test
    public void testConvertCollect() {
        checkConvertMutableAlg(
                "Collect",
                "select multiset(select deptno from dept) from (values(true))" );
    }


    @Test
    public void testConvertUncollect() {
        checkConvertMutableAlg(
                "Uncollect",
                "select * from unnest(multiset[1,2])" );
    }


    @Test
    public void testConvertTableModify() {
        checkConvertMutableAlg(
                "Modify",
                "insert into dept select empno, ename from emp" );
    }


    @Test
    public void testConvertSample() {
        checkConvertMutableAlg(
                "Sample",
                "select * from emp tablesample system(50) where empno > 5" );
    }


    @Test
    public void testConvertTableFunctionScan() {
        checkConvertMutableAlg(
                "TableFunctionScan",
                "select * from table(ramp(3))" );
    }


    @Test
    public void testConvertValues() {
        checkConvertMutableAlg(
                "Values",
                "select * from (values (1, 2))" );
    }


    @Test
    public void testConvertJoin() {
        checkConvertMutableAlg(
                "Join",
                "select * from emp join dept using (deptno)" );
    }


    @Test
    public void testConvertSemiJoin() {
        final String sql = "select * from dept where exists (\n"
                + "  select * from emp\n"
                + "  where emp.deptno = dept.deptno\n"
                + "  and emp.sal > 100)";
        checkConvertMutableAlg(
                "SemiJoin",
                sql,
                true,
                ImmutableList.of( FilterProjectTransposeRule.INSTANCE, FilterJoinRule.FILTER_ON_JOIN, ProjectMergeRule.INSTANCE, SemiJoinRules.PROJECT ) );
    }


    @Test
    public void testConvertCorrelate() {
        final String sql = "select * from dept where exists (\n"
                + "  select * from emp\n"
                + "  where emp.deptno = dept.deptno\n"
                + "  and emp.sal > 100)";
        checkConvertMutableAlg( "Correlate", sql );
    }


    @Test
    public void testConvertUnion() {
        checkConvertMutableAlg(
                "Union",
                "select * from emp where deptno = 10 union select * from emp where ename like 'John%'" );
    }


    @Test
    public void testConvertMinus() {
        checkConvertMutableAlg(
                "Minus",
                "select * from emp where deptno = 10 except select * from emp where ename like 'John%'" );
    }


    @Test
    public void testConvertIntersect() {
        checkConvertMutableAlg(
                "Intersect",
                "select * from emp where deptno = 10 intersect select * from emp where ename like 'John%'" );
    }


    /**
     * Verifies that after conversion to and from a MutableRel, the new {@link AlgNode} remains identical to the original AlgNode.
     */
    private static void checkConvertMutableAlg( String alg, String sql ) {
        checkConvertMutableAlg( alg, sql, false, null );
    }


    /**
     * Verifies that after conversion to and from a MutableRel, the new {@link AlgNode} remains identical to the original AlgNode.
     */
    private static void checkConvertMutableAlg( String alg, String sql, boolean decorrelate, List<AlgOptRule> rules ) {
        final SqlToAlgTestBase test = new SqlToAlgTestBase() {
        };
        AlgNode origRel = test.createTester().convertSqlToRel( sql ).alg;
        if ( decorrelate ) {
            final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( origRel.getCluster(), null );
            origRel = AlgDecorrelator.decorrelateQuery( origRel, algBuilder );
        }
        if ( rules != null ) {
            final HepProgram hepProgram = new HepProgramBuilder().addRuleCollection( rules ).build();
            final HepPlanner hepPlanner = new HepPlanner( hepProgram );
            hepPlanner.setRoot( origRel );
            origRel = hepPlanner.findBestExp();
        }
        // Convert to and from a mutable alg.
        final MutableAlg mutableRel = MutableAlgs.toMutable( origRel );
        final AlgNode newRel = MutableAlgs.fromMutable( mutableRel );

        // Check if the mutable alg digest contains the target alg.
        final String mutableRelStr = mutableRel.deep();
        final String msg1 = "Mutable rel: " + mutableRelStr + " does not contain target rel: " + alg;
        Assert.assertTrue( msg1, mutableRelStr.contains( alg ) );

        // Check if the mutable rel's row-type is identical to the original rel's row-type.
        final AlgDataType origRelType = origRel.getRowType();
        final AlgDataType mutableRelType = mutableRel.rowType;
        final String msg2 = "Mutable rel's row type does not match with the original alg.\n"
                + "Original alg type: " + origRelType + ";\nMutable alg type: " + mutableRelType;
        Assert.assertTrue(
                msg2,
                AlgOptUtil.equal( "origRelType", origRelType, "mutableRelType", mutableRelType, Litmus.IGNORE ) );

        // Check if the new alg converted from the mutable alg is identical to the original alg.
        final String origRelStr = AlgOptUtil.toString( origRel );
        final String newRelStr = AlgOptUtil.toString( newRel );
        final String msg3 = "The converted new alg is different from the original alg.\n"
                + "Original rel: " + origRelStr + ";\nNew rel: " + newRelStr;
        Assert.assertEquals( msg3, origRelStr, newRelStr );
    }

}

