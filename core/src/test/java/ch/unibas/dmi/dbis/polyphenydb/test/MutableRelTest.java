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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgram;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.mutable.MutableRel;
import ch.unibas.dmi.dbis.polyphenydb.rel.mutable.MutableRels;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterJoinRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterProjectTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterToCalcRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectMergeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectToWindowRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.SemiJoinRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.RelDecorrelator;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests for {@link MutableRel} sub-classes.
 */
public class MutableRelTest {

    @Test
    public void testConvertAggregate() {
        checkConvertMutableRel(
                "Aggregate",
                "select empno, sum(sal) from emp group by empno" );
    }


    @Test
    public void testConvertFilter() {
        checkConvertMutableRel(
                "Filter",
                "select * from emp where ename = 'DUMMY'" );
    }


    @Test
    public void testConvertProject() {
        checkConvertMutableRel(
                "Project",
                "select ename from emp" );
    }


    @Test
    public void testConvertSort() {
        checkConvertMutableRel(
                "Sort",
                "select * from emp order by ename" );
    }


    @Test
    public void testConvertCalc() {
        checkConvertMutableRel(
                "Calc",
                "select * from emp where ename = 'DUMMY'",
                false,
                ImmutableList.of( FilterToCalcRule.INSTANCE ) );
    }


    @Test
    public void testConvertWindow() {
        checkConvertMutableRel(
                "Window",
                "select sal, avg(sal) over (partition by deptno) from emp",
                false,
                ImmutableList.of( ProjectToWindowRule.PROJECT ) );
    }


    @Test
    public void testConvertCollect() {
        checkConvertMutableRel(
                "Collect",
                "select multiset(select deptno from dept) from (values(true))" );
    }


    @Test
    public void testConvertUncollect() {
        checkConvertMutableRel(
                "Uncollect",
                "select * from unnest(multiset[1,2])" );
    }


    @Test
    public void testConvertTableModify() {
        checkConvertMutableRel(
                "TableModify",
                "insert into dept select empno, ename from emp" );
    }


    @Test
    public void testConvertSample() {
        checkConvertMutableRel(
                "Sample",
                "select * from emp tablesample system(50) where empno > 5" );
    }


    @Test
    public void testConvertTableFunctionScan() {
        checkConvertMutableRel(
                "TableFunctionScan",
                "select * from table(ramp(3))" );
    }


    @Test
    public void testConvertValues() {
        checkConvertMutableRel(
                "Values",
                "select * from (values (1, 2))" );
    }


    @Test
    public void testConvertJoin() {
        checkConvertMutableRel(
                "Join",
                "select * from emp join dept using (deptno)" );
    }


    @Test
    public void testConvertSemiJoin() {
        final String sql = "select * from dept where exists (\n"
                + "  select * from emp\n"
                + "  where emp.deptno = dept.deptno\n"
                + "  and emp.sal > 100)";
        checkConvertMutableRel(
                "SemiJoin",
                sql,
                true,
                ImmutableList.of( FilterProjectTransposeRule.INSTANCE, FilterJoinRule.FILTER_ON_JOIN, ProjectMergeRule.INSTANCE, SemiJoinRule.PROJECT ) );
    }


    @Test
    public void testConvertCorrelate() {
        final String sql = "select * from dept where exists (\n"
                + "  select * from emp\n"
                + "  where emp.deptno = dept.deptno\n"
                + "  and emp.sal > 100)";
        checkConvertMutableRel( "Correlate", sql );
    }


    @Test
    public void testConvertUnion() {
        checkConvertMutableRel(
                "Union",
                "select * from emp where deptno = 10 union select * from emp where ename like 'John%'" );
    }


    @Test
    public void testConvertMinus() {
        checkConvertMutableRel(
                "Minus",
                "select * from emp where deptno = 10 except select * from emp where ename like 'John%'" );
    }


    @Test
    public void testConvertIntersect() {
        checkConvertMutableRel(
                "Intersect",
                "select * from emp where deptno = 10 intersect select * from emp where ename like 'John%'" );
    }


    /**
     * Verifies that after conversion to and from a MutableRel, the new RelNode remains identical to the original RelNode.
     */
    private static void checkConvertMutableRel( String rel, String sql ) {
        checkConvertMutableRel( rel, sql, false, null );
    }


    /**
     * Verifies that after conversion to and from a MutableRel, the new RelNode remains identical to the original RelNode.
     */
    private static void checkConvertMutableRel( String rel, String sql, boolean decorrelate, List<RelOptRule> rules ) {
        final SqlToRelTestBase test = new SqlToRelTestBase() {
        };
        RelNode origRel = test.createTester().convertSqlToRel( sql ).rel;
        if ( decorrelate ) {
            final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create( origRel.getCluster(), null );
            origRel = RelDecorrelator.decorrelateQuery( origRel, relBuilder );
        }
        if ( rules != null ) {
            final HepProgram hepProgram = new HepProgramBuilder().addRuleCollection( rules ).build();
            final HepPlanner hepPlanner = new HepPlanner( hepProgram );
            hepPlanner.setRoot( origRel );
            origRel = hepPlanner.findBestExp();
        }
        // Convert to and from a mutable rel.
        final MutableRel mutableRel = MutableRels.toMutable( origRel );
        final RelNode newRel = MutableRels.fromMutable( mutableRel );

        // Check if the mutable rel digest contains the target rel.
        final String mutableRelStr = mutableRel.deep();
        final String msg1 = "Mutable rel: " + mutableRelStr + " does not contain target rel: " + rel;
        Assert.assertTrue( msg1, mutableRelStr.contains( rel ) );

        // Check if the mutable rel's row-type is identical to the original rel's row-type.
        final RelDataType origRelType = origRel.getRowType();
        final RelDataType mutableRelType = mutableRel.rowType;
        final String msg2 = "Mutable rel's row type does not match with the original rel.\n"
                + "Original rel type: " + origRelType + ";\nMutable rel type: " + mutableRelType;
        Assert.assertTrue(
                msg2,
                RelOptUtil.equal( "origRelType", origRelType, "mutableRelType", mutableRelType, Litmus.IGNORE ) );

        // Check if the new rel converted from the mutable rel is identical to the original rel.
        final String origRelStr = RelOptUtil.toString( origRel );
        final String newRelStr = RelOptUtil.toString( newRel );
        final String msg3 = "The converted new rel is different from the original rel.\n"
                + "Original rel: " + origRelStr + ";\nNew rel: " + newRelStr;
        Assert.assertEquals( msg3, origRelStr, newRelStr );
    }
}

