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

package ch.unibas.dmi.dbis.polyphenydb.test.enumerable;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRules;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.ReflectiveSchema;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionProperty;
import ch.unibas.dmi.dbis.polyphenydb.config.Lex;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.JoinToCorrelateRule;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.test.PolyphenyDbAssert;
import ch.unibas.dmi.dbis.polyphenydb.test.PolyphenyDbAssert.AssertThat;
import ch.unibas.dmi.dbis.polyphenydb.test.JdbcTest;
import java.util.function.Consumer;
import org.junit.Test;


/**
 * Unit test for {@link ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableCorrelate}.
 */
public class EnumerableCorrelateTest {

    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-2605">[POLYPHENYDB-2605] NullPointerException when left outer join implemented with EnumerableCorrelate</a>
     */
    @Test
    public void leftOuterJoinCorrelate() {
        tester( false, new JdbcTest.HrSchema() )
                .query( "select e.empid, e.name, d.name as dept from emps e left outer join depts d on e.deptno=d.deptno" )
                .withHook( Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
                    // force the left outer join to run via EnumerableCorrelate instead of EnumerableJoin
                    planner.addRule( JoinToCorrelateRule.INSTANCE );
                    planner.removeRule( EnumerableRules.ENUMERABLE_JOIN_RULE );
                } )
                .explainContains( ""
                        + "EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], name=[$t2], dept=[$t4])\n"
                        + "  EnumerableCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{1}])\n"
                        + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
                        + "      EnumerableTableScan(table=[[s, emps]])\n"
                        + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[$cor0], expr#5=[$t4.deptno], expr#6=[=($t5, $t0)], proj#0..1=[{exprs}], $condition=[$t6])\n"
                        + "      EnumerableTableScan(table=[[s, depts]])" )
                .returnsUnordered(
                        "empid=100; name=Bill; dept=Sales",
                        "empid=110; name=Theodore; dept=Sales",
                        "empid=150; name=Sebastian; dept=Sales",
                        "empid=200; name=Eric; dept=null" );
    }


    @Test
    public void simpleCorrelateDecorrelated() {
        tester( true, new JdbcTest.HrSchema() )
                .query( "select empid, name from emps e where exists (select 1 from depts d where d.deptno=e.deptno)" )
                .explainContains( ""
                        + "EnumerableCalc(expr#0..2=[{inputs}], empid=[$t0], name=[$t2])\n"
                        + "  EnumerableSemiJoin(condition=[=($1, $3)], joinType=[inner])\n"
                        + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
                        + "      EnumerableTableScan(table=[[s, emps]])\n"
                        + "    EnumerableTableScan(table=[[s, depts]])" )
                .returnsUnordered(
                        "empid=100; name=Bill",
                        "empid=110; name=Theodore",
                        "empid=150; name=Sebastian" );
    }


    @Test
    public void simpleCorrelate() {
        tester( false, new JdbcTest.HrSchema() )
                .query( "select empid, name from emps e where exists (select 1 from depts d where d.deptno=e.deptno)" )
                .explainContains( ""
                        + "EnumerableCalc(expr#0..3=[{inputs}], empid=[$t0], name=[$t2])\n"
                        + "  EnumerableCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
                        + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
                        + "      EnumerableTableScan(table=[[s, emps]])\n"
                        + "    EnumerableAggregate(group=[{0}])\n"
                        + "      EnumerableCalc(expr#0..3=[{inputs}], expr#4=[true], expr#5=[$cor0], expr#6=[$t5.deptno], expr#7=[=($t0, $t6)], i=[$t4], $condition=[$t7])\n"
                        + "        EnumerableTableScan(table=[[s, depts]])" )
                .returnsUnordered(
                        "empid=100; name=Bill",
                        "empid=110; name=Theodore",
                        "empid=150; name=Sebastian" );
    }


    @Test
    public void simpleCorrelateWithConditionIncludingBoxedPrimitive() {
        final String sql = "select empid from emps e where not exists (\n" + "  select 1 from depts d where d.deptno=e.commission)";
        tester( false, new JdbcTest.HrSchema() )
                .query( sql )
                .returnsUnordered(
                        "empid=100",
                        "empid=110",
                        "empid=150",
                        "empid=200" );
    }


    private AssertThat tester( boolean forceDecorrelate, Object schema ) {
        return PolyphenyDbAssert.that()
                .with( PolyphenyDbConnectionProperty.LEX, Lex.JAVA )
                .with( PolyphenyDbConnectionProperty.FORCE_DECORRELATE, forceDecorrelate )
                .withSchema( "s", new ReflectiveSchema( schema ) );
    }
}

