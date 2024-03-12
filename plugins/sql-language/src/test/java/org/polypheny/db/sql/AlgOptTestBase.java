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

package org.polypheny.db.sql;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgDecorrelator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.metadata.AlgMetadataProvider;
import org.polypheny.db.algebra.metadata.ChainedAlgMetadataProvider;
import org.polypheny.db.algebra.metadata.DefaultAlgMetadataProvider;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.hep.HepPlanner;
import org.polypheny.db.plan.hep.HepProgram;
import org.polypheny.db.plan.hep.HepProgramBuilder;
import org.polypheny.db.sql.language.SqlToAlgTestBase;
import org.polypheny.db.tools.AlgBuilder;


/**
 * RelOptTestBase is an abstract base for tests which exercise a planner and/or rules via {@link DiffRepository}.
 */
abstract class AlgOptTestBase extends SqlToAlgTestBase {

    @Override
    public Tester createTester() {
        return super.createTester().withDecorrelation( false );
    }


    /**
     * Checks the plan for a SQL statement before/after executing a given rule.
     *
     * @param rule Planner rule
     * @param sql SQL query
     */
    protected void checkPlanning( AlgOptRule rule, String sql ) {
        HepProgramBuilder programBuilder = HepProgram.builder();
        programBuilder.addRuleInstance( rule );

        checkPlanning( programBuilder.build(), sql );
    }


    /**
     * Checks the plan for a SQL statement before/after executing a given program.
     *
     * @param program Planner program
     * @param sql SQL query
     */
    protected void checkPlanning( HepProgram program, String sql ) {
        checkPlanning( new HepPlanner( program ), sql );
    }


    /**
     * Checks the plan for a SQL statement before/after executing a given planner.
     *
     * @param planner Planner
     * @param sql SQL query
     */
    protected void checkPlanning( AlgPlanner planner, String sql ) {
        checkPlanning( tester, null, planner, sql );
    }


    /**
     * Checks the plan for a SQL statement before/after executing a given rule, with a pre-program to prepare the tree.
     *
     * @param tester Tester
     * @param preProgram Program to execute before comparing before state
     * @param planner Planner
     * @param sql SQL query
     */
    protected void checkPlanning( Tester tester, HepProgram preProgram, AlgPlanner planner, String sql ) {
        checkPlanning( tester, preProgram, planner, sql, false );
    }


    /**
     * Checks the plan for a SQL statement before/after executing a given rule, with a pre-program to prepare the tree.
     *
     * @param tester Tester
     * @param preProgram Program to execute before comparing before state
     * @param planner Planner
     * @param sql SQL query
     * @param unchanged Whether the rule is to have no effect
     */
    protected void checkPlanning( Tester tester, HepProgram preProgram, AlgPlanner planner, String sql, boolean unchanged ) {
        final DiffRepository diffRepos = getDiffRepos();
        String sql2 = diffRepos.expand( "sql", sql );
        final AlgRoot root = tester.convertSqlToAlg( sql2 );
        final AlgNode algInitial = root.alg;

        assertNotNull( algInitial );

        List<AlgMetadataProvider> list = new ArrayList<>();
        list.add( DefaultAlgMetadataProvider.INSTANCE );
        planner.registerMetadataProviders( list );
        AlgMetadataProvider plannerChain = ChainedAlgMetadataProvider.of( list );
        final AlgCluster cluster = algInitial.getCluster();
        cluster.setMetadataProvider( plannerChain );

        AlgNode algBefore;
        if ( preProgram == null ) {
            algBefore = algInitial;
        } else {
            HepPlanner prePlanner = new HepPlanner( preProgram );
            prePlanner.setRoot( algInitial );
            algBefore = prePlanner.findBestExp();
        }

        assertThat( algBefore, notNullValue() );

        final String planBefore = NL + AlgOptUtil.toString( algBefore );
        diffRepos.assertEquals( "planBefore", "${planBefore}", planBefore );
        SqlToAlgTestBase.assertValid( algBefore );

        planner.setRoot( algBefore );
        AlgNode r = planner.findBestExp();
        if ( tester.isLateDecorrelate() ) {
            final String planMid = NL + AlgOptUtil.toString( r );
            diffRepos.assertEquals( "planMid", "${planMid}", planMid );
            SqlToAlgTestBase.assertValid( r );
            final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( cluster, null );
            r = AlgDecorrelator.decorrelateQuery( r, algBuilder );
        }
        final String planAfter = NL + AlgOptUtil.toString( r );
        if ( unchanged ) {
            assertThat( planAfter, is( planBefore ) );
        } else {
            diffRepos.assertEquals( "planAfter", "${planAfter}", planAfter );
            if ( planBefore.equals( planAfter ) ) {
                throw new AssertionError( "Expected plan before and after is the same.\n"
                        + "You must use unchanged=true or call checkUnchanged" );
            }
        }
        SqlToAlgTestBase.assertValid( r );
    }

}

