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


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.polypheny.db.algebra.AlgDecorrelator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.metadata.AlgMetadataProvider;
import org.polypheny.db.algebra.metadata.ChainedAlgMetadataProvider;
import org.polypheny.db.algebra.metadata.DefaultAlgMetadataProvider;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.hep.HepPlanner;
import org.polypheny.db.plan.hep.HepProgram;
import org.polypheny.db.plan.hep.HepProgramBuilder;
import org.polypheny.db.runtime.FlatLists;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.sql.language.SqlToAlgTestBase;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.Closer;


/**
 * RelOptTestBase is an abstract base for tests which exercise a planner and/or rules via {@link DiffRepository}.
 */
abstract class RelOptTestBase extends SqlToAlgTestBase {

    @Override
    public Tester createTester() {
        return super.createTester().withDecorrelation( false );
    }


    protected Tester createDynamicTester() {
        return getTesterWithDynamicTable();
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
     * Checks the plan for a SQL statement before/after executing a given rule.
     *
     * @param rule Planner rule
     * @param sql SQL query
     */
    protected void checkPlanningDynamic( AlgOptRule rule, String sql ) {
        HepProgramBuilder programBuilder = HepProgram.builder();
        programBuilder.addRuleInstance( rule );
        checkPlanning( createDynamicTester(), null, new HepPlanner( programBuilder.build() ), sql );
    }


    /**
     * Checks the plan for a SQL statement before/after executing a given rule.
     *
     * @param sql SQL query
     */
    protected void checkPlanningDynamic( String sql ) {
        checkPlanning( createDynamicTester(), null, new HepPlanner( HepProgram.builder().build() ), sql );
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
    protected void checkPlanning( AlgOptPlanner planner, String sql ) {
        checkPlanning( tester, null, planner, sql );
    }


    /**
     * Checks that the plan is the same before and after executing a given planner. Useful for checking circumstances where rules should not fire.
     *
     * @param planner Planner
     * @param sql SQL query
     */
    protected void checkPlanUnchanged( AlgOptPlanner planner, String sql ) {
        checkPlanning( tester, null, planner, sql, true );
    }


    /**
     * Checks the plan for a SQL statement before/after executing a given rule, with a pre-program to prepare the tree.
     *
     * @param tester Tester
     * @param preProgram Program to execute before comparing before state
     * @param planner Planner
     * @param sql SQL query
     */
    protected void checkPlanning( Tester tester, HepProgram preProgram, AlgOptPlanner planner, String sql ) {
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
    protected void checkPlanning( Tester tester, HepProgram preProgram, AlgOptPlanner planner, String sql, boolean unchanged ) {
        final DiffRepository diffRepos = getDiffRepos();
        String sql2 = diffRepos.expand( "sql", sql );
        final AlgRoot root = tester.convertSqlToRel( sql2 );
        final AlgNode algInitial = root.alg;

        assertNotNull( algInitial );

        List<AlgMetadataProvider> list = new ArrayList<>();
        list.add( DefaultAlgMetadataProvider.INSTANCE );
        planner.registerMetadataProviders( list );
        AlgMetadataProvider plannerChain = ChainedAlgMetadataProvider.of( list );
        final AlgOptCluster cluster = algInitial.getCluster();
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


    /**
     * Sets the SQL statement for a test.
     */
    Sql sql( String sql ) {
        return new Sql( sql, null, null, ImmutableMap.of(), ImmutableList.of() );
    }


    /**
     * Allows fluent testing.
     */
    class Sql {

        private final String sql;
        private HepProgram preProgram;
        private final HepPlanner hepPlanner;
        private final ImmutableMap<Hook, Consumer<?>> hooks;
        private ImmutableList<Function<Tester, Tester>> transforms;


        Sql( String sql, HepProgram preProgram, HepPlanner hepPlanner, ImmutableMap<Hook, Consumer<?>> hooks, ImmutableList<Function<Tester, Tester>> transforms ) {
            this.sql = sql;
            this.preProgram = preProgram;
            this.hepPlanner = hepPlanner;
            this.hooks = hooks;
            this.transforms = transforms;
        }


        public Sql withPre( HepProgram preProgram ) {
            return new Sql( sql, preProgram, hepPlanner, hooks, transforms );
        }


        public Sql with( HepPlanner hepPlanner ) {
            return new Sql( sql, preProgram, hepPlanner, hooks, transforms );
        }


        public Sql with( HepProgram program ) {
            return new Sql( sql, preProgram, new HepPlanner( program ), hooks, transforms );
        }


        public Sql withRule( AlgOptRule rule ) {
            return with( HepProgram.builder().addRuleInstance( rule ).build() );
        }


        /**
         * Adds a transform that will be applied to {@link #tester} just before running the query.
         */
        private Sql withTransform( Function<Tester, Tester> transform ) {
            return new Sql( sql, preProgram, hepPlanner, hooks, FlatLists.append( transforms, transform ) );
        }


        /**
         * Adds a hook and a handler for that hook. Polypheny-DB will create a thread hook (by calling {@link Hook#addThread(Consumer)}) just before running the query, and remove the hook afterwards.
         */
        public <T> Sql withHook( Hook hook, Consumer<T> handler ) {
            return new Sql( sql, preProgram, hepPlanner, FlatLists.append( hooks, hook, handler ), transforms );
        }


        public <V> Sql withProperty( Hook hook, V value ) {
            return withHook( hook, Hook.propertyJ( value ) );
        }


        public Sql expand( final boolean b ) {
            return withTransform( tester -> tester.withExpand( b ) );
        }


        public Sql withLateDecorrelation( final boolean b ) {
            return withTransform( tester -> tester.withLateDecorrelation( b ) );
        }


        public Sql withDecorrelation( final boolean b ) {
            return withTransform( tester -> tester.withDecorrelation( b ) );
        }


        public Sql withTrim( final boolean b ) {
            return withTransform( tester -> tester.withTrim( b ) );
        }


        public Sql withContext( final Context context ) {
            return withTransform( tester -> tester.withContext( context ) );
        }


        public void check() {
            check( false );
        }


        public void checkUnchanged() {
            check( true );
        }


        @SuppressWarnings("unchecked")
        private void check( boolean unchanged ) {
            try ( Closer closer = new Closer() ) {
                for ( Map.Entry<Hook, Consumer<?>> entry : hooks.entrySet() ) {
                    closer.add( entry.getKey().addThread( entry.getValue() ) );
                }
                Tester t = tester;
                for ( Function<Tester, Tester> transform : transforms ) {
                    t = transform.apply( t );
                }
                checkPlanning( t, preProgram, hepPlanner, sql, unchanged );
            }
        }

    }

}

