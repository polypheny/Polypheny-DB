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

package org.polypheny.db.sql.volcano;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.polypheny.db.test.Matchers.isLinux;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableRules;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.rules.ProjectRemoveRule;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptListener;
import org.polypheny.db.plan.AlgOptListener.AlgChosenEvent;
import org.polypheny.db.plan.AlgOptListener.AlgEquivalenceEvent;
import org.polypheny.db.plan.AlgOptListener.AlgEvent;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.sql.volcano.PlannerTests.GoodSingleRule;
import org.polypheny.db.sql.volcano.PlannerTests.NoneLeafAlg;
import org.polypheny.db.sql.volcano.PlannerTests.NoneSingleAlg;
import org.polypheny.db.sql.volcano.PlannerTests.PhysLeafAlg;
import org.polypheny.db.sql.volcano.PlannerTests.PhysLeafRule;
import org.polypheny.db.sql.volcano.PlannerTests.PhysSingleAlg;
import org.polypheny.db.sql.volcano.PlannerTests.TestSingleAlg;
import org.polypheny.db.tools.AlgBuilder;


/**
 * Unit test for {@link VolcanoPlanner the optimizer}.
 */
public class VolcanoPlannerTest {

    public VolcanoPlannerTest() {
    }


    /**
     * Tests transformation of a leaf from NONE to PHYS.
     */
    @Test
    public void testTransformLeaf() {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        planner.addRule( new PhysLeafRule() );

        AlgOptCluster cluster = PlannerTests.newCluster( planner );
        NoneLeafAlg leafRel = new NoneLeafAlg( cluster, "a" );
        AlgNode convertedRel = planner.changeTraits( leafRel, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertTrue( result instanceof PhysLeafAlg );
    }


    /**
     * Tests transformation of a single+leaf from NONE to PHYS.
     */
    @Test
    public void testTransformSingleGood() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        planner.addRule( new PhysLeafRule() );
        planner.addRule( new GoodSingleRule() );

        AlgOptCluster cluster = PlannerTests.newCluster( planner );
        NoneLeafAlg leafRel = new NoneLeafAlg( cluster, "a" );
        NoneSingleAlg singleRel = new NoneSingleAlg( cluster, leafRel );
        AlgNode convertedRel = planner.changeTraits( singleRel, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertTrue( result instanceof PhysSingleAlg );
    }


    /**
     * Tests a rule that is fired once per subset (whereas most rules are fired once per alg in a set or alg in a subset)
     */
    @Test
    public void testSubsetRule() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        planner.addRule( new PhysLeafRule() );
        planner.addRule( new GoodSingleRule() );
        final List<String> buf = new ArrayList<>();
        planner.addRule( new SubsetRule( buf ) );

        AlgOptCluster cluster = PlannerTests.newCluster( planner );
        NoneLeafAlg leafRel = new NoneLeafAlg( cluster, "a" );
        NoneSingleAlg singleRel = new NoneSingleAlg( cluster, leafRel );
        AlgNode convertedRel = planner.changeTraits( singleRel, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertTrue( result instanceof PhysSingleAlg );
        assertThat( sort( buf ), equalTo( sort( "NoneSingleAlg:Subset#0.NONE", "PhysSingleAlg:Subset#0.NONE", "PhysSingleAlg:Subset#0.PHYS" ) ) );
    }


    private static <E extends Comparable> List<E> sort( List<E> list ) {
        final List<E> list2 = new ArrayList<>( list );
        Collections.sort( list2 );
        return list2;
    }


    private static <E extends Comparable> List<E> sort( E... es ) {
        return sort( Arrays.asList( es ) );
    }


    /**
     * Tests transformation of a single+leaf from NONE to PHYS. In the past, this one didn't work due to the definition of ReformedSingleRule.
     */
    @Ignore // broken, because ReformedSingleRule matches child traits strictly
    @Test
    public void testTransformSingleReformed() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        planner.addRule( new PhysLeafRule() );
        planner.addRule( new ReformedSingleRule() );

        AlgOptCluster cluster = PlannerTests.newCluster( planner );
        NoneLeafAlg leafRel = new NoneLeafAlg( cluster, "a" );
        NoneSingleAlg singleRel = new NoneSingleAlg( cluster, leafRel );
        AlgNode convertedRel = planner.changeTraits( singleRel, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertTrue( result instanceof PhysSingleAlg );
    }


    private void removeTrivialProject( boolean useRule ) {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.ambitious = true;

        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        if ( useRule ) {
            planner.addRule( ProjectRemoveRule.INSTANCE );
        }

        planner.addRule( new PhysLeafRule() );
        planner.addRule( new GoodSingleRule() );
        planner.addRule( new PhysProjectRule() );

        planner.addRule( new ConverterRule( AlgNode.class, PlannerTests.PHYS_CALLING_CONVENTION, EnumerableConvention.INSTANCE, "PhysToIteratorRule" ) {
            @Override
            public AlgNode convert( AlgNode alg ) {
                return new PhysToIteratorConverter(
                        alg.getCluster(),
                        alg );
            }
        } );

        AlgOptCluster cluster = PlannerTests.newCluster( planner );
        PhysLeafAlg leafRel = new PhysLeafAlg( cluster, "a" );
        final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( leafRel.getCluster(), null );
        AlgNode projectRel =
                algBuilder.push( leafRel )
                        .project( algBuilder.alias( algBuilder.field( 0 ), "this" ) )
                        .build();
        NoneSingleAlg singleRel =
                new NoneSingleAlg(
                        cluster,
                        projectRel );
        AlgNode convertedRel =
                planner.changeTraits(
                        singleRel,
                        cluster.traitSetOf( EnumerableConvention.INSTANCE ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertTrue( result instanceof PhysToIteratorConverter );
    }


    // NOTE:  this used to fail but now works
    @Test
    public void testWithRemoveTrivialProject() {
        removeTrivialProject( true );
    }


    // NOTE:  this always worked; it's here as contrast to testWithRemoveTrivialProject()
    @Test
    public void testWithoutRemoveTrivialProject() {
        removeTrivialProject( false );
    }


    /**
     * Previously, this didn't work because ReformedRemoveSingleRule uses a pattern which spans calling conventions.
     */
    @Ignore // broken, because ReformedSingleRule matches child traits strictly
    @Test
    public void testRemoveSingleReformed() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.ambitious = true;
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        planner.addRule( new PhysLeafRule() );
        planner.addRule( new ReformedRemoveSingleRule() );

        AlgOptCluster cluster = PlannerTests.newCluster( planner );
        NoneLeafAlg leafRel = new NoneLeafAlg( cluster, "a" );
        NoneSingleAlg singleRel = new NoneSingleAlg( cluster, leafRel );
        AlgNode convertedRel = planner.changeTraits( singleRel, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertTrue( result instanceof PhysLeafAlg );
        PhysLeafAlg resultLeaf = (PhysLeafAlg) result;
        assertEquals( "c", resultLeaf.label );
    }


    /**
     * This always worked (in contrast to testRemoveSingleReformed) because it uses a completely-physical pattern (requiring GoodSingleRule to fire first).
     */
    @Test
    public void testRemoveSingleGood() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.ambitious = true;
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        planner.addRule( new PhysLeafRule() );
        planner.addRule( new GoodSingleRule() );
        planner.addRule( new GoodRemoveSingleRule() );

        AlgOptCluster cluster = PlannerTests.newCluster( planner );
        NoneLeafAlg leafRel = new NoneLeafAlg( cluster, "a" );
        NoneSingleAlg singleRel = new NoneSingleAlg( cluster, leafRel );
        AlgNode convertedRel = planner.changeTraits( singleRel, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertTrue( result instanceof PhysLeafAlg );
        PhysLeafAlg resultLeaf = (PhysLeafAlg) result;
        assertEquals( "c", resultLeaf.label );
    }


    @Ignore("POLYPHENYDB-2592 EnumerableMergeJoin is never taken")
    @Test
    public void testMergeJoin() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        // Below two lines are important for the planner to use collation trait and generate merge join
        planner.addAlgTraitDef( AlgCollationTraitDef.INSTANCE );
        planner.registerAbstractRelationalRules();

        planner.addRule( EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE );
        planner.addRule( EnumerableRules.ENUMERABLE_VALUES_RULE );
        planner.addRule( EnumerableRules.ENUMERABLE_SORT_RULE );

        AlgOptCluster cluster = PlannerTests.newCluster( planner );

        AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( cluster, null );
        AlgNode logicalPlan = algBuilder
                .values( new String[]{ "id", "name" }, "2", "a", "1", "b" )
                .values( new String[]{ "id", "name" }, "1", "x", "2", "y" )
                .join( JoinAlgType.INNER, "id" )
                .build();

        AlgTraitSet desiredTraits = cluster.traitSet().replace( EnumerableConvention.INSTANCE );
        final AlgNode newRoot = planner.changeTraits( logicalPlan, desiredTraits );
        planner.setRoot( newRoot );

        AlgNode bestExp = planner.findBestExp();

        final String plan = ""
                + "EnumerableMergeJoin(condition=[=($0, $2)], joinType=[inner])\n"
                + "  EnumerableSort(sort0=[$0], dir0=[ASC])\n"
                + "    EnumerableValues(tuples=[[{ '2', 'a' }, { '1', 'b' }]])\n"
                + "  EnumerableValues(tuples=[[{ '1', 'x' }, { '2', 'y' }]])\n";
        assertThat( "Merge join + sort is expected", plan, isLinux( AlgOptUtil.toString( bestExp ) ) );
    }


    /**
     * Tests whether planner correctly notifies listeners of events.
     */
    @Ignore
    @Test
    public void testListener() {
        TestListener listener = new TestListener();

        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addListener( listener );

        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        planner.addRule( new PhysLeafRule() );

        AlgOptCluster cluster = PlannerTests.newCluster( planner );
        NoneLeafAlg leafRel = new NoneLeafAlg( cluster, "a" );
        AlgNode convertedRel = planner.changeTraits( leafRel, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertTrue( result instanceof PhysLeafAlg );

        List<AlgEvent> eventList = listener.getEventList();

        // add node
        checkEvent( eventList, 0, AlgEquivalenceEvent.class, leafRel, null );

        // internal subset
        checkEvent( eventList, 1, AlgEquivalenceEvent.class, null, null );

        // before rule
        checkEvent( eventList, 2, AlgOptListener.RuleAttemptedEvent.class, leafRel, PhysLeafRule.class );

        // before rule
        checkEvent( eventList, 3, AlgOptListener.RuleProductionEvent.class, result, PhysLeafRule.class );

        // result of rule
        checkEvent( eventList, 4, AlgEquivalenceEvent.class, result, null );

        // after rule
        checkEvent( eventList, 5, AlgOptListener.RuleProductionEvent.class, result, PhysLeafRule.class );

        // after rule
        checkEvent( eventList, 6, AlgOptListener.RuleAttemptedEvent.class, leafRel, PhysLeafRule.class );

        // choose plan
        checkEvent( eventList, 7, AlgChosenEvent.class, result, null );

        // finish choosing plan
        checkEvent( eventList, 8, AlgChosenEvent.class, null, null );
    }


    private void checkEvent( List<AlgEvent> eventList, int iEvent, Class expectedEventClass, AlgNode expectedRel, Class<? extends AlgOptRule> expectedRuleClass ) {
        assertTrue( iEvent < eventList.size() );
        AlgEvent event = eventList.get( iEvent );
        assertSame( expectedEventClass, event.getClass() );
        if ( expectedRel != null ) {
            assertSame( expectedRel, event.getRel() );
        }
        if ( expectedRuleClass != null ) {
            AlgOptListener.RuleEvent ruleEvent = (AlgOptListener.RuleEvent) event;
            assertSame( expectedRuleClass, ruleEvent.getRuleCall().getRule().getClass() );
        }
    }

    //~ Inner Classes ----------------------------------------------------------


    /**
     * Converter from PHYS to ENUMERABLE convention.
     */
    static class PhysToIteratorConverter extends ConverterImpl {

        PhysToIteratorConverter( AlgOptCluster cluster, AlgNode child ) {
            super( cluster, ConventionTraitDef.INSTANCE, cluster.traitSetOf( EnumerableConvention.INSTANCE ), child );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( EnumerableConvention.INSTANCE );
            return new PhysToIteratorConverter( getCluster(), AbstractAlgNode.sole( inputs ) );
        }

    }


    /**
     * Rule that matches a {@link AlgSubset}.
     */
    private static class SubsetRule extends AlgOptRule {

        private final List<String> buf;


        SubsetRule( List<String> buf ) {
            super( operand( TestSingleAlg.class, operand( AlgSubset.class, any() ) ) );
            this.buf = buf;
        }


        @Override
        public Convention getOutConvention() {
            return PlannerTests.PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            // Do not transform to anything; just log the calls.
            TestSingleAlg singleRel = call.alg( 0 );
            AlgSubset childRel = call.alg( 1 );
            assertThat( call.algs.length, equalTo( 2 ) );
            buf.add( singleRel.getClass().getSimpleName() + ":" + childRel.getDigest() );
        }

    }

    // NOTE: Previously, ReformedSingleRule didn't work because it explicitly specifies PhysLeafAlg rather than {@link AlgNode} for the single input. Since the PhysLeafAlg is in a different subset from the original NoneLeafAlg,
    // ReformedSingleRule never saw it.  (GoodSingleRule saw the NoneLeafAlg instead and fires off of that; later the NoneLeafAlg gets converted into a PhysLeafAlg).  Now Volcano supports rules which match across subsets.


    /**
     * Planner rule that matches a {@link NoneSingleAlg} whose input is
     * a {@link PhysLeafAlg} in a different subset.
     */
    private static class ReformedSingleRule extends AlgOptRule {

        ReformedSingleRule() {
            super( operand( NoneSingleAlg.class, operand( PhysLeafAlg.class, any() ) ) );
        }


        @Override
        public Convention getOutConvention() {
            return PlannerTests.PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            NoneSingleAlg singleRel = call.alg( 0 );
            AlgNode childRel = call.alg( 1 );
            AlgNode physInput = convert( childRel, singleRel.getTraitSet().replace( PlannerTests.PHYS_CALLING_CONVENTION ) );
            call.transformTo( new PhysSingleAlg( singleRel.getCluster(), physInput ) );
        }

    }


    /**
     * Planner rule that converts a {@link LogicalProject} to PHYS convention.
     */
    private static class PhysProjectRule extends AlgOptRule {

        PhysProjectRule() {
            super( operand( LogicalProject.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return PlannerTests.PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final LogicalProject project = call.alg( 0 );
            AlgNode childRel = project.getInput();
            call.transformTo( new PhysLeafAlg( childRel.getCluster(), "b" ) );
        }

    }


    /**
     * Planner rule that successfully removes a {@link PhysSingleAlg}.
     */
    private static class GoodRemoveSingleRule extends AlgOptRule {

        GoodRemoveSingleRule() {
            super( operand( PhysSingleAlg.class, operand( PhysLeafAlg.class, any() ) ) );
        }


        @Override
        public Convention getOutConvention() {
            return PlannerTests.PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            PhysSingleAlg singleRel = call.alg( 0 );
            PhysLeafAlg leafRel = call.alg( 1 );
            call.transformTo( new PhysLeafAlg( singleRel.getCluster(), "c" ) );
        }

    }


    /**
     * Planner rule that removes a {@link NoneSingleAlg}.
     */
    private static class ReformedRemoveSingleRule extends AlgOptRule {

        ReformedRemoveSingleRule() {
            super( operand( NoneSingleAlg.class, operand( PhysLeafAlg.class, any() ) ) );
        }


        @Override
        public Convention getOutConvention() {
            return PlannerTests.PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            NoneSingleAlg singleRel = call.alg( 0 );
            PhysLeafAlg leafRel = call.alg( 1 );
            call.transformTo( new PhysLeafAlg( singleRel.getCluster(), "c" ) );
        }

    }


    /**
     * Implementation of {@link AlgOptListener}.
     */
    private static class TestListener implements AlgOptListener {

        private List<AlgEvent> eventList;


        TestListener() {
            eventList = new ArrayList<>();
        }


        List<AlgEvent> getEventList() {
            return eventList;
        }


        private void recordEvent( AlgEvent event ) {
            eventList.add( event );
        }


        @Override
        public void algChosen( AlgChosenEvent event ) {
            recordEvent( event );
        }


        @Override
        public void algDiscarded( AlgDiscardedEvent event ) {
            // Volcano is quite a pack rat--it never discards anything!
            throw new AssertionError( event );
        }


        @Override
        public void algEquivalenceFound( AlgEquivalenceEvent event ) {
            if ( !event.isPhysical() ) {
                return;
            }
            recordEvent( event );
        }


        @Override
        public void ruleAttempted( RuleAttemptedEvent event ) {
            recordEvent( event );
        }


        @Override
        public void ruleProductionSucceeded( RuleProductionEvent event ) {
            recordEvent( event );
        }

    }

}

