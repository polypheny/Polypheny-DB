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

package org.polypheny.db.sql.volcano;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.rules.ProjectRemoveRule;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.sql.SqlLanguageDependent;
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
public class VolcanoPlannerTest extends SqlLanguageDependent {

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

        AlgCluster cluster = PlannerTests.newCluster( planner );
        NoneLeafAlg leafRel = new NoneLeafAlg( cluster, "a" );
        AlgNode convertedRel = planner.changeTraits( leafRel, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertInstanceOf( PhysLeafAlg.class, result );
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

        AlgCluster cluster = PlannerTests.newCluster( planner );
        NoneLeafAlg leafRel = new NoneLeafAlg( cluster, "a" );
        NoneSingleAlg singleRel = new NoneSingleAlg( cluster, leafRel );
        AlgNode convertedRel = planner.changeTraits( singleRel, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertInstanceOf( PhysSingleAlg.class, result );
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

        AlgCluster cluster = PlannerTests.newCluster( planner );
        NoneLeafAlg leafRel = new NoneLeafAlg( cluster, "a" );
        NoneSingleAlg singleRel = new NoneSingleAlg( cluster, leafRel );
        AlgNode convertedRel = planner.changeTraits( singleRel, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertInstanceOf( PhysSingleAlg.class, result );
        assertThat( sort( buf ), equalTo( sort( "NoneSingleAlg:Subset#0.NONE", "PhysSingleAlg:Subset#0.NONE", "PhysSingleAlg:Subset#0.PHYS" ) ) );
    }


    private static <E extends Comparable<E>> List<E> sort( List<E> list ) {
        final List<E> list2 = new ArrayList<>( list );
        Collections.sort( list2 );
        return list2;
    }


    @SafeVarargs
    private static <E extends Comparable<E>> List<E> sort( E... es ) {
        return sort( Arrays.asList( es ) );
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

        AlgCluster cluster = PlannerTests.newCluster( planner );
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
        assertInstanceOf( PhysToIteratorConverter.class, result );
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
     * This always worked because it uses a completely-physical pattern (requiring GoodSingleRule to fire first).
     */
    @Test
    public void testRemoveSingleGood() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.ambitious = true;
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        planner.addRule( new PhysLeafRule() );
        planner.addRule( new GoodSingleRule() );
        planner.addRule( new GoodRemoveSingleRule() );

        AlgCluster cluster = PlannerTests.newCluster( planner );
        NoneLeafAlg leafRel = new NoneLeafAlg( cluster, "a" );
        NoneSingleAlg singleRel = new NoneSingleAlg( cluster, leafRel );
        AlgNode convertedRel = planner.changeTraits( singleRel, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertInstanceOf( PhysLeafAlg.class, result );
        PhysLeafAlg resultLeaf = (PhysLeafAlg) result;
        assertEquals( "c", resultLeaf.label );
    }

    /**
     * Converter from PHYS to ENUMERABLE convention.
     */
    static class PhysToIteratorConverter extends ConverterImpl {

        PhysToIteratorConverter( AlgCluster cluster, AlgNode child ) {
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

    /**
     * Planner rule that converts a {@link LogicalRelProject} to PHYS convention.
     */
    private static class PhysProjectRule extends AlgOptRule {

        PhysProjectRule() {
            super( operand( LogicalRelProject.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return PlannerTests.PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final LogicalRelProject project = call.alg( 0 );
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



}

