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


import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationImpl;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.volcano.PlannerTests.TestLeafAlg;
import org.polypheny.db.sql.volcano.PlannerTests.TestSingleAlg;


/**
 * Unit test for {@link AlgCollationTraitDef}.
 */
public class CollationConversionTest {

    private static final TestRelCollationImpl LEAF_COLLATION = new TestRelCollationImpl( ImmutableList.of( new AlgFieldCollation( 0, Direction.CLUSTERED ) ) );

    private static final TestRelCollationImpl ROOT_COLLATION = new TestRelCollationImpl( ImmutableList.of( new AlgFieldCollation( 0 ) ) );

    private static final TestRelCollationTraitDef COLLATION_TRAIT_DEF = new TestRelCollationTraitDef();


    @Test
    public void testCollationConversion() {
        final VolcanoPlanner planner = new VolcanoPlanner();
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( COLLATION_TRAIT_DEF );

        planner.addRule( new SingleNodeRule() );
        planner.addRule( new LeafTraitRule() );
        planner.addRule( ExpandConversionRule.INSTANCE );

        final AlgOptCluster cluster = PlannerTests.newCluster( planner );
        final NoneLeafRel leafRel = new NoneLeafRel( cluster, "a" );
        final NoneSingleRel singleRel = new NoneSingleRel( cluster, leafRel );
        final AlgNode convertedRel = planner.changeTraits( singleRel, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ).plus( ROOT_COLLATION ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertTrue( result instanceof RootSingleRel );
        assertTrue( result.getTraitSet().contains( ROOT_COLLATION ) );
        assertTrue( result.getTraitSet().contains( PlannerTests.PHYS_CALLING_CONVENTION ) );

        final AlgNode input = result.getInput( 0 );
        assertTrue( input instanceof PhysicalSort );
        assertTrue( result.getTraitSet().contains( ROOT_COLLATION ) );
        assertTrue( input.getTraitSet().contains( PlannerTests.PHYS_CALLING_CONVENTION ) );

        final AlgNode input2 = input.getInput( 0 );
        assertTrue( input2 instanceof LeafRel );
        assertTrue( input2.getTraitSet().contains( LEAF_COLLATION ) );
        assertTrue( input.getTraitSet().contains( PlannerTests.PHYS_CALLING_CONVENTION ) );
    }


    /**
     * Converts a NoneSingleAlg to RootSingleRel.
     */
    private static class SingleNodeRule extends AlgOptRule {

        SingleNodeRule() {
            super( operand( NoneSingleRel.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return PlannerTests.PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            NoneSingleRel single = call.alg( 0 );
            AlgNode input = single.getInput();
            AlgNode physInput = convert(
                    input,
                    single.getTraitSet()
                            .replace( PlannerTests.PHYS_CALLING_CONVENTION )
                            .plus( ROOT_COLLATION ) );
            call.transformTo( new RootSingleRel( single.getCluster(), physInput ) );
        }

    }


    /**
     * Root node with physical convention and ROOT_COLLATION trait.
     */
    private static class RootSingleRel extends TestSingleAlg {

        RootSingleRel( AlgOptCluster cluster, AlgNode input ) {
            super( cluster, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ).plus( ROOT_COLLATION ), input );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new RootSingleRel( getCluster(), sole( inputs ) );
        }

    }


    /**
     * Converts a {@link NoneLeafRel} (with none convention) to {@link LeafRel} (with physical convention).
     */
    private static class LeafTraitRule extends AlgOptRule {

        LeafTraitRule() {
            super( operand( NoneLeafRel.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return PlannerTests.PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            NoneLeafRel leafRel = call.alg( 0 );
            call.transformTo( new LeafRel( leafRel.getCluster(), leafRel.label ) );
        }

    }


    /**
     * Leaf node with physical convention and LEAF_COLLATION trait.
     */
    private static class LeafRel extends TestLeafAlg {

        LeafRel( AlgOptCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ).plus( LEAF_COLLATION ), label );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new LeafRel( getCluster(), label );
        }

    }


    /**
     * Leaf node with none convention and LEAF_COLLATION trait.
     */
    private static class NoneLeafRel extends TestLeafAlg {

        NoneLeafRel( AlgOptCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( Convention.NONE ).plus( LEAF_COLLATION ), label );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( Convention.NONE, LEAF_COLLATION );
            assert inputs.isEmpty();
            return this;
        }

    }


    /**
     * A single-input node with none convention and LEAF_COLLATION trait.
     */
    private static class NoneSingleRel extends TestSingleAlg {

        NoneSingleRel( AlgOptCluster cluster, AlgNode input ) {
            super( cluster, cluster.traitSetOf( Convention.NONE ).plus( LEAF_COLLATION ), input );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( Convention.NONE, LEAF_COLLATION );
            return new NoneSingleRel( getCluster(), sole( inputs ) );
        }

    }


    /**
     * Dummy collation trait implementation for the test.
     */
    private static class TestRelCollationImpl extends AlgCollationImpl {

        TestRelCollationImpl( ImmutableList<AlgFieldCollation> fieldCollations ) {
            super( fieldCollations );
        }


        @Override
        public AlgTraitDef getTraitDef() {
            return COLLATION_TRAIT_DEF;
        }

    }


    /**
     * Dummy collation trait def implementation for the test (uses {@link PhysicalSort} below).
     */
    private static class TestRelCollationTraitDef extends AlgTraitDef<AlgCollation> {

        @Override
        public Class<AlgCollation> getTraitClass() {
            return AlgCollation.class;
        }


        @Override
        public String getSimpleName() {
            return "testsort";
        }


        @Override
        public boolean multiple() {
            return true;
        }


        @Override
        public AlgCollation getDefault() {
            return LEAF_COLLATION;
        }


        @Override
        public AlgNode convert( AlgOptPlanner planner, AlgNode alg, AlgCollation toCollation, boolean allowInfiniteCostConverters ) {
            if ( toCollation.getFieldCollations().isEmpty() ) {
                // An empty sort doesn't make sense.
                return null;
            }

            return new PhysicalSort( alg.getCluster(), alg.getTraitSet().replace( toCollation ), alg, toCollation, null, null );
        }


        @Override
        public boolean canConvert( AlgOptPlanner planner, AlgCollation fromTrait, AlgCollation toTrait ) {
            return true;
        }

    }


    /**
     * Physical sort node (not logical).
     */
    private static class PhysicalSort extends Sort {

        PhysicalSort( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, AlgCollation collation, RexNode offset, RexNode fetch ) {
            super( cluster, traits, input, collation, offset, fetch );
        }


        @Override
        public Sort copy( AlgTraitSet traitSet, AlgNode newInput, AlgCollation newCollation, RexNode offset, RexNode fetch ) {
            return new PhysicalSort( getCluster(), traitSet, newInput, newCollation, offset, fetch );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }

    }

}

