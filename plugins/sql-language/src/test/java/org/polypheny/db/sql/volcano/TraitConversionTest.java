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


import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.sql.volcano.PlannerTests.TestLeafAlg;
import org.polypheny.db.sql.volcano.PlannerTests.TestSingleAlg;


/**
 * Unit test for {@link AlgDistributionTraitDef}.
 */
public class TraitConversionTest {

    private static final ConvertRelDistributionTraitDef NEW_TRAIT_DEF_INSTANCE = new ConvertRelDistributionTraitDef();
    private static final SimpleDistribution SIMPLE_DISTRIBUTION_ANY = new SimpleDistribution( "ANY" );
    private static final SimpleDistribution SIMPLE_DISTRIBUTION_RANDOM = new SimpleDistribution( "RANDOM" );
    private static final SimpleDistribution SIMPLE_DISTRIBUTION_SINGLETON = new SimpleDistribution( "SINGLETON" );


    @Test
    public void testTraitConversion() {
        final VolcanoPlanner planner = new VolcanoPlanner();
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( NEW_TRAIT_DEF_INSTANCE );

        planner.addRule( new RandomSingleTraitRule() );
        planner.addRule( new SingleLeafTraitRule() );
        planner.addRule( ExpandConversionRule.INSTANCE );

        final AlgCluster cluster = PlannerTests.newCluster( planner );
        final NoneLeafRel leafRel = new NoneLeafRel( cluster, "a" );
        final NoneSingleRel singleRel = new NoneSingleRel( cluster, leafRel );
        final AlgNode convertedRel = planner.changeTraits( singleRel, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ) );
        planner.setRoot( convertedRel );
        final AlgNode result = planner.chooseDelegate().findBestExp();

        assertInstanceOf( RandomSingleRel.class, result );
        assertTrue( result.getTraitSet().contains( PlannerTests.PHYS_CALLING_CONVENTION ) );
        assertTrue( result.getTraitSet().contains( SIMPLE_DISTRIBUTION_RANDOM ) );

        final AlgNode input = result.getInput( 0 );
        assertInstanceOf( BridgeRel.class, input );
        assertTrue( input.getTraitSet().contains( PlannerTests.PHYS_CALLING_CONVENTION ) );
        assertTrue( input.getTraitSet().contains( SIMPLE_DISTRIBUTION_RANDOM ) );

        final AlgNode input2 = input.getInput( 0 );
        assertInstanceOf( SingletonLeafRel.class, input2 );
        assertTrue( input2.getTraitSet().contains( PlannerTests.PHYS_CALLING_CONVENTION ) );
        assertTrue( input2.getTraitSet().contains( SIMPLE_DISTRIBUTION_SINGLETON ) );
    }


    /**
     * Converts a {@link NoneSingleRel} (none convention, distribution any) to {@link RandomSingleRel} (physical convention, distribution random).
     */
    private static class RandomSingleTraitRule extends AlgOptRule {

        RandomSingleTraitRule() {
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
                            .plus( SIMPLE_DISTRIBUTION_RANDOM ) );
            call.transformTo( new RandomSingleRel( single.getCluster(), physInput ) );
        }

    }


    /**
     * Rel with physical convention and random distribution.
     */
    private static class RandomSingleRel extends TestSingleAlg {

        RandomSingleRel( AlgCluster cluster, AlgNode input ) {
            super( cluster, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ).plus( SIMPLE_DISTRIBUTION_RANDOM ), input );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new RandomSingleRel( getCluster(), sole( inputs ) );
        }

    }


    /**
     * Converts {@link NoneLeafRel} (none convention, any distribution) to {@link SingletonLeafRel} (physical convention, singleton distribution).
     */
    private static class SingleLeafTraitRule extends AlgOptRule {

        SingleLeafTraitRule() {
            super( operand( NoneLeafRel.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return PlannerTests.PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            NoneLeafRel leafRel = call.alg( 0 );
            call.transformTo( new SingletonLeafRel( leafRel.getCluster(), leafRel.label ) );
        }

    }


    /**
     * Rel with singleton distribution, physical convention.
     */
    private static class SingletonLeafRel extends TestLeafAlg {

        SingletonLeafRel( AlgCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ).plus( SIMPLE_DISTRIBUTION_SINGLETON ), label );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new SingletonLeafRel( getCluster(), label );
        }

    }


    /**
     * Bridges the {@link SimpleDistribution}, difference between {@link SingletonLeafRel} and {@link RandomSingleRel}.
     */
    private static class BridgeRel extends TestSingleAlg {

        BridgeRel( AlgCluster cluster, AlgNode input ) {
            super( cluster, cluster.traitSetOf( PlannerTests.PHYS_CALLING_CONVENTION ).plus( SIMPLE_DISTRIBUTION_RANDOM ), input );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new BridgeRel( getCluster(), sole( inputs ) );
        }

    }


    /**
     * Dummy distribution for test (simplified version of RelDistribution).
     */
    private static class SimpleDistribution implements AlgTrait<ConvertRelDistributionTraitDef> {

        private final String name;


        SimpleDistribution( String name ) {
            this.name = name;
        }


        @Override
        public String toString() {
            return name;
        }


        @Override
        public ConvertRelDistributionTraitDef getTraitDef() {
            return NEW_TRAIT_DEF_INSTANCE;
        }


        @Override
        public boolean satisfies( AlgTrait<?> trait ) {
            return trait == this || trait == SIMPLE_DISTRIBUTION_ANY;
        }


        @Override
        public void register( AlgPlanner planner ) {
        }

    }


    /**
     * Dummy distribution trait def for test (handles conversion of SimpleDistribution)
     */
    private static class ConvertRelDistributionTraitDef extends AlgTraitDef<SimpleDistribution> {

        @Override
        public Class<SimpleDistribution> getTraitClass() {
            return SimpleDistribution.class;
        }


        @Override
        public String toString() {
            return getSimpleName();
        }


        @Override
        public String getSimpleName() {
            return "ConvertRelDistributionTraitDef";
        }


        @Override
        public AlgNode convert( AlgPlanner planner, AlgNode alg, SimpleDistribution toTrait, boolean allowInfiniteCostConverters ) {
            if ( toTrait == SIMPLE_DISTRIBUTION_ANY ) {
                return alg;
            }

            return new BridgeRel( alg.getCluster(), alg );
        }


        @Override
        public boolean canConvert( AlgPlanner planner, SimpleDistribution fromTrait, SimpleDistribution toTrait ) {
            return (fromTrait == toTrait)
                    || (toTrait == SIMPLE_DISTRIBUTION_ANY)
                    || (fromTrait == SIMPLE_DISTRIBUTION_SINGLETON
                    && toTrait == SIMPLE_DISTRIBUTION_RANDOM);

        }


        @Override
        public SimpleDistribution getDefault() {
            return SIMPLE_DISTRIBUTION_ANY;
        }

    }


    /**
     * Any distribution and none convention.
     */
    private static class NoneLeafRel extends TestLeafAlg {

        NoneLeafRel( AlgCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( Convention.NONE ), label );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( Convention.NONE, SIMPLE_DISTRIBUTION_ANY );
            assert inputs.isEmpty();
            return this;
        }

    }


    /**
     * Rel with any distribution and none convention.
     */
    private static class NoneSingleRel extends TestSingleAlg {

        NoneSingleRel( AlgCluster cluster, AlgNode input ) {
            super( cluster, cluster.traitSetOf( Convention.NONE ), input );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( Convention.NONE, SIMPLE_DISTRIBUTION_ANY );
            return new NoneSingleRel( getCluster(), sole( inputs ) );
        }

    }

}

