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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.List;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.adapter.enumerable.EnumerableAlg;
import org.polypheny.db.adapter.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.util.Pair;


/**
 * Unit test for handling of traits by {@link VolcanoPlanner}.
 */
public class VolcanoPlannerTraitTest {

    /**
     * Private calling convention representing a generic "physical" calling convention.
     */
    private static final Convention PHYS_CALLING_CONVENTION = new Convention.Impl( "PHYS", AlgNode.class );

    /**
     * Private trait definition for an alternate type of traits.
     */
    private static final AltTraitDef ALT_TRAIT_DEF = new AltTraitDef();

    /**
     * Private alternate trait.
     */
    private static final AltTrait ALT_EMPTY_TRAIT = new AltTrait( ALT_TRAIT_DEF, "ALT_EMPTY" );

    /**
     * Private alternate trait.
     */
    private static final AltTrait ALT_TRAIT = new AltTrait( ALT_TRAIT_DEF, "ALT" );

    /**
     * Private alternate trait.
     */
    private static final AltTrait ALT_TRAIT2 = new AltTrait( ALT_TRAIT_DEF, "ALT2" );

    /**
     * Ordinal count for alternate traits (so they can implement equals() and avoid being canonized into the same trait).
     */
    private static int altTraitOrdinal = 0;


    public VolcanoPlannerTraitTest() {
    }


    @Ignore
    @Test
    public void testDoubleConversion() {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( ALT_TRAIT_DEF );

        planner.addRule( new PhysToIteratorConverterRule() );
        planner.addRule( new AltTraitConverterRule( ALT_TRAIT, ALT_TRAIT2, "AltToAlt2ConverterRule" ) );
        planner.addRule( new PhysLeafRule() );
        planner.addRule( new IterSingleRule() );

        AlgOptCluster cluster = PlannerTests.newCluster( planner );

        NoneLeafRel noneLeafRel = AlgOptUtil.addTrait( new NoneLeafRel( cluster, "noneLeafRel" ), ALT_TRAIT );

        NoneSingleAlg noneRel = AlgOptUtil.addTrait( new NoneSingleAlg( cluster, noneLeafRel ), ALT_TRAIT2 );

        AlgNode convertedRel = planner.changeTraits( noneRel, cluster.traitSetOf( EnumerableConvention.INSTANCE ).replace( ALT_TRAIT2 ) );

        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();

        assertTrue( result instanceof IterSingleRel );
        Assert.assertEquals( EnumerableConvention.INSTANCE, result.getTraitSet().getTrait( ConventionTraitDef.INSTANCE ) );
        Assert.assertEquals( ALT_TRAIT2, result.getTraitSet().getTrait( ALT_TRAIT_DEF ) );

        AlgNode child = result.getInputs().get( 0 );
        assertTrue( (child instanceof AltTraitConverter) || (child instanceof PhysToIteratorConverter) );

        child = child.getInputs().get( 0 );
        assertTrue( (child instanceof AltTraitConverter) || (child instanceof PhysToIteratorConverter) );

        child = child.getInputs().get( 0 );
        assertTrue( child instanceof PhysLeafRel );
    }


    @Test
    public void testRuleMatchAfterConversion() {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( ALT_TRAIT_DEF );

        planner.addRule( new PhysToIteratorConverterRule() );
        planner.addRule( new PhysLeafRule() );
        planner.addRule( new IterSingleRule() );
        planner.addRule( new IterSinglePhysMergeRule() );

        AlgOptCluster cluster = PlannerTests.newCluster( planner );

        NoneLeafRel noneLeafRel = AlgOptUtil.addTrait( new NoneLeafRel( cluster, "noneLeafRel" ), ALT_TRAIT );

        NoneSingleAlg noneRel = AlgOptUtil.addTrait( new NoneSingleAlg( cluster, noneLeafRel ), ALT_EMPTY_TRAIT );

        AlgNode convertedRel = planner.changeTraits( noneRel, cluster.traitSetOf( EnumerableConvention.INSTANCE ).replace( ALT_EMPTY_TRAIT ) );

        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();

        assertTrue( result instanceof IterMergedRel );
    }


    @Ignore
    @Test
    public void testTraitPropagation() {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( ALT_TRAIT_DEF );

        planner.addRule( new PhysToIteratorConverterRule() );
        planner.addRule( new AltTraitConverterRule( ALT_TRAIT, ALT_TRAIT2, "AltToAlt2ConverterRule" ) );
        planner.addRule( new PhysLeafRule() );
        planner.addRule( new IterSingleRule2() );

        AlgOptCluster cluster = PlannerTests.newCluster( planner );

        NoneLeafRel noneLeafRel = AlgOptUtil.addTrait( new NoneLeafRel( cluster, "noneLeafRel" ), ALT_TRAIT );

        NoneSingleAlg noneRel = AlgOptUtil.addTrait( new NoneSingleAlg( cluster, noneLeafRel ), ALT_TRAIT2 );

        AlgNode convertedRel = planner.changeTraits( noneRel, cluster.traitSetOf( EnumerableConvention.INSTANCE ).replace( ALT_TRAIT2 ) );

        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();

        assertTrue( result instanceof IterSingleRel );
        Assert.assertEquals( EnumerableConvention.INSTANCE, result.getTraitSet().getTrait( ConventionTraitDef.INSTANCE ) );
        Assert.assertEquals( ALT_TRAIT2, result.getTraitSet().getTrait( ALT_TRAIT_DEF ) );

        AlgNode child = result.getInputs().get( 0 );
        assertTrue( child instanceof IterSingleRel );
        Assert.assertEquals( EnumerableConvention.INSTANCE, child.getTraitSet().getTrait( ConventionTraitDef.INSTANCE ) );
        Assert.assertEquals( ALT_TRAIT2, child.getTraitSet().getTrait( ALT_TRAIT_DEF ) );

        child = child.getInputs().get( 0 );
        assertTrue( (child instanceof AltTraitConverter) || (child instanceof PhysToIteratorConverter) );

        child = child.getInputs().get( 0 );
        assertTrue( (child instanceof AltTraitConverter) || (child instanceof PhysToIteratorConverter) );

        child = child.getInputs().get( 0 );
        assertTrue( child instanceof PhysLeafRel );
    }


    /**
     * Implementation of {@link AlgTrait} for testing.
     */
    private static class AltTrait implements AlgTrait {

        private final AltTraitDef traitDef;
        private final int ordinal;
        private final String description;


        private AltTrait( AltTraitDef traitDef, String description ) {
            this.traitDef = traitDef;
            this.description = description;
            this.ordinal = altTraitOrdinal++;
        }


        @Override
        public void register( AlgOptPlanner planner ) {
        }


        @Override
        public AlgTraitDef getTraitDef() {
            return traitDef;
        }


        public boolean equals( Object other ) {
            if ( other == this ) {
                return true;
            }
            if ( !(other instanceof AltTrait) ) {
                return false;
            }
            AltTrait that = (AltTrait) other;
            return this.ordinal == that.ordinal;
        }


        public int hashCode() {
            return ordinal;
        }


        @Override
        public boolean satisfies( AlgTrait trait ) {
            return trait.equals( ALT_EMPTY_TRAIT ) || equals( trait );
        }


        public String toString() {
            return description;
        }

    }


    /**
     * Definition of {@link AltTrait}.
     */
    private static class AltTraitDef extends AlgTraitDef<AltTrait> {

        private Multimap<AlgTrait, Pair<AlgTrait, ConverterRule>> conversionMap = HashMultimap.create();


        @Override
        public Class<AltTrait> getTraitClass() {
            return AltTrait.class;
        }


        @Override
        public String getSimpleName() {
            return "alt_phys";
        }


        @Override
        public AltTrait getDefault() {
            return ALT_TRAIT;
        }


        @Override
        public AlgNode convert( AlgOptPlanner planner, AlgNode alg, AltTrait toTrait, boolean allowInfiniteCostConverters ) {
            AlgTrait fromTrait = alg.getTraitSet().getTrait( this );

            if ( conversionMap.containsKey( fromTrait ) ) {
                final AlgMetadataQuery mq = AlgMetadataQuery.instance();
                for ( Pair<AlgTrait, ConverterRule> traitAndRule : conversionMap.get( fromTrait ) ) {
                    AlgTrait trait = traitAndRule.left;
                    ConverterRule rule = traitAndRule.right;

                    if ( trait == toTrait ) {
                        AlgNode converted = rule.convert( alg );
                        if ( (converted != null)
                                && (!planner.getCost( converted, mq ).isInfinite()
                                || allowInfiniteCostConverters) ) {
                            return converted;
                        }
                    }
                }
            }

            return null;
        }


        @Override
        public boolean canConvert( AlgOptPlanner planner, AltTrait fromTrait, AltTrait toTrait ) {
            if ( conversionMap.containsKey( fromTrait ) ) {
                for ( Pair<AlgTrait, ConverterRule> traitAndRule : conversionMap.get( fromTrait ) ) {
                    if ( traitAndRule.left == toTrait ) {
                        return true;
                    }
                }
            }

            return false;
        }


        @Override
        public void registerConverterRule( AlgOptPlanner planner, ConverterRule converterRule ) {
            if ( !converterRule.isGuaranteed() ) {
                return;
            }

            AlgTrait fromTrait = converterRule.getInTrait();
            AlgTrait toTrait = converterRule.getOutTrait();

            conversionMap.put( fromTrait, Pair.of( toTrait, converterRule ) );
        }

    }


    /**
     * A relational expression with zero inputs.
     */
    private abstract static class TestLeafRel extends AbstractAlgNode {

        private String label;


        protected TestLeafRel( AlgOptCluster cluster, AlgTraitSet traits, String label ) {
            super( cluster, traits );
            this.label = label;
        }


        public String getLabel() {
            return label;
        }


        // implement AlgNode
        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeInfiniteCost();
        }


        // implement AlgNode
        @Override
        protected AlgDataType deriveRowType() {
            final AlgDataTypeFactory typeFactory = getCluster().getTypeFactory();
            return typeFactory.builder()
                    .add( "this", null, typeFactory.createJavaType( Void.TYPE ) )
                    .build();
        }


        @Override
        public AlgWriter explainTerms( AlgWriter pw ) {
            return super.explainTerms( pw ).item( "label", label );
        }


        @Override
        public String algCompareString() {
            return this.getClass().getSimpleName() + "$" + label + "&";
        }

    }


    /**
     * A relational expression with zero inputs, of NONE convention.
     */
    private static class NoneLeafRel extends TestLeafRel {

        protected NoneLeafRel( AlgOptCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( Convention.NONE ), label );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new NoneLeafRel( getCluster(), getLabel() );
        }

    }


    /**
     * Relational expression with zero inputs, of PHYS convention.
     */
    private static class PhysLeafRel extends TestLeafRel {

        PhysLeafRel( AlgOptCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( PHYS_CALLING_CONVENTION ), label );
        }


        // implement AlgNode
        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }

        // TODO: SWZ Implement clone?
    }


    /**
     * Relational expression with one input.
     */
    private abstract static class TestSingleAlg extends SingleAlg {

        protected TestSingleAlg( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child ) {
            super( cluster, traits, child );
        }


        // implement AlgNode
        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeInfiniteCost();
        }


        // implement AlgNode
        @Override
        protected AlgDataType deriveRowType() {
            return getInput().getRowType();
        }

        // TODO: SWZ Implement clone?


        @Override
        public String algCompareString() {
            return this.getClass().getSimpleName() + "$" + input.algCompareString() + "&";
        }

    }


    /**
     * Relational expression with one input, of NONE convention.
     */
    private static class NoneSingleAlg extends TestSingleAlg {

        protected NoneSingleAlg( AlgOptCluster cluster, AlgNode child ) {
            this( cluster, cluster.traitSetOf( Convention.NONE ), child );
        }


        protected NoneSingleAlg( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode child ) {
            super( cluster, traitSet, child );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new NoneSingleAlg( getCluster(), traitSet, sole( inputs ) );
        }

    }


    /**
     * A mix-in interface to extend {@link AlgNode}, for testing.
     */
    interface FooRel extends EnumerableAlg {

    }


    /**
     * Relational expression with one input, that implements the {@link FooRel} mix-in interface.
     */
    private static class IterSingleRel extends TestSingleAlg implements FooRel {

        IterSingleRel( AlgOptCluster cluster, AlgNode child ) {
            super( cluster, cluster.traitSetOf( EnumerableConvention.INSTANCE ), child );
        }


        // implement AlgNode
        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( EnumerableConvention.INSTANCE );
            return new IterSingleRel( getCluster(), sole( inputs ) );
        }


        @Override
        public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
            return null;
        }

    }


    /**
     * Relational expression with zero inputs, of the PHYS convention.
     */
    private static class PhysLeafRule extends AlgOptRule {

        PhysLeafRule() {
            super( operand( NoneLeafRel.class, any() ) );
        }


        // implement RelOptRule
        @Override
        public Convention getOutConvention() {
            return PHYS_CALLING_CONVENTION;
        }


        // implement RelOptRule
        @Override
        public void onMatch( AlgOptRuleCall call ) {
            NoneLeafRel leafRel = call.alg( 0 );
            call.transformTo( new PhysLeafRel( leafRel.getCluster(), leafRel.getLabel() ) );
        }

    }


    /**
     * Planner rule to convert a {@link NoneSingleAlg} to ENUMERABLE convention.
     */
    private static class IterSingleRule extends AlgOptRule {

        IterSingleRule() {
            super( operand( NoneSingleAlg.class, any() ) );
        }


        // implement RelOptRule
        @Override
        public Convention getOutConvention() {
            return EnumerableConvention.INSTANCE;
        }


        @Override
        public AlgTrait getOutTrait() {
            return getOutConvention();
        }


        // implement RelOptRule
        @Override
        public void onMatch( AlgOptRuleCall call ) {
            NoneSingleAlg alg = call.alg( 0 );

            AlgNode converted = convert( alg.getInput( 0 ), alg.getTraitSet().replace( getOutTrait() ) );

            call.transformTo( new IterSingleRel( alg.getCluster(), converted ) );
        }

    }


    /**
     * Another planner rule to convert a {@link NoneSingleAlg} to ENUMERABLE convention.
     */
    private static class IterSingleRule2 extends AlgOptRule {

        IterSingleRule2() {
            super( operand( NoneSingleAlg.class, any() ) );
        }


        // implement RelOptRule
        @Override
        public Convention getOutConvention() {
            return EnumerableConvention.INSTANCE;
        }


        @Override
        public AlgTrait getOutTrait() {
            return getOutConvention();
        }


        // implement RelOptRule
        @Override
        public void onMatch( AlgOptRuleCall call ) {
            NoneSingleAlg alg = call.alg( 0 );

            AlgNode converted = convert( alg.getInput( 0 ), alg.getTraitSet().replace( getOutTrait() ) );

            IterSingleRel child = new IterSingleRel( alg.getCluster(), converted );

            call.transformTo( new IterSingleRel( alg.getCluster(), child ) );
        }

    }


    /**
     * Planner rule that converts between {@link AltTrait}s.
     */
    private static class AltTraitConverterRule extends ConverterRule {

        private final AlgTrait toTrait;


        private AltTraitConverterRule( AltTrait fromTrait, AltTrait toTrait, String description ) {
            super( AlgNode.class, fromTrait, toTrait, description );
            this.toTrait = toTrait;
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            return new AltTraitConverter( alg.getCluster(), alg, toTrait );
        }


        @Override
        public boolean isGuaranteed() {
            return true;
        }

    }


    /**
     * Relational expression that converts between {@link AltTrait} values.
     */
    private static class AltTraitConverter extends ConverterImpl {

        private final AlgTrait toTrait;


        private AltTraitConverter( AlgOptCluster cluster, AlgNode child, AlgTrait toTrait ) {
            super( cluster, toTrait.getTraitDef(), child.getTraitSet().replace( toTrait ), child );
            this.toTrait = toTrait;
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new AltTraitConverter( getCluster(), AbstractAlgNode.sole( inputs ), toTrait );
        }

    }


    /**
     * Planner rule that converts from PHYS to ENUMERABLE convention.
     */
    private static class PhysToIteratorConverterRule extends ConverterRule {

        PhysToIteratorConverterRule() {
            super( AlgNode.class, PHYS_CALLING_CONVENTION, EnumerableConvention.INSTANCE, "PhysToIteratorRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            return new PhysToIteratorConverter( alg.getCluster(), alg );
        }

    }


    /**
     * Planner rule that converts PHYS to ENUMERABLE convention.
     */
    private static class PhysToIteratorConverter extends ConverterImpl {

        PhysToIteratorConverter( AlgOptCluster cluster, AlgNode child ) {
            super( cluster, ConventionTraitDef.INSTANCE, child.getTraitSet().replace( EnumerableConvention.INSTANCE ), child );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new PhysToIteratorConverter( getCluster(), AbstractAlgNode.sole( inputs ) );
        }

    }


    /**
     * Planner rule that converts an {@link IterSingleRel} on a {@link PhysToIteratorConverter} into a {@link IterMergedRel}.
     */
    private static class IterSinglePhysMergeRule extends AlgOptRule {

        IterSinglePhysMergeRule() {
            super( operand( IterSingleRel.class, operand( PhysToIteratorConverter.class, any() ) ) );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            IterSingleRel singleRel = call.alg( 0 );
            call.transformTo( new IterMergedRel( singleRel.getCluster(), null ) );
        }

    }


    /**
     * Relational expression with no inputs, that implements the {@link FooRel} mix-in interface.
     */
    private static class IterMergedRel extends TestLeafRel implements FooRel {

        IterMergedRel( AlgOptCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( EnumerableConvention.INSTANCE ), label );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeZeroCost();
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( EnumerableConvention.INSTANCE );
            assert inputs.isEmpty();
            return new IterMergedRel( getCluster(), this.getLabel() );
        }


        @Override
        public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
            return null;
        }

    }

}
