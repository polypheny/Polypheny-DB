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

package org.polypheny.db.sql.volcano;


import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.List;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.sql.SqlLanguageDependent;
import org.polypheny.db.util.Pair;


/**
 * Unit test for handling of traits by {@link VolcanoPlanner}.
 */
public class VolcanoPlannerTraitTest extends SqlLanguageDependent {

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


    @Test
    public void testRuleMatchAfterConversion() {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( ALT_TRAIT_DEF );

        planner.addRule( new PhysToIteratorConverterRule() );
        planner.addRule( new PhysLeafRule() );
        planner.addRule( new IterSingleRule() );
        planner.addRule( new IterSinglePhysMergeRule() );

        AlgCluster cluster = PlannerTests.newCluster( planner );

        NoneLeafAlg noneLeafAlg = AlgOptUtil.addTrait( new NoneLeafAlg( cluster, "noneLeafAlg" ), ALT_TRAIT );

        NoneSingleAlg noneAlg = AlgOptUtil.addTrait( new NoneSingleAlg( cluster, noneLeafAlg ), ALT_EMPTY_TRAIT );

        AlgNode convertedAlg = planner.changeTraits( noneAlg, cluster.traitSetOf( EnumerableConvention.INSTANCE ).replace( ALT_EMPTY_TRAIT ) );

        planner.setRoot( convertedAlg );
        AlgNode result = planner.chooseDelegate().findBestExp();

        assertInstanceOf( IterMergedAlg.class, result );
    }


    /**
     * Implementation of {@link AlgTrait} for testing.
     */
    private static class AltTrait implements AlgTrait<AltTraitDef> {

        private final AltTraitDef traitDef;
        private final int ordinal;
        private final String description;


        private AltTrait( AltTraitDef traitDef, String description ) {
            this.traitDef = traitDef;
            this.description = description;
            this.ordinal = altTraitOrdinal++;
        }


        @Override
        public void register( AlgPlanner planner ) {
        }


        @Override
        public AltTraitDef getTraitDef() {
            return traitDef;
        }

        public boolean equals( Object other ) {
            if ( other == this ) {
                return true;
            }
            if ( !(other instanceof AltTrait that) ) {
                return false;
            }
            return this.ordinal == that.ordinal;
        }


        public int hashCode() {
            return ordinal;
        }


        @Override
        public boolean satisfies( AlgTrait<?> trait ) {
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

        private final Multimap<AlgTrait<?>, Pair<AlgTrait<?>, ConverterRule>> conversions = HashMultimap.create();


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
        public AlgNode convert( AlgPlanner planner, AlgNode alg, AltTrait toTrait, boolean allowInfiniteCostConverters ) {
            AlgTrait<?> fromTrait = alg.getTraitSet().getTrait( this );

            if ( conversions.containsKey( fromTrait ) ) {
                final AlgMetadataQuery mq = AlgMetadataQuery.instance();
                for ( Pair<AlgTrait<?>, ConverterRule> traitAndRule : conversions.get( fromTrait ) ) {
                    AlgTrait<?> trait = traitAndRule.left;
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
        public boolean canConvert( AlgPlanner planner, AltTrait fromTrait, AltTrait toTrait ) {
            if ( conversions.containsKey( fromTrait ) ) {
                for ( Pair<AlgTrait<?>, ConverterRule> traitAndRule : conversions.get( fromTrait ) ) {
                    if ( traitAndRule.left == toTrait ) {
                        return true;
                    }
                }
            }

            return false;
        }


        @Override
        public void registerConverterRule( AlgPlanner planner, ConverterRule converterRule ) {
            if ( !converterRule.isGuaranteed() ) {
                return;
            }

            AlgTrait<?> fromTrait = converterRule.getInTrait();
            AlgTrait<?> toTrait = converterRule.getOutTrait();

            conversions.put( fromTrait, Pair.of( toTrait, converterRule ) );
        }

    }


    /**
     * A relational expression with zero inputs.
     */
    @Getter
    private abstract static class TestLeafAlg extends AbstractAlgNode {

        private final String label;


        protected TestLeafAlg( AlgCluster cluster, AlgTraitSet traits, String label ) {
            super( cluster, traits );
            this.label = label;
        }


        // implement AlgNode
        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeInfiniteCost();
        }


        // implement AlgNode
        @Override
        protected AlgDataType deriveRowType() {
            final AlgDataTypeFactory typeFactory = getCluster().getTypeFactory();
            return typeFactory.builder()
                    .add( null, "this", null, typeFactory.createJavaType( Void.TYPE ) )
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
    private static class NoneLeafAlg extends TestLeafAlg {

        protected NoneLeafAlg( AlgCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( Convention.NONE ), label );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new NoneLeafAlg( getCluster(), getLabel() );
        }

    }


    /**
     * Algebra expression with zero inputs, of PHYS convention.
     */
    private static class PhysLeafAlg extends TestLeafAlg {

        PhysLeafAlg( AlgCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( PHYS_CALLING_CONVENTION ), label );
        }


        // implement AlgNode
        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }

    }


    /**
     * Algebra expression with one input.
     */
    private abstract static class TestSingleAlg extends SingleAlg {

        protected TestSingleAlg( AlgCluster cluster, AlgTraitSet traits, AlgNode child ) {
            super( cluster, traits, child );
        }


        // implement AlgNode
        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeInfiniteCost();
        }


        // implement AlgNode
        @Override
        protected AlgDataType deriveRowType() {
            return getInput().getTupleType();
        }


        @Override
        public String algCompareString() {
            return this.getClass().getSimpleName() + "$" + input.algCompareString() + "&";
        }

    }


    /**
     * Algebra expression with one input, of NONE convention.
     */
    private static class NoneSingleAlg extends TestSingleAlg {

        protected NoneSingleAlg( AlgCluster cluster, AlgNode child ) {
            this( cluster, cluster.traitSetOf( Convention.NONE ), child );
        }


        protected NoneSingleAlg( AlgCluster cluster, AlgTraitSet traitSet, AlgNode child ) {
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
    interface FooAlg extends EnumerableAlg {

    }


    /**
     * Algebra expression with one input, that implements the {@link FooAlg} mix-in interface.
     */
    private static class IterSingleAlg extends TestSingleAlg implements FooAlg {

        IterSingleAlg( AlgCluster cluster, AlgNode child ) {
            super( cluster, cluster.traitSetOf( EnumerableConvention.INSTANCE ), child );
        }


        // implement AlgNode
        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( EnumerableConvention.INSTANCE );
            return new IterSingleAlg( getCluster(), sole( inputs ) );
        }


        @Override
        public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
            return null;
        }

    }


    /**
     * Algebra expression with zero inputs, of the PHYS convention.
     */
    private static class PhysLeafRule extends AlgOptRule {

        PhysLeafRule() {
            super( operand( NoneLeafAlg.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            NoneLeafAlg leafAlg = call.alg( 0 );
            call.transformTo( new PhysLeafAlg( leafAlg.getCluster(), leafAlg.getLabel() ) );
        }

    }


    /**
     * Planner rule to convert a {@link NoneSingleAlg} to ENUMERABLE convention.
     */
    private static class IterSingleRule extends AlgOptRule {

        IterSingleRule() {
            super( operand( NoneSingleAlg.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return EnumerableConvention.INSTANCE;
        }


        @Override
        public AlgTrait<?> getOutTrait() {
            return getOutConvention();
        }

        @Override
        public void onMatch( AlgOptRuleCall call ) {
            NoneSingleAlg alg = call.alg( 0 );

            AlgNode converted = convert( alg.getInput( 0 ), alg.getTraitSet().replace( getOutTrait() ) );

            call.transformTo( new IterSingleAlg( alg.getCluster(), converted ) );
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

        PhysToIteratorConverter( AlgCluster cluster, AlgNode child ) {
            super( cluster, ConventionTraitDef.INSTANCE, child.getTraitSet().replace( EnumerableConvention.INSTANCE ), child );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new PhysToIteratorConverter( getCluster(), AbstractAlgNode.sole( inputs ) );
        }

    }


    /**
     * Planner rule that converts an {@link IterSingleAlg} on a {@link PhysToIteratorConverter} into a {@link IterMergedAlg}.
     */
    private static class IterSinglePhysMergeRule extends AlgOptRule {

        IterSinglePhysMergeRule() {
            super( operand( IterSingleAlg.class, operand( PhysToIteratorConverter.class, any() ) ) );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            IterSingleAlg singleAlg = call.alg( 0 );
            call.transformTo( new IterMergedAlg( singleAlg.getCluster(), null ) );
        }

    }


    /**
     * Algebra expression with no inputs, that implements the {@link FooAlg} mix-in interface.
     */
    private static class IterMergedAlg extends TestLeafAlg implements FooAlg {

        IterMergedAlg( AlgCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( EnumerableConvention.INSTANCE ), label );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeZeroCost();
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( EnumerableConvention.INSTANCE );
            assert inputs.isEmpty();
            return new IterMergedAlg( getCluster(), this.getLabel() );
        }


        @Override
        public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
            return null;
        }

    }

}
