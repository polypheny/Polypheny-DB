/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.languages.core.volcano;


import java.util.List;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelWriter;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.type.PolyTypeFactoryImpl;


/**
 * Common classes and utility methods for Volcano planner tests.
 */
class PlannerTests {

    private PlannerTests() {
    }


    /**
     * Private calling convention representing a physical implementation.
     */
    static final Convention PHYS_CALLING_CONVENTION = new Convention.Impl( "PHYS", RelNode.class ) {
        @Override
        public boolean canConvertConvention( Convention toConvention ) {
            return true;
        }


        @Override
        public boolean useAbstractConvertersForConversion( RelTraitSet fromTraits, RelTraitSet toTraits ) {
            return true;
        }
    };


    static RelOptCluster newCluster( VolcanoPlanner planner ) {
        final RelDataTypeFactory typeFactory = new PolyTypeFactoryImpl( org.polypheny.db.rel.type.RelDataTypeSystem.DEFAULT );
        return RelOptCluster.create( planner, new RexBuilder( typeFactory ) );
    }


    /**
     * Leaf relational expression.
     */
    abstract static class TestLeafRel extends AbstractRelNode {

        final String label;


        TestLeafRel( RelOptCluster cluster, RelTraitSet traits, String label ) {
            super( cluster, traits );
            this.label = label;
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return planner.getCostFactory().makeInfiniteCost();
        }


        @Override
        protected RelDataType deriveRowType() {
            final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
            return typeFactory.builder()
                    .add( "this", null, typeFactory.createJavaType( Void.TYPE ) )
                    .build();
        }


        @Override
        public RelWriter explainTerms( RelWriter pw ) {
            return super.explainTerms( pw ).item( "label", label );
        }


        @Override
        public String relCompareString() {
            return this.getClass().getSimpleName() + "$" + label + "&";
        }
    }


    /**
     * Relational expression with one input.
     */
    abstract static class TestSingleRel extends SingleRel {

        TestSingleRel( RelOptCluster cluster, RelTraitSet traits, RelNode input ) {
            super( cluster, traits, input );
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return planner.getCostFactory().makeInfiniteCost();
        }


        @Override
        protected RelDataType deriveRowType() {
            return getInput().getRowType();
        }


        @Override
        public String relCompareString() {
            return this.getClass().getSimpleName() + "$" + input.relCompareString() + "&";
        }
    }


    /**
     * Relational expression with one input and convention NONE.
     */
    static class NoneSingleRel extends TestSingleRel {

        NoneSingleRel( RelOptCluster cluster, RelNode input ) {
            super( cluster, cluster.traitSetOf( Convention.NONE ), input );
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            assert traitSet.comprises( Convention.NONE );
            return new NoneSingleRel( getCluster(), sole( inputs ) );
        }
    }


    /**
     * Relational expression with zero inputs and convention NONE.
     */
    static class NoneLeafRel extends TestLeafRel {

        NoneLeafRel( RelOptCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( Convention.NONE ), label );
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            assert traitSet.comprises( Convention.NONE );
            assert inputs.isEmpty();
            return this;
        }
    }


    /**
     * Relational expression with zero inputs and convention PHYS.
     */
    static class PhysLeafRel extends TestLeafRel {

        PhysLeafRel( RelOptCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( PHYS_CALLING_CONVENTION ), label );
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            assert traitSet.comprises( PHYS_CALLING_CONVENTION );
            assert inputs.isEmpty();
            return this;
        }
    }


    /**
     * Relational expression with one input and convention PHYS.
     */
    static class PhysSingleRel extends TestSingleRel {

        PhysSingleRel( RelOptCluster cluster, RelNode input ) {
            super( cluster, cluster.traitSetOf( PHYS_CALLING_CONVENTION ), input );
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            assert traitSet.comprises( PHYS_CALLING_CONVENTION );
            return new PhysSingleRel( getCluster(), sole( inputs ) );
        }
    }


    /**
     * Planner rule that converts {@link NoneLeafRel} to PHYS convention.
     */
    static class PhysLeafRule extends RelOptRule {

        PhysLeafRule() {
            super( operand( NoneLeafRel.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            NoneLeafRel leafRel = call.rel( 0 );
            call.transformTo( new PhysLeafRel( leafRel.getCluster(), leafRel.label ) );
        }
    }


    /**
     * Planner rule that matches a {@link NoneSingleRel} and succeeds.
     */
    static class GoodSingleRule extends RelOptRule {

        GoodSingleRule() {
            super( operand( NoneSingleRel.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            NoneSingleRel single = call.rel( 0 );
            RelNode input = single.getInput();
            RelNode physInput = convert( input, single.getTraitSet().replace( PHYS_CALLING_CONVENTION ) );
            call.transformTo( new PhysSingleRel( single.getCluster(), physInput ) );
        }
    }
}

