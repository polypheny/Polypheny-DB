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


import java.util.List;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
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
    static final Convention PHYS_CALLING_CONVENTION = new Convention.Impl( "PHYS", AlgNode.class ) {

        @Override
        public boolean useAbstractConvertersForConversion( AlgTraitSet fromTraits, AlgTraitSet toTraits ) {
            return true;
        }
    };


    static AlgCluster newCluster( VolcanoPlanner planner ) {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        return AlgCluster.create( planner, new RexBuilder( typeFactory ), planner.emptyTraitSet(), Catalog.snapshot() );
    }


    /**
     * Leaf relational expression.
     */
    abstract static class TestLeafAlg extends AbstractAlgNode {

        final String label;


        TestLeafAlg( AlgCluster cluster, AlgTraitSet traits, String label ) {
            super( cluster, traits );
            this.label = label;
        }


        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeInfiniteCost();
        }


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
     * Relational expression with one input.
     */
    abstract static class TestSingleAlg extends SingleAlg {

        TestSingleAlg( AlgCluster cluster, AlgTraitSet traits, AlgNode input ) {
            super( cluster, traits, input );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeInfiniteCost();
        }


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
     * Relational expression with one input and convention NONE.
     */
    static class NoneSingleAlg extends TestSingleAlg {

        NoneSingleAlg( AlgCluster cluster, AlgNode input ) {
            super( cluster, cluster.traitSetOf( Convention.NONE ), input );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( Convention.NONE );
            return new NoneSingleAlg( getCluster(), sole( inputs ) );
        }

    }


    /**
     * Relational expression with zero inputs and convention NONE.
     */
    static class NoneLeafAlg extends TestLeafAlg {

        NoneLeafAlg( AlgCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( Convention.NONE ), label );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( Convention.NONE );
            assert inputs.isEmpty();
            return this;
        }

    }


    /**
     * Relational expression with zero inputs and convention PHYS.
     */
    static class PhysLeafAlg extends TestLeafAlg {

        PhysLeafAlg( AlgCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( PHYS_CALLING_CONVENTION ), label );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( PHYS_CALLING_CONVENTION );
            assert inputs.isEmpty();
            return this;
        }

    }


    /**
     * Relational expression with one input and convention PHYS.
     */
    static class PhysSingleAlg extends TestSingleAlg {

        PhysSingleAlg( AlgCluster cluster, AlgNode input ) {
            super( cluster, cluster.traitSetOf( PHYS_CALLING_CONVENTION ), input );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( PHYS_CALLING_CONVENTION );
            return new PhysSingleAlg( getCluster(), sole( inputs ) );
        }

    }


    /**
     * Planner rule that converts {@link NoneLeafAlg} to PHYS convention.
     */
    static class PhysLeafRule extends AlgOptRule {

        PhysLeafRule() {
            super( operand( NoneLeafAlg.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            NoneLeafAlg leafRel = call.alg( 0 );
            call.transformTo( new PhysLeafAlg( leafRel.getCluster(), leafRel.label ) );
        }

    }


    /**
     * Planner rule that matches a {@link NoneSingleAlg} and succeeds.
     */
    static class GoodSingleRule extends AlgOptRule {

        GoodSingleRule() {
            super( operand( NoneSingleAlg.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            NoneSingleAlg single = call.alg( 0 );
            AlgNode input = single.getInput();
            AlgNode physInput = convert( input, single.getTraitSet().replace( PHYS_CALLING_CONVENTION ) );
            call.transformTo( new PhysSingleAlg( single.getCluster(), physInput ) );
        }

    }

}

