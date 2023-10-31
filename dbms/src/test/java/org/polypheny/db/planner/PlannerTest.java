/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.planner;

import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.type.PolyTypeFactoryImpl;

public class PlannerTest {

    static final Convention PHYS_CALLING_CONVENTION = new Convention.Impl( "PHYS", AlgNode.class ) {

        @Override
        public boolean useAbstractConvertersForConversion( AlgTraitSet fromTraits, AlgTraitSet toTraits ) {
            return true;
        }
    };


    @BeforeClass
    public static void init() {
        // to load the main classes
        TestHelper.getInstance();
    }


    @Test
    public void basicTest() {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addRule( new PhysRule() );
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        AlgOptCluster cluster = DummyCluster.newCluster( planner );
        AlgNode node = new NoneDummyNode( cluster, cluster.traitSetOf( Convention.NONE ) );

        planner.setRoot( node );

        AlgNode newAlg = planner.changeTraits( node, cluster.traitSetOf( PHYS_CALLING_CONVENTION ) );
        assert newAlg.getTraitSet().contains( PHYS_CALLING_CONVENTION );
    }


    public void dynamicRegister() {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addRule( new PhysRule() );
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        AlgOptCluster cluster = DummyCluster.newCluster( planner );
        AlgNode node = new NoneDummyNode( cluster, cluster.traitSetOf( Convention.NONE ) );
    }


    public static class DynamicRule extends AlgOptRule {

        public DynamicRule() {
            super( operandJ( NoneDummyNode.class, ), description );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {

        }

    }


    public static class PhysRule extends AlgOptRule {

        public PhysRule() {
            super( operandJ( AbstractAlgNode.class, Convention.NONE, r -> true, any() ), AddRule.class.getSimpleName() );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            AlgNode alg = call.alg( 0 );

            AlgNode dummy = alg.copy( alg.getTraitSet().replace( PHYS_CALLING_CONVENTION ), List.of() );

            call.transformTo( dummy );
        }

    }


    public static class AddRule extends AlgOptRule {

        public AddRule() {
            super( operand( AlgNode.class, any() ), AddRule.class.getSimpleName() );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            AlgNode alg = call.alg( 0 );

            SingleDummyNode singleDummy = new SingleDummyNode( alg.getCluster(), alg.getTraitSet(), alg );

            call.transformTo( singleDummy );
        }

    }


    public static class SingleDummyNode extends SingleAlg {

        protected SingleDummyNode( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input ) {
            super( cluster, traits, input );
        }


        @Override
        public String algCompareString() {
            return "$" + SingleDummyNode.class;
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new SingleDummyNode( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ) );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            if ( traitSet.contains( PHYS_CALLING_CONVENTION ) ) {
                return planner.getCostFactory().makeCost( 1d, 1d, 1d );
            }
            return planner.getCostFactory().makeInfiniteCost();
        }


        @Override
        protected AlgDataType deriveRowType() {
            final AlgDataTypeFactory typeFactory = getCluster().getTypeFactory();
            return typeFactory.builder()
                    .add( null, "this", null, typeFactory.createJavaType( Void.TYPE ) )
                    .build();
        }

    }


    public static class NoneDummyNode extends AbstractAlgNode {


        public NoneDummyNode( AlgOptCluster cluster, AlgTraitSet traitSet ) {
            super( cluster, traitSet );
        }


        @Override
        public String algCompareString() {
            return "$" + NoneDummyNode.class;
        }


        @Override
        protected AlgDataType deriveRowType() {
            final AlgDataTypeFactory typeFactory = getCluster().getTypeFactory();
            return typeFactory.builder()
                    .add( null, "this", null, typeFactory.createJavaType( Void.TYPE ) )
                    .build();
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert inputs.isEmpty();
            return new NoneDummyNode( getCluster(), traitSet );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            if ( traitSet.contains( PHYS_CALLING_CONVENTION ) ) {
                return planner.getCostFactory().makeCost( 1d, 1d, 1d );
            }
            return planner.getCostFactory().makeInfiniteCost();
        }

    }


    public static class DummyCluster {

        static AlgOptCluster newCluster( VolcanoPlanner planner ) {
            final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
            return AlgOptCluster.create( planner, new RexBuilder( typeFactory ), planner.emptyTraitSet(), Catalog.snapshot() );
        }

    }

}
