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

package org.polypheny.db.planner;

import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
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

public class PlannerTest {

    static final Convention PHYS_CALLING_CONVENTION = new Convention.Impl( "PHYS", AlgNode.class ) {

        @Override
        public boolean useAbstractConvertersForConversion( AlgTraitSet fromTraits, AlgTraitSet toTraits ) {
            return true;
        }
    };


    @BeforeAll
    public static void init() {
        // to load the main classes
        TestHelper.getInstance();
    }


    @Test
    public void basicTest() {
        VolcanoPlanner planner = new VolcanoPlanner();

        // add rule to transform to Physical
        planner.addRule( new PhysRule() );

        // add trait so it is known to the optimizer
        planner.addAlgTraitDef( PHYS_CALLING_CONVENTION.getTraitDef() );

        // build the nodes
        AlgCluster cluster = DummyCluster.newCluster( planner );
        AlgNode node = new LeafDummyNode( cluster, cluster.traitSetOf( Convention.NONE ) );
        AlgNode root = new SingleDummyNode( cluster, cluster.traitSetOf( Convention.NONE ), node );

        AlgNode newRoot = planner.changeTraits( root, root.getTraitSet().replace( PHYS_CALLING_CONVENTION ) );
        planner.setRoot( newRoot );

        AlgNode newAlg = planner.findBestExp();
        assert newAlg.getTraitSet().contains( PHYS_CALLING_CONVENTION );
    }


    @Test
    @Disabled
    public void dynamicAddRuleTest() {
        VolcanoPlanner planner = new VolcanoPlanner();

        // add rule to transform to Physical
        planner.addRule( new DynamicRule() );

        // add trait so it is known to the optimizer
        planner.addAlgTraitDef( PHYS_CALLING_CONVENTION.getTraitDef() );

        // build the nodes
        AlgCluster cluster = DummyCluster.newCluster( planner );
        AlgNode node = new LeafDummyNode( cluster, cluster.traitSetOf( Convention.NONE ) );
        AlgNode root = new SingleDummyNode( cluster, cluster.traitSetOf( Convention.NONE ), node );

        AlgNode newRoot = planner.changeTraits( root, root.getTraitSet().replace( PHYS_CALLING_CONVENTION ) );
        planner.setRoot( newRoot );

        AlgNode newAlg = planner.findBestExp();
        assert newAlg.getTraitSet().contains( PHYS_CALLING_CONVENTION );
    }


    public static class DynamicRule extends AlgOptRule {

        public DynamicRule() {
            super( operand( LeafDummyNode.class, Convention.NONE, r -> true, any() ), DynamicRule.class.getSimpleName() );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            AlgNode alg = call.alg( 0 );

            call.getPlanner().addRule( PhysRule.INSTANCE );
            call.getPlanner().addRule( PhysRule.INSTANCE );
        }

    }


    public static class PhysRule extends AlgOptRule {

        public final static PhysRule INSTANCE = new PhysRule();


        public PhysRule() {
            super( operand( AlgNode.class, Convention.NONE, r -> r instanceof LeafDummyNode || r instanceof SingleDummyNode, any() ), PhysRule.class.getSimpleName() );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            AlgNode alg = call.alg( 0 );

            AlgNode dummy;
            if ( alg.getInputs().isEmpty() ) {
                dummy = alg.copy( alg.getTraitSet().replace( PHYS_CALLING_CONVENTION ), alg.getInputs() );
            } else {
                AlgNode converted = convert( alg.getInputs().get( 0 ), alg.getTraitSet().replace( PHYS_CALLING_CONVENTION ) );
                dummy = alg.copy( alg.getTraitSet().replace( PHYS_CALLING_CONVENTION ), List.of( converted ) );
            }

            call.transformTo( dummy );
        }

    }


    public static class SingleDummyNode extends SingleAlg {

        protected SingleDummyNode( AlgCluster cluster, AlgTraitSet traits, AlgNode input ) {
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
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            if ( traitSet.contains( PHYS_CALLING_CONVENTION ) ) {
                return planner.getCostFactory().makeTinyCost();
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


    public static class LeafDummyNode extends AbstractAlgNode {


        public LeafDummyNode( AlgCluster cluster, AlgTraitSet traitSet ) {
            super( cluster, traitSet );
        }


        @Override
        public String algCompareString() {
            return "$" + LeafDummyNode.class;
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
            return new LeafDummyNode( getCluster(), traitSet );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            if ( traitSet.contains( PHYS_CALLING_CONVENTION ) ) {
                return planner.getCostFactory().makeTinyCost();
            }
            return planner.getCostFactory().makeInfiniteCost();
        }

    }


    public static class DummyCluster {

        static AlgCluster newCluster( VolcanoPlanner planner ) {
            final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
            return AlgCluster.create( planner, new RexBuilder( typeFactory ), planner.emptyTraitSet(), Catalog.snapshot() );
        }

    }

}
