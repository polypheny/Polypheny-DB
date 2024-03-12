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


import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.polypheny.db.sql.volcano.PlannerTests.GoodSingleRule;
import static org.polypheny.db.sql.volcano.PlannerTests.PHYS_CALLING_CONVENTION;
import static org.polypheny.db.sql.volcano.PlannerTests.PhysSingleAlg;
import static org.polypheny.db.sql.volcano.PlannerTests.TestSingleAlg;
import static org.polypheny.db.sql.volcano.PlannerTests.newCluster;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.sql.volcano.PlannerTests.NoneLeafAlg;
import org.polypheny.db.sql.volcano.PlannerTests.NoneSingleAlg;
import org.polypheny.db.sql.volcano.PlannerTests.PhysLeafAlg;


/**
 * Unit test for {@link VolcanoPlanner}
 */
public class ComboRuleTest {

    @Test
    public void testCombo() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        planner.addRule( new ComboRule() );
        planner.addRule( new AddIntermediateNodeRule() );
        planner.addRule( new GoodSingleRule() );

        AlgCluster cluster = newCluster( planner );
        NoneLeafAlg leafRel = new NoneLeafAlg( cluster, "a" );
        NoneSingleAlg singleRel = new NoneSingleAlg( cluster, leafRel );
        NoneSingleAlg singleRel2 = new NoneSingleAlg( cluster, singleRel );
        AlgNode convertedRel = planner.changeTraits( singleRel2, cluster.traitSetOf( PHYS_CALLING_CONVENTION ) );
        planner.setRoot( convertedRel );
        AlgNode result = planner.chooseDelegate().findBestExp();
        assertTrue( result instanceof IntermediateNode );
    }


    /**
     * Intermediate node, the cost decreases as it is pushed up the tree (more inputs it has, cheaper it gets).
     */
    private static class IntermediateNode extends TestSingleAlg {

        final int nodesBelowCount;


        IntermediateNode( AlgCluster cluster, AlgNode input, int nodesBelowCount ) {
            super( cluster, cluster.traitSetOf( PHYS_CALLING_CONVENTION ), input );
            this.nodesBelowCount = nodesBelowCount;
        }


        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeCost( 100, 100, 100 ).multiplyBy( 1.0 / nodesBelowCount );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert traitSet.comprises( PHYS_CALLING_CONVENTION );
            return new IntermediateNode( getCluster(), sole( inputs ), nodesBelowCount );
        }

    }


    /**
     * Rule that adds an intermediate node above the {@link PhysLeafAlg}.
     */
    private static class AddIntermediateNodeRule extends AlgOptRule {

        AddIntermediateNodeRule() {
            super( operand( NoneLeafAlg.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            NoneLeafAlg leaf = call.alg( 0 );

            AlgNode physLeaf = new PhysLeafAlg( leaf.getCluster(), leaf.label );
            AlgNode intermediateNode = new IntermediateNode( physLeaf.getCluster(), physLeaf, 1 );

            call.transformTo( intermediateNode );
        }

    }


    /**
     * Matches {@link PhysSingleAlg}-{@link IntermediateNode}-Any and converts to {@link IntermediateNode}-{@link PhysSingleAlg}-Any.
     */
    private static class ComboRule extends AlgOptRule {

        ComboRule() {
            super( createOperand() );
        }


        private static AlgOptRuleOperand createOperand() {
            AlgOptRuleOperand input = operand( AlgNode.class, any() );
            input = operand( IntermediateNode.class, some( input ) );
            input = operand( PhysSingleAlg.class, some( input ) );
            return input;
        }


        @Override
        public Convention getOutConvention() {
            return PHYS_CALLING_CONVENTION;
        }


        @Override
        public boolean matches( AlgOptRuleCall call ) {
            if ( call.algs.length < 3 ) {
                return false;
            }

            return call.alg( 0 ) instanceof PhysSingleAlg
                    && call.alg( 1 ) instanceof IntermediateNode
                    && call.alg( 2 ) != null;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            List<AlgNode> newInputs = ImmutableList.of( call.alg( 2 ) );
            IntermediateNode oldInter = call.alg( 1 );
            AlgNode physRel = call.alg( 0 ).copy( call.alg( 0 ).getTraitSet(), newInputs );
            AlgNode converted = new IntermediateNode( physRel.getCluster(), physRel, oldInter.nodesBelowCount + 1 );
            call.transformTo( converted );
        }

    }

}

