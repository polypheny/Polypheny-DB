/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.plan.volcano;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.AbstractRelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.SingleRel;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFactoryImpl;
import java.util.List;


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
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl( ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem.DEFAULT );
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
        public RelOptCost computeSelfCost( RelOptPlanner planner,
                RelMetadataQuery mq ) {
            return planner.getCostFactory().makeInfiniteCost();
        }


        @Override
        protected RelDataType deriveRowType() {
            final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
            return typeFactory.builder()
                    .add( "this", typeFactory.createJavaType( Void.TYPE ) )
                    .build();
        }


        @Override
        public RelWriter explainTerms( RelWriter pw ) {
            return super.explainTerms( pw ).item( "label", label );
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

