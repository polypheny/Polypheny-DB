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
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.rules;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepRelVertex;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.RelSubset;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelShuttleImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Correlate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.CorrelationId;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCorrelVariable;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexFieldAccess;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexShuttle;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.BitSets;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;


/**
 * Push Project under Correlate to apply on Correlate's left and right child
 */
public class ProjectCorrelateTransposeRule extends RelOptRule {

    public static final ProjectCorrelateTransposeRule INSTANCE = new ProjectCorrelateTransposeRule( expr -> true, RelFactories.LOGICAL_BUILDER );


    /**
     * preserveExprCondition to define the condition for a expression not to be pushed
     */
    private final PushProjector.ExprCondition preserveExprCondition;


    public ProjectCorrelateTransposeRule( PushProjector.ExprCondition preserveExprCondition, RelBuilderFactory relFactory ) {
        super(
                operand( Project.class, operand( Correlate.class, any() ) ),
                relFactory, null );
        this.preserveExprCondition = preserveExprCondition;
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        Project origProj = call.rel( 0 );
        final Correlate corr = call.rel( 1 );

        // Locate all fields referenced in the projection determine which inputs are referenced in the projection;
        // If all fields are being referenced and there are no special expressions, no point in proceeding any further
        PushProjector pushProject =
                new PushProjector(
                        origProj,
                        call.builder().literal( true ),
                        corr,
                        preserveExprCondition,
                        call.builder() );
        if ( pushProject.locateAllRefs() ) {
            return;
        }

        // Create left and right projections, projecting only those fields referenced on each side
        RelNode leftProjRel =
                pushProject.createProjectRefsAndExprs(
                        corr.getLeft(),
                        true,
                        false );
        RelNode rightProjRel =
                pushProject.createProjectRefsAndExprs(
                        corr.getRight(),
                        true,
                        true );

        Map<Integer, Integer> requiredColsMap = new HashMap<>();

        // adjust requiredColumns that reference the projected columns
        int[] adjustments = pushProject.getAdjustments();
        BitSet updatedBits = new BitSet();
        for ( Integer col : corr.getRequiredColumns() ) {
            int newCol = col + adjustments[col];
            updatedBits.set( newCol );
            requiredColsMap.put( col, newCol );
        }

        RexBuilder rexBuilder = call.builder().getRexBuilder();

        CorrelationId correlationId = corr.getCluster().createCorrel();
        RexCorrelVariable rexCorrel = (RexCorrelVariable) rexBuilder.makeCorrel( leftProjRel.getRowType(), correlationId );

        // updates RexCorrelVariable and sets actual RelDataType for RexFieldAccess
        rightProjRel = rightProjRel.accept( new RelNodesExprsHandler( new RexFieldAccessReplacer( corr.getCorrelationId(), rexCorrel, rexBuilder, requiredColsMap ) ) );

        // create a new correlate with the projected children
        Correlate newCorrRel =
                corr.copy(
                        corr.getTraitSet(),
                        leftProjRel,
                        rightProjRel,
                        correlationId,
                        ImmutableBitSet.of( BitSets.toIter( updatedBits ) ),
                        corr.getJoinType() );

        // put the original project on top of the correlate, converting it to reference the modified projection list
        RelNode topProject = pushProject.createNewProject( newCorrRel, adjustments );

        call.transformTo( topProject );
    }


    /**
     * Visitor for RexNodes which replaces {@link RexCorrelVariable} with specified.
     */
    public static class RexFieldAccessReplacer extends RexShuttle {

        private final RexBuilder builder;
        private final CorrelationId rexCorrelVariableToReplace;
        private final RexCorrelVariable rexCorrelVariable;
        private final Map<Integer, Integer> requiredColsMap;


        public RexFieldAccessReplacer( CorrelationId rexCorrelVariableToReplace, RexCorrelVariable rexCorrelVariable, RexBuilder builder, Map<Integer, Integer> requiredColsMap ) {
            this.rexCorrelVariableToReplace = rexCorrelVariableToReplace;
            this.rexCorrelVariable = rexCorrelVariable;
            this.builder = builder;
            this.requiredColsMap = requiredColsMap;
        }


        @Override
        public RexNode visitCorrelVariable( RexCorrelVariable variable ) {
            if ( variable.id.equals( rexCorrelVariableToReplace ) ) {
                return rexCorrelVariable;
            }
            return variable;
        }


        @Override
        public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
            RexNode refExpr = fieldAccess.getReferenceExpr().accept( this );
            // creates new RexFieldAccess instance for the case when referenceExpr was replaced.
            // Otherwise calls super method.
            if ( refExpr == rexCorrelVariable ) {
                return builder.makeFieldAccess( refExpr, requiredColsMap.get( fieldAccess.getField().getIndex() ) );
            }
            return super.visitFieldAccess( fieldAccess );
        }
    }


    /**
     * Visitor for RelNodes which applies specified {@link RexShuttle} visitor for every node in the tree.
     */
    public static class RelNodesExprsHandler extends RelShuttleImpl {

        private final RexShuttle rexVisitor;


        public RelNodesExprsHandler( RexShuttle rexVisitor ) {
            this.rexVisitor = rexVisitor;
        }


        @Override
        protected RelNode visitChild( RelNode parent, int i, RelNode child ) {
            if ( child instanceof HepRelVertex ) {
                child = ((HepRelVertex) child).getCurrentRel();
            } else if ( child instanceof RelSubset ) {
                RelSubset subset = (RelSubset) child;
                child = Util.first( subset.getBest(), subset.getOriginal() );
            }
            return super.visitChild( parent, i, child ).accept( rexVisitor );
        }
    }
}

