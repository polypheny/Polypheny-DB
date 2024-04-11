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

package org.polypheny.db.algebra.rules;


import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Correlate;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.hep.HepAlgVertex;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.BitSets;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Util;


/**
 * Push Project under Correlate to apply on Correlate's left and right child
 */
public class ProjectCorrelateTransposeRule extends AlgOptRule {

    public static final ProjectCorrelateTransposeRule INSTANCE = new ProjectCorrelateTransposeRule( expr -> true, AlgFactories.LOGICAL_BUILDER );


    /**
     * preserveExprCondition to define the condition for a expression not to be pushed
     */
    private final PushProjector.ExprCondition preserveExprCondition;


    public ProjectCorrelateTransposeRule( PushProjector.ExprCondition preserveExprCondition, AlgBuilderFactory algFactory ) {
        super(
                operand( Project.class, operand( Correlate.class, any() ) ),
                algFactory, null );
        this.preserveExprCondition = preserveExprCondition;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        Project origProj = call.alg( 0 );
        final Correlate corr = call.alg( 1 );

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
        AlgNode leftProjRel =
                pushProject.createProjectRefsAndExprs(
                        corr.getLeft(),
                        true,
                        false );
        AlgNode rightProjRel =
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
        RexCorrelVariable rexCorrel = (RexCorrelVariable) rexBuilder.makeCorrel( leftProjRel.getTupleType(), correlationId );

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
        AlgNode topProject = pushProject.createNewProject( newCorrRel, adjustments );

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
    public static class RelNodesExprsHandler extends AlgShuttleImpl {

        private final RexShuttle rexVisitor;


        public RelNodesExprsHandler( RexShuttle rexVisitor ) {
            this.rexVisitor = rexVisitor;
        }


        @Override
        protected AlgNode visitChild( AlgNode parent, int i, AlgNode child ) {
            if ( child instanceof HepAlgVertex ) {
                child = ((HepAlgVertex) child).getCurrentAlg();
            } else if ( child instanceof AlgSubset ) {
                AlgSubset subset = (AlgSubset) child;
                child = Util.first( subset.getBest(), subset.getOriginal() );
            }
            return super.visitChild( parent, i, child ).accept( rexVisitor );
        }

    }

}

