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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.ValidatorUtil;


/**
 * Planner rule that matches a {@link org.polypheny.db.algebra.core.Join} one of whose inputs is a {@link LogicalRelProject}, and pulls the project up.
 *
 * Projections are pulled up if the {@link LogicalRelProject} doesn't originate from a null generating input in an outer join.
 */
public class JoinProjectTransposeRule extends AlgOptRule {

    public static final JoinProjectTransposeRule BOTH_PROJECT =
            new JoinProjectTransposeRule(
                    operand(
                            LogicalRelJoin.class,
                            operand( LogicalRelProject.class, any() ),
                            operand( LogicalRelProject.class, any() ) ),
                    "JoinProjectTransposeRule(Project-Project)" );

    public static final JoinProjectTransposeRule LEFT_PROJECT =
            new JoinProjectTransposeRule(
                    operand( LogicalRelJoin.class, some( operand( LogicalRelProject.class, any() ) ) ),
                    "JoinProjectTransposeRule(Project-Other)" );

    public static final JoinProjectTransposeRule RIGHT_PROJECT =
            new JoinProjectTransposeRule(
                    operand(
                            LogicalRelJoin.class,
                            operand( AlgNode.class, any() ),
                            operand( LogicalRelProject.class, any() ) ),
                    "JoinProjectTransposeRule(Other-Project)" );

    public static final JoinProjectTransposeRule BOTH_PROJECT_INCLUDE_OUTER =
            new JoinProjectTransposeRule(
                    operand(
                            LogicalRelJoin.class,
                            operand( LogicalRelProject.class, any() ),
                            operand( LogicalRelProject.class, any() ) ),
                    "Join(IncludingOuter)ProjectTransposeRule(Project-Project)",
                    true,
                    AlgFactories.LOGICAL_BUILDER );

    public static final JoinProjectTransposeRule LEFT_PROJECT_INCLUDE_OUTER =
            new JoinProjectTransposeRule(
                    operand(
                            LogicalRelJoin.class,
                            some( operand( LogicalRelProject.class, any() ) ) ),
                    "Join(IncludingOuter)ProjectTransposeRule(Project-Other)",
                    true,
                    AlgFactories.LOGICAL_BUILDER );

    public static final JoinProjectTransposeRule RIGHT_PROJECT_INCLUDE_OUTER =
            new JoinProjectTransposeRule(
                    operand(
                            LogicalRelJoin.class,
                            operand( AlgNode.class, any() ),
                            operand( LogicalRelProject.class, any() ) ),
                    "Join(IncludingOuter)ProjectTransposeRule(Other-Project)",
                    true,
                    AlgFactories.LOGICAL_BUILDER );

    private final boolean includeOuter;


    /**
     * Creates a JoinProjectTransposeRule.
     */
    public JoinProjectTransposeRule( AlgOptRuleOperand operand, String description, boolean includeOuter, AlgBuilderFactory algBuilderFactory ) {
        super( operand, algBuilderFactory, description );
        this.includeOuter = includeOuter;
    }


    /**
     * Creates a JoinProjectTransposeRule with default factory.
     */
    public JoinProjectTransposeRule( AlgOptRuleOperand operand, String description ) {
        this( operand, description, false, AlgFactories.LOGICAL_BUILDER );
    }


    // implement RelOptRule
    @Override
    public void onMatch( AlgOptRuleCall call ) {
        Join joinRel = call.alg( 0 );
        JoinAlgType joinType = joinRel.getJoinType();

        Project leftProj;
        Project rightProj;
        AlgNode leftJoinChild;
        AlgNode rightJoinChild;

        // If 1) the rule works on outer joins, or
        //    2) input's projection doesn't generate nulls
        if ( hasLeftChild( call ) && (includeOuter || !joinType.generatesNullsOnLeft()) ) {
            leftProj = call.alg( 1 );
            leftJoinChild = getProjectChild( call, leftProj, true );
        } else {
            leftProj = null;
            leftJoinChild = call.alg( 1 );
        }
        if ( hasRightChild( call ) && (includeOuter || !joinType.generatesNullsOnRight()) ) {
            rightProj = getRightChild( call );
            rightJoinChild = getProjectChild( call, rightProj, false );
        } else {
            rightProj = null;
            rightJoinChild = joinRel.getRight();
        }
        if ( (leftProj == null) && (rightProj == null) ) {
            return;
        }

        // Construct two RexPrograms and combine them.  The bottom program is a join of the projection expressions from the
        // left and/or right projects that feed into the join.  The top program contains the join condition.

        // Create a row type representing a concatenation of the inputs underneath the projects that feed into the join.
        // This is the input into the bottom RexProgram.  Note that the join type is an inner join because the inputs haven't
        // actually been joined yet.
        AlgDataType joinChildrenRowType =
                ValidatorUtil.deriveJoinRowType(
                        leftJoinChild.getTupleType(),
                        rightJoinChild.getTupleType(),
                        JoinAlgType.INNER,
                        joinRel.getCluster().getTypeFactory(),
                        null );

        // Create projection expressions, combining the projection expressions from the projects that feed into the join.
        // For the RHS projection expressions, shift them to the right by the number of fields on the LHS.
        // If the join input was not a projection, simply create references to the inputs.
        int nProjExprs = joinRel.getTupleType().getFieldCount();
        final List<Pair<RexNode, String>> projects = new ArrayList<>();
        final RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();

        createProjectExprs(
                leftProj,
                leftJoinChild,
                0,
                rexBuilder,
                joinChildrenRowType.getFields(),
                projects );

        List<AlgDataTypeField> leftFields = leftJoinChild.getTupleType().getFields();
        int nFieldsLeft = leftFields.size();
        createProjectExprs(
                rightProj,
                rightJoinChild,
                nFieldsLeft,
                rexBuilder,
                joinChildrenRowType.getFields(),
                projects );

        final List<AlgDataType> projTypes = new ArrayList<>();
        for ( int i = 0; i < nProjExprs; i++ ) {
            projTypes.add( projects.get( i ).left.getType() );
        }
        AlgDataType projRowType = rexBuilder.getTypeFactory().createStructType( null, projTypes, Pair.right( projects ) );

        // create the RexPrograms and merge them
        RexProgram bottomProgram =
                RexProgram.create(
                        joinChildrenRowType,
                        Pair.left( projects ),
                        null,
                        projRowType,
                        rexBuilder );
        RexProgramBuilder topProgramBuilder = new RexProgramBuilder( projRowType, rexBuilder );
        topProgramBuilder.addIdentity();
        topProgramBuilder.addCondition( joinRel.getCondition() );
        RexProgram topProgram = topProgramBuilder.getProgram();
        RexProgram mergedProgram = RexProgramBuilder.mergePrograms( topProgram, bottomProgram, rexBuilder );

        // Expand out the join condition and construct a new LogicalJoin that directly references the join children without the intervening ProjectRels
        RexNode newCondition = mergedProgram.expandLocalRef( mergedProgram.getCondition() );
        Join newJoinRel = joinRel.copy(
                joinRel.getTraitSet(),
                newCondition,
                leftJoinChild,
                rightJoinChild,
                joinRel.getJoinType(),
                joinRel.isSemiJoinDone() );

        // Expand out the new projection expressions; if the join is an outer join, modify the expressions to reference the join output
        final List<RexNode> newProjExprs = new ArrayList<>();
        List<RexLocalRef> projList = mergedProgram.getProjectList();
        List<AlgDataTypeField> newJoinFields = newJoinRel.getTupleType().getFields();
        int nJoinFields = newJoinFields.size();
        int[] adjustments = new int[nJoinFields];
        for ( int i = 0; i < nProjExprs; i++ ) {
            RexNode newExpr = mergedProgram.expandLocalRef( projList.get( i ) );
            if ( joinType != JoinAlgType.INNER ) {
                newExpr =
                        newExpr.accept(
                                new AlgOptUtil.RexInputConverter(
                                        rexBuilder,
                                        joinChildrenRowType.getFields(),
                                        newJoinFields,
                                        adjustments ) );
            }
            newProjExprs.add( newExpr );
        }

        // Finally, create the projection on top of the join
        final AlgBuilder algBuilder = call.builder();
        algBuilder.push( newJoinRel );
        algBuilder.project( newProjExprs, joinRel.getTupleType().getFieldNames() );
        // If the join was outer, we might need a cast after the projection to fix differences wrt nullability of fields
        if ( joinType != JoinAlgType.INNER ) {
            algBuilder.convert( joinRel.getTupleType(), false );
        }

        call.transformTo( algBuilder.build() );
    }


    /**
     * @param call RelOptRuleCall
     * @return true if the rule was invoked with a left project child
     */
    protected boolean hasLeftChild( AlgOptRuleCall call ) {
        return call.alg( 1 ) instanceof Project;
    }


    /**
     * @param call RelOptRuleCall
     * @return true if the rule was invoked with 2 children
     */
    protected boolean hasRightChild( AlgOptRuleCall call ) {
        return call.algs.length == 3;
    }


    /**
     * @param call RelOptRuleCall
     * @return LogicalProject corresponding to the right child
     */
    protected Project getRightChild( AlgOptRuleCall call ) {
        return call.alg( 2 );
    }


    /**
     * Returns the child of the project that will be used as input into the new LogicalJoin once the projects are
     * pulled above the LogicalJoin.
     *
     * @param call RelOptRuleCall
     * @param project project AlgNode
     * @param leftChild true if the project corresponds to the left projection
     * @return child of the project that will be used as input into the new LogicalJoin once the projects are pulled above the LogicalJoin
     */
    protected AlgNode getProjectChild( AlgOptRuleCall call, Project project, boolean leftChild ) {
        return project.getInput();
    }


    /**
     * Creates projection expressions corresponding to one of the inputs into the join
     *
     * @param projRel the projection input into the join (if it exists)
     * @param joinChild the child of the projection input (if there is a projection); otherwise, this is the join input
     * @param adjustmentAmount the amount the expressions need to be shifted by
     * @param rexBuilder rex builder
     * @param joinChildrenFields concatenation of the fields from the left and right join inputs (once the projections have been removed)
     * @param projects Projection expressions &amp; names to be created
     */
    protected void createProjectExprs( Project projRel, AlgNode joinChild, int adjustmentAmount, RexBuilder rexBuilder, List<AlgDataTypeField> joinChildrenFields, List<Pair<RexNode, String>> projects ) {
        List<AlgDataTypeField> childFields = joinChild.getTupleType().getFields();
        if ( projRel != null ) {
            List<Pair<RexNode, String>> namedProjects = projRel.getNamedProjects();
            int nChildFields = childFields.size();
            int[] adjustments = new int[nChildFields];
            for ( int i = 0; i < nChildFields; i++ ) {
                adjustments[i] = adjustmentAmount;
            }
            for ( Pair<RexNode, String> pair : namedProjects ) {
                RexNode e = pair.left;
                if ( adjustmentAmount != 0 ) {
                    // Shift the references by the adjustment amount
                    e = e.accept( new AlgOptUtil.RexInputConverter( rexBuilder, childFields, joinChildrenFields, adjustments ) );
                }
                projects.add( Pair.of( e, pair.right ) );
            }
        } else {
            // No projection; just create references to the inputs
            for ( int i = 0; i < childFields.size(); i++ ) {
                final AlgDataTypeField field = childFields.get( i );
                projects.add( Pair.of( (RexNode) rexBuilder.makeInputRef( field.getType(), i + adjustmentAmount ), field.getName() ) );
            }
        }
    }

}

