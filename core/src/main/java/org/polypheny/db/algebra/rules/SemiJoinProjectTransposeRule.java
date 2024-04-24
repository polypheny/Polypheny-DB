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


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.ValidatorUtil;


/**
 * Planner rule that pushes
 * a {@link org.polypheny.db.algebra.core.SemiJoin} down in a tree past
 * a {@link org.polypheny.db.algebra.core.Project}.
 *
 * The intention is to trigger other rules that will convert {@code SemiJoin}s.
 *
 * SemiJoin(LogicalProject(X), Y) &rarr; LogicalProject(SemiJoin(X, Y))
 *
 * @see org.polypheny.db.algebra.rules.SemiJoinFilterTransposeRule
 */
public class SemiJoinProjectTransposeRule extends AlgOptRule {

    public static final SemiJoinProjectTransposeRule INSTANCE = new SemiJoinProjectTransposeRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a SemiJoinProjectTransposeRule.
     */
    private SemiJoinProjectTransposeRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( SemiJoin.class, some( operand( LogicalRelProject.class, any() ) ) ),
                algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        SemiJoin semiJoin = call.alg( 0 );
        LogicalRelProject project = call.alg( 1 );

        // Convert the LHS semi-join keys to reference the child projection expression; all projection expressions must be RexInputRefs, otherwise, we wouldn't have created this semi-join.
        final List<Integer> newLeftKeys = new ArrayList<>();
        final List<Integer> leftKeys = semiJoin.getLeftKeys();
        final List<RexNode> projExprs = project.getProjects();
        for ( int leftKey : leftKeys ) {
            RexIndexRef inputRef = (RexIndexRef) projExprs.get( leftKey );
            newLeftKeys.add( inputRef.getIndex() );
        }

        // convert the semijoin condition to reflect the LHS with the project pulled up
        RexNode newCondition = adjustCondition( project, semiJoin );

        SemiJoin newSemiJoin = SemiJoin.create( project.getInput(), semiJoin.getRight(), newCondition, ImmutableList.copyOf( newLeftKeys ), semiJoin.getRightKeys() );

        // Create the new projection.  Note that the projection expressions are the same as the original because they only reference the LHS of the semijoin and the semijoin only projects out the LHS
        final AlgBuilder algBuilder = call.builder();
        algBuilder.push( newSemiJoin );
        algBuilder.project( projExprs, project.getTupleType().getFieldNames() );

        call.transformTo( algBuilder.build() );
    }


    /**
     * Pulls the project above the semijoin and returns the resulting semijoin condition. As a result, the semijoin condition should be modified such that references to the LHS of a semijoin should now reference the
     * children of the project that's on the LHS.
     *
     * @param project LogicalProject on the LHS of the semijoin
     * @param semiJoin the semijoin
     * @return the modified semijoin condition
     */
    private RexNode adjustCondition( LogicalRelProject project, SemiJoin semiJoin ) {
        // create two RexPrograms -- the bottom one representing a concatenation of the project and the RHS of the semijoin and the top one representing the semijoin condition

        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        AlgNode rightChild = semiJoin.getRight();

        // for the bottom RexProgram, the input is a concatenation of the child of the project and the RHS of the semijoin
        AlgDataType bottomInputRowType =
                ValidatorUtil.deriveJoinRowType(
                        project.getInput().getTupleType(),
                        rightChild.getTupleType(),
                        JoinAlgType.INNER,
                        typeFactory,
                        null );
        RexProgramBuilder bottomProgramBuilder = new RexProgramBuilder( bottomInputRowType, rexBuilder );

        // add the project expressions, then add input references for the RHS of the semijoin
        for ( Pair<RexNode, String> pair : project.getNamedProjects() ) {
            bottomProgramBuilder.addProject( pair.left, pair.right );
        }
        int nLeftFields = project.getInput().getTupleType().getFieldCount();
        List<AlgDataTypeField> rightFields = rightChild.getTupleType().getFields();
        int nRightFields = rightFields.size();
        for ( int i = 0; i < nRightFields; i++ ) {
            final AlgDataTypeField field = rightFields.get( i );
            RexNode inputRef = rexBuilder.makeInputRef( field.getType(), i + nLeftFields );
            bottomProgramBuilder.addProject( inputRef, field.getName() );
        }
        RexProgram bottomProgram = bottomProgramBuilder.getProgram();

        // input rowtype into the top program is the concatenation of the project and the RHS of the semijoin
        AlgDataType topInputRowType =
                ValidatorUtil.deriveJoinRowType(
                        project.getTupleType(),
                        rightChild.getTupleType(),
                        JoinAlgType.INNER,
                        typeFactory,
                        null );
        RexProgramBuilder topProgramBuilder = new RexProgramBuilder( topInputRowType, rexBuilder );
        topProgramBuilder.addIdentity();
        topProgramBuilder.addCondition( semiJoin.getCondition() );
        RexProgram topProgram = topProgramBuilder.getProgram();

        // merge the programs and expand out the local references to form the new semijoin condition; it now references a concatenation of the project's child and the RHS of the semijoin
        RexProgram mergedProgram = RexProgramBuilder.mergePrograms( topProgram, bottomProgram, rexBuilder );

        return mergedProgram.expandLocalRef( mergedProgram.getCondition() );
    }

}

