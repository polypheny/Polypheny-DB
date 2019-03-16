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
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.SemiJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorUtil;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import java.util.ArrayList;
import java.util.List;


/**
 * Planner rule that pushes
 * a {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.SemiJoin} down in a tree past
 * a {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Project}.
 *
 * The intention is to trigger other rules that will convert {@code SemiJoin}s.
 *
 * SemiJoin(LogicalProject(X), Y) &rarr; LogicalProject(SemiJoin(X, Y))
 *
 * @see ch.unibas.dmi.dbis.polyphenydb.rel.rules.SemiJoinFilterTransposeRule
 */
public class SemiJoinProjectTransposeRule extends RelOptRule {

    public static final SemiJoinProjectTransposeRule INSTANCE = new SemiJoinProjectTransposeRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a SemiJoinProjectTransposeRule.
     */
    private SemiJoinProjectTransposeRule( RelBuilderFactory relBuilderFactory ) {
        super(
                operand( SemiJoin.class, some( operand( LogicalProject.class, any() ) ) ),
                relBuilderFactory, null );
    }


    public void onMatch( RelOptRuleCall call ) {
        SemiJoin semiJoin = call.rel( 0 );
        LogicalProject project = call.rel( 1 );

        // Convert the LHS semi-join keys to reference the child projection expression; all projection expressions must be RexInputRefs, otherwise, we wouldn't have created this semi-join.
        final List<Integer> newLeftKeys = new ArrayList<>();
        final List<Integer> leftKeys = semiJoin.getLeftKeys();
        final List<RexNode> projExprs = project.getProjects();
        for ( int leftKey : leftKeys ) {
            RexInputRef inputRef = (RexInputRef) projExprs.get( leftKey );
            newLeftKeys.add( inputRef.getIndex() );
        }

        // convert the semijoin condition to reflect the LHS with the project pulled up
        RexNode newCondition = adjustCondition( project, semiJoin );

        SemiJoin newSemiJoin = SemiJoin.create( project.getInput(), semiJoin.getRight(), newCondition, ImmutableIntList.copyOf( newLeftKeys ), semiJoin.getRightKeys() );

        // Create the new projection.  Note that the projection expressions are the same as the original because they only reference the LHS of the semijoin and the semijoin only projects out the LHS
        final RelBuilder relBuilder = call.builder();
        relBuilder.push( newSemiJoin );
        relBuilder.project( projExprs, project.getRowType().getFieldNames() );

        call.transformTo( relBuilder.build() );
    }


    /**
     * Pulls the project above the semijoin and returns the resulting semijoin condition. As a result, the semijoin condition should be modified such that references to the LHS of a semijoin should now reference the
     * children of the project that's on the LHS.
     *
     * @param project LogicalProject on the LHS of the semijoin
     * @param semiJoin the semijoin
     * @return the modified semijoin condition
     */
    private RexNode adjustCondition( LogicalProject project, SemiJoin semiJoin ) {
        // create two RexPrograms -- the bottom one representing a concatenation of the project and the RHS of the semijoin and the top one representing the semijoin condition

        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelNode rightChild = semiJoin.getRight();

        // for the bottom RexProgram, the input is a concatenation of the child of the project and the RHS of the semijoin
        RelDataType bottomInputRowType =
                SqlValidatorUtil.deriveJoinRowType(
                        project.getInput().getRowType(),
                        rightChild.getRowType(),
                        JoinRelType.INNER,
                        typeFactory,
                        null,
                        semiJoin.getSystemFieldList() );
        RexProgramBuilder bottomProgramBuilder = new RexProgramBuilder( bottomInputRowType, rexBuilder );

        // add the project expressions, then add input references for the RHS of the semijoin
        for ( Pair<RexNode, String> pair : project.getNamedProjects() ) {
            bottomProgramBuilder.addProject( pair.left, pair.right );
        }
        int nLeftFields = project.getInput().getRowType().getFieldCount();
        List<RelDataTypeField> rightFields = rightChild.getRowType().getFieldList();
        int nRightFields = rightFields.size();
        for ( int i = 0; i < nRightFields; i++ ) {
            final RelDataTypeField field = rightFields.get( i );
            RexNode inputRef = rexBuilder.makeInputRef( field.getType(), i + nLeftFields );
            bottomProgramBuilder.addProject( inputRef, field.getName() );
        }
        RexProgram bottomProgram = bottomProgramBuilder.getProgram();

        // input rowtype into the top program is the concatenation of the project and the RHS of the semijoin
        RelDataType topInputRowType =
                SqlValidatorUtil.deriveJoinRowType(
                        project.getRowType(),
                        rightChild.getRowType(),
                        JoinRelType.INNER,
                        typeFactory,
                        null,
                        semiJoin.getSystemFieldList() );
        RexProgramBuilder topProgramBuilder = new RexProgramBuilder( topInputRowType, rexBuilder );
        topProgramBuilder.addIdentity();
        topProgramBuilder.addCondition( semiJoin.getCondition() );
        RexProgram topProgram = topProgramBuilder.getProgram();

        // merge the programs and expand out the local references to form the new semijoin condition; it now references a concatenation of the project's child and the RHS of the semijoin
        RexProgram mergedProgram = RexProgramBuilder.mergePrograms( topProgram, bottomProgram, rexBuilder );

        return mergedProgram.expandLocalRef( mergedProgram.getCondition() );
    }
}

