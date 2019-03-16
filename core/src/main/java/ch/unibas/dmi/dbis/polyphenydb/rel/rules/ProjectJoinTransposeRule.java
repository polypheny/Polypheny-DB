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
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.SemiJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import java.util.ArrayList;
import java.util.List;


/**
 * Planner rule that pushes a {@link Project} past a {@link Join} by splitting the projection into a projection on top of each child of the join.
 */
public class ProjectJoinTransposeRule extends RelOptRule {

    public static final ProjectJoinTransposeRule INSTANCE = new ProjectJoinTransposeRule( expr -> true, RelFactories.LOGICAL_BUILDER );


    /**
     * Condition for expressions that should be preserved in the projection.
     */
    private final PushProjector.ExprCondition preserveExprCondition;


    /**
     * Creates a ProjectJoinTransposeRule with an explicit condition.
     *
     * @param preserveExprCondition Condition for expressions that should be preserved in the projection
     */
    public ProjectJoinTransposeRule( PushProjector.ExprCondition preserveExprCondition, RelBuilderFactory relFactory ) {
        super( operand( Project.class, operand( Join.class, any() ) ), relFactory, null );
        this.preserveExprCondition = preserveExprCondition;
    }


    // implement RelOptRule
    public void onMatch( RelOptRuleCall call ) {
        Project origProj = call.rel( 0 );
        final Join join = call.rel( 1 );

        if ( join instanceof SemiJoin ) {
            return; // TODO: support SemiJoin
        }
        // Locate all fields referenced in the projection and join condition; determine which inputs are referenced in the projection and join condition; if all fields are being referenced and there are no
        // special expressions, no point in proceeding any further
        PushProjector pushProject =
                new PushProjector(
                        origProj,
                        join.getCondition(),
                        join,
                        preserveExprCondition,
                        call.builder() );
        if ( pushProject.locateAllRefs() ) {
            return;
        }

        // Create left and right projections, projecting only those fields referenced on each side
        RelNode leftProjRel =
                pushProject.createProjectRefsAndExprs(
                        join.getLeft(),
                        true,
                        false );
        RelNode rightProjRel =
                pushProject.createProjectRefsAndExprs(
                        join.getRight(),
                        true,
                        true );

        // Convert the join condition to reference the projected columns
        RexNode newJoinFilter = null;
        int[] adjustments = pushProject.getAdjustments();
        if ( join.getCondition() != null ) {
            List<RelDataTypeField> projJoinFieldList = new ArrayList<>();
            projJoinFieldList.addAll( join.getSystemFieldList() );
            projJoinFieldList.addAll( leftProjRel.getRowType().getFieldList() );
            projJoinFieldList.addAll( rightProjRel.getRowType().getFieldList() );
            newJoinFilter = pushProject.convertRefsAndExprs( join.getCondition(), projJoinFieldList, adjustments );
        }

        // create a new join with the projected children
        Join newJoinRel =
                join.copy(
                        join.getTraitSet(),
                        newJoinFilter,
                        leftProjRel,
                        rightProjRel,
                        join.getJoinType(),
                        join.isSemiJoinDone() );

        // put the original project on top of the join, converting it to reference the modified projection list
        RelNode topProject = pushProject.createNewProject( newJoinRel, adjustments );

        call.transformTo( topProject );
    }
}

