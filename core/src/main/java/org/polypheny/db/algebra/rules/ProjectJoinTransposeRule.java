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
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes a {@link Project} past a {@link Join} by splitting the projection into a projection on top of each child of the join.
 */
public class ProjectJoinTransposeRule extends AlgOptRule {

    public static final ProjectJoinTransposeRule INSTANCE = new ProjectJoinTransposeRule( expr -> true, AlgFactories.LOGICAL_BUILDER );


    /**
     * Condition for expressions that should be preserved in the projection.
     */
    private final PushProjector.ExprCondition preserveExprCondition;


    /**
     * Creates a ProjectJoinTransposeRule with an explicit condition.
     *
     * @param preserveExprCondition Condition for expressions that should be preserved in the projection
     */
    public ProjectJoinTransposeRule( PushProjector.ExprCondition preserveExprCondition, AlgBuilderFactory algFactory ) {
        super( operand( Project.class, operand( Join.class, any() ) ), algFactory, null );
        this.preserveExprCondition = preserveExprCondition;
    }


    // implement RelOptRule
    @Override
    public void onMatch( AlgOptRuleCall call ) {
        Project origProj = call.alg( 0 );
        final Join join = call.alg( 1 );

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
        AlgNode leftProjAlg =
                pushProject.createProjectRefsAndExprs(
                        join.getLeft(),
                        true,
                        false );
        AlgNode rightProjAlg =
                pushProject.createProjectRefsAndExprs(
                        join.getRight(),
                        true,
                        true );

        // Convert the join condition to reference the projected columns
        RexNode newJoinFilter = null;
        int[] adjustments = pushProject.getAdjustments();
        if ( join.getCondition() != null ) {
            List<AlgDataTypeField> projJoinFieldList = new ArrayList<>();
            projJoinFieldList.addAll( leftProjAlg.getTupleType().getFields() );
            projJoinFieldList.addAll( rightProjAlg.getTupleType().getFields() );
            newJoinFilter = pushProject.convertRefsAndExprs( join.getCondition(), projJoinFieldList, adjustments );
        }

        // create a new join with the projected children
        Join newJoinRel =
                join.copy(
                        join.getTraitSet(),
                        newJoinFilter,
                        leftProjAlg,
                        rightProjAlg,
                        join.getJoinType(),
                        join.isSemiJoinDone() );

        // put the original project on top of the join, converting it to reference the modified projection list
        AlgNode topProject = pushProject.createNewProject( newJoinRel, adjustments );

        call.transformTo( topProject );
    }

}

