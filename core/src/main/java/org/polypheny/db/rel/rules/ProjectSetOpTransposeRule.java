/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.rel.rules;


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.SetOp;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Planner rule that pushes a {@link org.polypheny.db.rel.logical.LogicalProject} past a {@link SetOp}.
 *
 * The children of the {@code SetOp} will project only the {@link RexInputRef}s referenced in the original {@code LogicalProject}.
 */
public class ProjectSetOpTransposeRule extends RelOptRule {

    public static final ProjectSetOpTransposeRule INSTANCE = new ProjectSetOpTransposeRule( expr -> false, RelFactories.LOGICAL_BUILDER );


    /**
     * Expressions that should be preserved in the projection
     */
    private PushProjector.ExprCondition preserveExprCondition;


    /**
     * Creates a ProjectSetOpTransposeRule with an explicit condition whether to preserve expressions.
     *
     * @param preserveExprCondition Condition whether to preserve expressions
     */
    public ProjectSetOpTransposeRule( PushProjector.ExprCondition preserveExprCondition, RelBuilderFactory relBuilderFactory ) {
        super(
                operand( LogicalProject.class, operand( SetOp.class, any() ) ),
                relBuilderFactory, null );
        this.preserveExprCondition = preserveExprCondition;
    }


    // implement RelOptRule
    @Override
    public void onMatch( RelOptRuleCall call ) {
        LogicalProject origProj = call.rel( 0 );
        SetOp setOp = call.rel( 1 );

        // cannot push project past a distinct
        if ( !setOp.all ) {
            return;
        }

        // locate all fields referenced in the projection
        PushProjector pushProject = new PushProjector( origProj, null, setOp, preserveExprCondition, call.builder() );
        pushProject.locateAllRefs();

        List<RelNode> newSetOpInputs = new ArrayList<>();
        int[] adjustments = pushProject.getAdjustments();

        // Push the projects completely below the setop; this is different from pushing below a join, where we decompose to try to keep expensive expressions above the join,
        // because UNION ALL does not have any filtering effect, and it is the only operator this rule currently acts on
        for ( RelNode input : setOp.getInputs() ) {
            // Be lazy:  produce two ProjectRels, and let another rule merge them (could probably just clone origProj instead?)
            Project p = pushProject.createProjectRefsAndExprs( input, true, false );
            newSetOpInputs.add( pushProject.createNewProject( p, adjustments ) );
        }

        // Create a new setop whose children are the ProjectRels created above
        SetOp newSetOp = setOp.copy( setOp.getTraitSet(), newSetOpInputs );

        call.transformTo( newSetOp );
    }
}

