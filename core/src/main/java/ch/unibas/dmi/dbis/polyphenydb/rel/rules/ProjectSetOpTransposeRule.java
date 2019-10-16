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
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.SetOp;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import java.util.ArrayList;
import java.util.List;


/**
 * Planner rule that pushes a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject} past a {@link SetOp}.
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

