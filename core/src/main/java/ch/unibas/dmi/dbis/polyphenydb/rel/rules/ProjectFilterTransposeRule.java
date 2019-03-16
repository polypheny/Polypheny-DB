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
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexOver;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Planner rule that pushes a {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Project} past a {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter}.
 */
public class ProjectFilterTransposeRule extends RelOptRule {

    public static final ProjectFilterTransposeRule INSTANCE = new ProjectFilterTransposeRule( LogicalProject.class, LogicalFilter.class, RelFactories.LOGICAL_BUILDER, expr -> false );

    /**
     * Expressions that should be preserved in the projection
     */
    private final PushProjector.ExprCondition preserveExprCondition;


    /**
     * Creates a ProjectFilterTransposeRule.
     *
     * @param preserveExprCondition Condition for expressions that should be preserved in the projection
     */
    public ProjectFilterTransposeRule( Class<? extends Project> projectClass, Class<? extends Filter> filterClass, RelBuilderFactory relBuilderFactory, PushProjector.ExprCondition preserveExprCondition ) {
        this( operand( projectClass, operand( filterClass, any() ) ), preserveExprCondition, relBuilderFactory );
    }


    protected ProjectFilterTransposeRule( RelOptRuleOperand operand, PushProjector.ExprCondition preserveExprCondition, RelBuilderFactory relBuilderFactory ) {
        super( operand, relBuilderFactory, null );
        this.preserveExprCondition = preserveExprCondition;
    }


    // implement RelOptRule
    public void onMatch( RelOptRuleCall call ) {
        Project origProj;
        Filter filter;
        if ( call.rels.length >= 2 ) {
            origProj = call.rel( 0 );
            filter = call.rel( 1 );
        } else {
            origProj = null;
            filter = call.rel( 0 );
        }
        RelNode rel = filter.getInput();
        RexNode origFilter = filter.getCondition();

        if ( (origProj != null) && RexOver.containsOver( origProj.getProjects(), null ) ) {
            // Cannot push project through filter if project contains a windowed aggregate -- it will affect row counts. Abort this rule invocation; pushdown will be considered after the windowed
            // aggregate has been implemented. It's OK if the filter contains a windowed aggregate.
            return;
        }

        PushProjector pushProjector = new PushProjector( origProj, origFilter, rel, preserveExprCondition, call.builder() );
        RelNode topProject = pushProjector.convertProject( null );

        if ( topProject != null ) {
            call.transformTo( topProject );
        }
    }
}

