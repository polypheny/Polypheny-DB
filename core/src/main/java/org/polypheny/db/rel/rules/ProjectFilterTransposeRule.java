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


import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelOptRuleOperand;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexOver;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Planner rule that pushes a {@link org.polypheny.db.rel.core.Project} past a {@link org.polypheny.db.rel.core.Filter}.
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
    @Override
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

