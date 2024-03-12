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


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexOver;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes a {@link org.polypheny.db.algebra.core.Project} past a {@link org.polypheny.db.algebra.core.Filter}.
 */
public class ProjectFilterTransposeRule extends AlgOptRule {

    public static final ProjectFilterTransposeRule INSTANCE = new ProjectFilterTransposeRule( LogicalRelProject.class, LogicalRelFilter.class, AlgFactories.LOGICAL_BUILDER, expr -> false );

    /**
     * Expressions that should be preserved in the projection
     */
    private final PushProjector.ExprCondition preserveExprCondition;


    /**
     * Creates a ProjectFilterTransposeRule.
     *
     * @param preserveExprCondition Condition for expressions that should be preserved in the projection
     */
    public ProjectFilterTransposeRule( Class<? extends Project> projectClass, Class<? extends Filter> filterClass, AlgBuilderFactory algBuilderFactory, PushProjector.ExprCondition preserveExprCondition ) {
        this( operand( projectClass, operand( filterClass, any() ) ), preserveExprCondition, algBuilderFactory );
    }


    protected ProjectFilterTransposeRule( AlgOptRuleOperand operand, PushProjector.ExprCondition preserveExprCondition, AlgBuilderFactory algBuilderFactory ) {
        super( operand, algBuilderFactory, null );
        this.preserveExprCondition = preserveExprCondition;
    }


    // implement RelOptRule
    @Override
    public void onMatch( AlgOptRuleCall call ) {
        Project origProj;
        Filter filter;
        if ( call.algs.length >= 2 ) {
            origProj = call.alg( 0 );
            filter = call.alg( 1 );
        } else {
            origProj = null;
            filter = call.alg( 0 );
        }
        AlgNode alg = filter.getInput();
        RexNode origFilter = filter.getCondition();

        if ( (origProj != null) && RexOver.containsOver( origProj.getProjects(), null ) ) {
            // Cannot push project through filter if project contains a windowed aggregate -- it will affect row counts. Abort this rule invocation; pushdown will be considered after the windowed
            // aggregate has been implemented. It's OK if the filter contains a windowed aggregate.
            return;
        }

        PushProjector pushProjector = new PushProjector( origProj, origFilter, alg, preserveExprCondition, call.builder() );
        AlgNode topProject = pushProjector.convertProject( null );

        if ( topProject != null ) {
            call.transformTo( topProject );
        }
    }

}

