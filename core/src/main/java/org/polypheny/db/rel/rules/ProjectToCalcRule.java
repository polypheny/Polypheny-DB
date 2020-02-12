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
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.logical.LogicalCalc;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Rule to convert a
 * {@link org.polypheny.db.rel.logical.LogicalProject} to a
 * {@link LogicalCalc}
 *
 * The rule does not fire if the child is a
 * {@link org.polypheny.db.rel.logical.LogicalProject},
 * {@link LogicalFilter} or
 * {@link LogicalCalc}. If it did, then the same
 * {@link LogicalCalc} would be formed via
 * several transformation paths, which is a waste of effort.
 *
 * @see FilterToCalcRule
 */
public class ProjectToCalcRule extends RelOptRule {

    public static final ProjectToCalcRule INSTANCE = new ProjectToCalcRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a ProjectToCalcRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public ProjectToCalcRule( RelBuilderFactory relBuilderFactory ) {
        super( operand( LogicalProject.class, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final LogicalProject project = call.rel( 0 );
        final RelNode input = project.getInput();
        final RexProgram program =
                RexProgram.create(
                        input.getRowType(),
                        project.getProjects(),
                        null,
                        project.getRowType(),
                        project.getCluster().getRexBuilder() );
        final LogicalCalc calc = LogicalCalc.create( input, program );
        call.transformTo( calc );
    }
}

