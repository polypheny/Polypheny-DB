/*
 * Copyright 2019-2022 The Polypheny Project
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
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Rule to convert a
 * {@link LogicalProject} to a
 * {@link LogicalCalc}
 *
 * The rule does not fire if the child is a
 * {@link LogicalProject},
 * {@link LogicalFilter} or
 * {@link LogicalCalc}. If it did, then the same
 * {@link LogicalCalc} would be formed via
 * several transformation paths, which is a waste of effort.
 *
 * @see FilterToCalcRule
 */
public class ProjectToCalcRule extends AlgOptRule {

    public static final ProjectToCalcRule INSTANCE = new ProjectToCalcRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a ProjectToCalcRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public ProjectToCalcRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( LogicalProject.class, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalProject project = call.alg( 0 );
        final AlgNode input = project.getInput();
        final LogicalCalc calc = from( project, input );
        call.transformTo( calc );
    }


    public static LogicalCalc from( LogicalProject project, AlgNode input ) {
        final RexProgram program =
                RexProgram.create(
                        input.getRowType(),
                        project.getProjects(),
                        null,
                        project.getRowType(),
                        project.getCluster().getRexBuilder() );
        return LogicalCalc.create( input, program );
    }

}

