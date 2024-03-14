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


import com.google.common.collect.ImmutableList;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes a {@link org.polypheny.db.algebra.core.Project} past a {@link Sort}.
 *
 * @see org.polypheny.db.algebra.rules.SortProjectTransposeRule
 */
public class ProjectSortTransposeRule extends AlgOptRule {

    public static final ProjectSortTransposeRule INSTANCE = new ProjectSortTransposeRule( Project.class, Sort.class, AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a ProjectSortTransposeRule.
     */
    private ProjectSortTransposeRule( Class<Project> projectClass, Class<Sort> sortClass, AlgBuilderFactory algBuilderFactory ) {
        this(
                operand( projectClass, operand( sortClass, any() ) ),
                algBuilderFactory, null );
    }


    /**
     * Creates a ProjectSortTransposeRule with an operand.
     */
    protected ProjectSortTransposeRule( AlgOptRuleOperand operand, AlgBuilderFactory algBuilderFactory, String description ) {
        super( operand, algBuilderFactory, description );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Project project = call.alg( 0 );
        final Sort sort = call.alg( 1 );
        if ( sort.getClass() != Sort.class ) {
            return;
        }
        AlgNode newProject = project.copy( project.getTraitSet(), ImmutableList.of( sort.getInput() ) );
        final Sort newSort =
                sort.copy(
                        sort.getTraitSet(),
                        newProject,
                        sort.getCollation(),
                        null,
                        sort.offset,
                        sort.fetch );
        call.transformTo( newSort );
    }

}

