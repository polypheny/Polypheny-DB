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


import com.google.common.collect.ImmutableList;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelOptRuleOperand;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Planner rule that pushes a {@link org.polypheny.db.rel.core.Project} past a {@link Sort}.
 *
 * @see org.polypheny.db.rel.rules.SortProjectTransposeRule
 */
public class ProjectSortTransposeRule extends RelOptRule {

    public static final ProjectSortTransposeRule INSTANCE = new ProjectSortTransposeRule( Project.class, Sort.class, RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a ProjectSortTransposeRule.
     */
    private ProjectSortTransposeRule( Class<Project> projectClass, Class<Sort> sortClass, RelBuilderFactory relBuilderFactory ) {
        this(
                operand( projectClass, operand( sortClass, any() ) ),
                relBuilderFactory, null );
    }


    /**
     * Creates a ProjectSortTransposeRule with an operand.
     */
    protected ProjectSortTransposeRule( RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description ) {
        super( operand, relBuilderFactory, description );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Project project = call.rel( 0 );
        final Sort sort = call.rel( 1 );
        if ( sort.getClass() != Sort.class ) {
            return;
        }
        RelNode newProject = project.copy( project.getTraitSet(), ImmutableList.of( sort.getInput() ) );
        final Sort newSort =
                sort.copy(
                        sort.getTraitSet(),
                        newProject,
                        sort.getCollation(),
                        sort.offset,
                        sort.fetch );
        call.transformTo( newSort );
    }
}

