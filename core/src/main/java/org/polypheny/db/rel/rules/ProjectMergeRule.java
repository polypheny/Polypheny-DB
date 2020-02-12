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
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.Permutation;
import java.util.List;


/**
 * ProjectMergeRule merges a {@link org.polypheny.db.rel.core.Project} into another {@link org.polypheny.db.rel.core.Project}, provided the projects aren't projecting identical sets of input references.
 */
public class ProjectMergeRule extends RelOptRule {

    public static final ProjectMergeRule INSTANCE = new ProjectMergeRule( true, RelFactories.LOGICAL_BUILDER );


    /**
     * Whether to always merge projects.
     */
    private final boolean force;


    /**
     * Creates a ProjectMergeRule, specifying whether to always merge projects.
     *
     * @param force Whether to always merge projects
     */
    public ProjectMergeRule( boolean force, RelBuilderFactory relBuilderFactory ) {
        super(
                operand( Project.class, operand( Project.class, any() ) ),
                relBuilderFactory,
                "ProjectMergeRule" + (force ? ":force_mode" : "") );
        this.force = force;
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Project topProject = call.rel( 0 );
        final Project bottomProject = call.rel( 1 );
        final RelBuilder relBuilder = call.builder();

        // If one or both projects are permutations, short-circuit the complex logic of building a RexProgram.
        final Permutation topPermutation = topProject.getPermutation();
        if ( topPermutation != null ) {
            if ( topPermutation.isIdentity() ) {
                // Let ProjectRemoveRule handle this.
                return;
            }
            final Permutation bottomPermutation = bottomProject.getPermutation();
            if ( bottomPermutation != null ) {
                if ( bottomPermutation.isIdentity() ) {
                    // Let ProjectRemoveRule handle this.
                    return;
                }
                final Permutation product = topPermutation.product( bottomPermutation );
                relBuilder.push( bottomProject.getInput() );
                relBuilder.project( relBuilder.fields( product ), topProject.getRowType().getFieldNames() );
                call.transformTo( relBuilder.build() );
                return;
            }
        }

        // If we're not in force mode and the two projects reference identical inputs, then return and let ProjectRemoveRule replace the projects.
        if ( !force ) {
            if ( RexUtil.isIdentity( topProject.getProjects(), topProject.getInput().getRowType() ) ) {
                return;
            }
        }

        final List<RexNode> newProjects = RelOptUtil.pushPastProject( topProject.getProjects(), bottomProject );
        final RelNode input = bottomProject.getInput();
        if ( RexUtil.isIdentity( newProjects, input.getRowType() ) ) {
            if ( force || input.getRowType().getFieldNames().equals( topProject.getRowType().getFieldNames() ) ) {
                call.transformTo( input );
                return;
            }
        }

        // replace the two projects with a combined projection
        relBuilder.push( bottomProject.getInput() );
        relBuilder.project( newProjects, topProject.getRowType().getFieldNames() );
        call.transformTo( relBuilder.build() );
    }
}

