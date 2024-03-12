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


import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.Permutation;


/**
 * ProjectMergeRule merges a {@link org.polypheny.db.algebra.core.Project} into another {@link org.polypheny.db.algebra.core.Project}, provided the projects aren't projecting identical sets of input references.
 */
public class ProjectMergeRule extends AlgOptRule {

    public static final ProjectMergeRule INSTANCE = new ProjectMergeRule( true, AlgFactories.LOGICAL_BUILDER );


    /**
     * Whether to always merge projects.
     */
    private final boolean force;


    /**
     * Creates a ProjectMergeRule, specifying whether to always merge projects.
     *
     * @param force Whether to always merge projects
     */
    public ProjectMergeRule( boolean force, AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( Project.class, operand( Project.class, any() ) ),
                algBuilderFactory,
                "ProjectMergeRule" + (force ? ":force_mode" : "") );
        this.force = force;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Project topProject = call.alg( 0 );
        final Project bottomProject = call.alg( 1 );
        final AlgBuilder algBuilder = call.builder();

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
                algBuilder.push( bottomProject.getInput() );
                algBuilder.project( algBuilder.fields( product ), topProject.getTupleType().getFieldNames() );
                call.transformTo( algBuilder.build() );
                return;
            }
        }

        // If we're not in force mode and the two projects reference identical inputs, then return and let ProjectRemoveRule replace the projects.
        if ( !force ) {
            if ( RexUtil.isIdentity( topProject.getProjects(), topProject.getInput().getTupleType() ) ) {
                return;
            }
        }

        final List<RexNode> newProjects = AlgOptUtil.pushPastProject( topProject.getProjects(), bottomProject );
        final AlgNode input = bottomProject.getInput();
        if ( RexUtil.isIdentity( newProjects, input.getTupleType() ) ) {
            if ( force || input.getTupleType().getFieldNames().equals( topProject.getTupleType().getFieldNames() ) ) {
                call.transformTo( input );
                return;
            }
        }

        // replace the two projects with a combined projection
        algBuilder.push( bottomProject.getInput() );
        algBuilder.project( newProjects, topProject.getTupleType().getFieldNames() );
        call.transformTo( algBuilder.build() );
    }

}

