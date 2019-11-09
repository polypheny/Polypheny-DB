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
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.Permutation;
import java.util.List;


/**
 * ProjectMergeRule merges a {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Project} into another {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Project}, provided the projects aren't projecting identical sets of input references.
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

