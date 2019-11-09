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
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Planner rule that, given a {@link Project} node that merely returns its input, converts the node into its child.
 *
 * For example,
 * <code>Project(ArrayReader(a), {$input0})</code>
 * becomes
 * <code>ArrayReader(a)</code>.</p>
 *
 * @see CalcRemoveRule
 * @see ProjectMergeRule
 */
public class ProjectRemoveRule extends RelOptRule {

    public static final ProjectRemoveRule INSTANCE = new ProjectRemoveRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a ProjectRemoveRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public ProjectRemoveRule( RelBuilderFactory relBuilderFactory ) {
        // Create a specialized operand to detect non-matches early. This keeps the rule queue short.
        super( operandJ( Project.class, null, ProjectRemoveRule::isTrivial, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        Project project = call.rel( 0 );
        assert isTrivial( project );
        RelNode stripped = project.getInput();
        if ( stripped instanceof Project ) {
            // Rename columns of child projection if desired field names are given.
            Project childProject = (Project) stripped;
            stripped = childProject.copy(
                    childProject.getTraitSet(),
                    childProject.getInput(),
                    childProject.getProjects(),
                    project.getRowType() );
        }
        RelNode child = call.getPlanner().register( stripped, project );
        call.transformTo( child );
    }


    /**
     * Returns the child of a project if the project is trivial, otherwise the project itself.
     */
    public static RelNode strip( Project project ) {
        return isTrivial( project ) ? project.getInput() : project;
    }


    public static boolean isTrivial( Project project ) {
        return RexUtil.isIdentity( project.getProjects(), project.getInput().getRowType() );
    }

}

