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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalCalc;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexOver;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;


/**
 * Planner rule which merges a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject} and a {@link LogicalCalc}.
 *
 * The resulting {@link LogicalCalc} has the same project list as the original {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject}, but expressed in terms
 * of the original {@link LogicalCalc}'s inputs.
 *
 * @see FilterCalcMergeRule
 */
public class ProjectCalcMergeRule extends RelOptRule {

    public static final ProjectCalcMergeRule INSTANCE = new ProjectCalcMergeRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a ProjectCalcMergeRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public ProjectCalcMergeRule( RelBuilderFactory relBuilderFactory ) {
        super(
                operand(
                        LogicalProject.class,
                        operand( LogicalCalc.class, any() ) ),
                relBuilderFactory,
                null );
    }


    public void onMatch( RelOptRuleCall call ) {
        final LogicalProject project = call.rel( 0 );
        final LogicalCalc calc = call.rel( 1 );

        // Don't merge a project which contains windowed aggregates onto a calc. That would effectively be pushing a windowed aggregate down
        // through a filter. Transform the project into an identical calc, which we'll have chance to merge later, after the over is expanded.
        final RelOptCluster cluster = project.getCluster();
        RexProgram program =
                RexProgram.create(
                        calc.getRowType(),
                        project.getProjects(),
                        null,
                        project.getRowType(),
                        cluster.getRexBuilder() );
        if ( RexOver.containsOver( program ) ) {
            LogicalCalc projectAsCalc = LogicalCalc.create( calc, program );
            call.transformTo( projectAsCalc );
            return;
        }

        // Create a program containing the project node's expressions.
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final RexProgramBuilder progBuilder = new RexProgramBuilder( calc.getRowType(), rexBuilder );
        for ( Pair<RexNode, String> field : project.getNamedProjects() ) {
            progBuilder.addProject( field.left, field.right );
        }
        RexProgram topProgram = progBuilder.getProgram();
        RexProgram bottomProgram = calc.getProgram();

        // Merge the programs together.
        RexProgram mergedProgram = RexProgramBuilder.mergePrograms( topProgram, bottomProgram, rexBuilder );
        final LogicalCalc newCalc = LogicalCalc.create( calc.getInput(), mergedProgram );
        call.transformTo( newCalc );
    }
}

