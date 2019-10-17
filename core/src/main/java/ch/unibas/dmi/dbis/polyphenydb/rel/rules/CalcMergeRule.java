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
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Calc;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalCalc;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexOver;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Planner rule that merges a {@link LogicalCalc} onto a {@link LogicalCalc}.
 *
 * The resulting {@link LogicalCalc} has the same project list as the upper
 * {@link LogicalCalc}, but expressed in terms of the lower {@link LogicalCalc}'s inputs.
 */
public class CalcMergeRule extends RelOptRule {

    public static final CalcMergeRule INSTANCE = new CalcMergeRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a CalcMergeRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public CalcMergeRule( RelBuilderFactory relBuilderFactory ) {
        super( operand( Calc.class, operand( Calc.class, any() ) ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Calc topCalc = call.rel( 0 );
        final Calc bottomCalc = call.rel( 1 );

        // Don't merge a calc which contains windowed aggregates onto a calc. That would effectively be pushing a windowed aggregate down through a filter.
        RexProgram topProgram = topCalc.getProgram();
        if ( RexOver.containsOver( topProgram ) ) {
            return;
        }

        // Merge the programs together.
        RexProgram mergedProgram =
                RexProgramBuilder.mergePrograms(
                        topCalc.getProgram(),
                        bottomCalc.getProgram(),
                        topCalc.getCluster().getRexBuilder() );
        assert mergedProgram.getOutputRowType() == topProgram.getOutputRowType();
        final Calc newCalc =
                topCalc.copy(
                        topCalc.getTraitSet(),
                        bottomCalc.getInput(),
                        mergedProgram );

        if ( newCalc.getDigest().equals( bottomCalc.getDigest() ) ) {
            // newCalc is equivalent to bottomCalc, which means that topCalc must be trivial. Take it out of the game.
            call.getPlanner().setImportance( topCalc, 0.0 );
        }

        call.transformTo( newCalc );
    }
}

