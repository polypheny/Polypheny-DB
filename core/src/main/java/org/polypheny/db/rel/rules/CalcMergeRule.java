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

