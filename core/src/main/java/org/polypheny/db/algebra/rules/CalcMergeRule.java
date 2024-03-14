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


import java.util.Objects;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexOver;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that merges a {@link LogicalCalc} onto a {@link LogicalCalc}.
 *
 * The resulting {@link LogicalCalc} has the same project list as the upper
 * {@link LogicalCalc}, but expressed in terms of the lower {@link LogicalCalc}'s inputs.
 */
public class CalcMergeRule extends AlgOptRule {

    public static final CalcMergeRule INSTANCE = new CalcMergeRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a CalcMergeRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public CalcMergeRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( Calc.class, operand( Calc.class, any() ) ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Calc topCalc = call.alg( 0 );
        final Calc bottomCalc = call.alg( 1 );

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
        assert Objects.equals( mergedProgram.getOutputRowType(), topProgram.getOutputRowType() );
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

