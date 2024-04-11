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


import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that merges a {@link LogicalRelFilter} and a {@link LogicalCalc}. The result is a {@link LogicalCalc}
 * whose filter condition is the logical AND of the two.
 *
 * @see FilterMergeRule
 */
public class FilterCalcMergeRule extends AlgOptRule {

    public static final FilterCalcMergeRule INSTANCE = new FilterCalcMergeRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a FilterCalcMergeRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public FilterCalcMergeRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( Filter.class, operand( LogicalCalc.class, any() ) ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalRelFilter filter = call.alg( 0 );
        final LogicalCalc calc = call.alg( 1 );

        // Don't merge a filter onto a calc which contains windowed aggregates.
        // That would effectively be pushing a multiset down through a filter.
        // We'll have chance to merge later, when the over is expanded.
        if ( calc.getProgram().containsAggs() ) {
            return;
        }

        // Create a program containing the filter.
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final RexProgramBuilder progBuilder = new RexProgramBuilder( calc.getTupleType(), rexBuilder );
        progBuilder.addIdentity();
        progBuilder.addCondition( filter.getCondition() );
        RexProgram topProgram = progBuilder.getProgram();
        RexProgram bottomProgram = calc.getProgram();

        // Merge the programs together.
        RexProgram mergedProgram = RexProgramBuilder.mergePrograms( topProgram, bottomProgram, rexBuilder );
        final LogicalCalc newCalc = LogicalCalc.create( calc.getInput(), mergedProgram );
        call.transformTo( newCalc );
    }

}

