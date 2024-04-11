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
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that combines two {@link LogicalRelFilter}s.
 */
public class FilterMergeRule extends AlgOptRule {

    public static final FilterMergeRule INSTANCE = new FilterMergeRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a FilterMergeRule.
     */
    public FilterMergeRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( Filter.class, operand( Filter.class, any() ) ),
                algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Filter topFilter = call.alg( 0 );
        final Filter bottomFilter = call.alg( 1 );

        // use RexPrograms to merge the two FilterAlgs into a single program so we can convert the two LogicalFilter
        // conditions to directly reference the bottom LogicalFilter's child
        RexBuilder rexBuilder = topFilter.getCluster().getRexBuilder();
        RexProgram bottomProgram = createProgram( bottomFilter );
        RexProgram topProgram = createProgram( topFilter );

        RexProgram mergedProgram = RexProgramBuilder.mergePrograms( topProgram, bottomProgram, rexBuilder );

        RexNode newCondition = mergedProgram.expandLocalRef( mergedProgram.getCondition() );

        final AlgBuilder algBuilder = call.builder();
        algBuilder.push( bottomFilter.getInput() ).filter( newCondition );

        call.transformTo( algBuilder.build() );
    }


    /**
     * Creates a RexProgram corresponding to a LogicalFilter
     *
     * @param filterRel the LogicalFilter
     * @return created RexProgram
     */
    private RexProgram createProgram( Filter filterRel ) {
        RexProgramBuilder programBuilder = new RexProgramBuilder( filterRel.getTupleType(), filterRel.getCluster().getRexBuilder() );
        programBuilder.addIdentity();
        programBuilder.addCondition( filterRel.getCondition() );
        return programBuilder.getProgram();
    }

}

