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
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Planner rule that combines two {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter}s.
 */
public class FilterMergeRule extends RelOptRule {

    public static final FilterMergeRule INSTANCE = new FilterMergeRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a FilterMergeRule.
     */
    public FilterMergeRule( RelBuilderFactory relBuilderFactory ) {
        super(
                operand( Filter.class, operand( Filter.class, any() ) ),
                relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Filter topFilter = call.rel( 0 );
        final Filter bottomFilter = call.rel( 1 );

        // use RexPrograms to merge the two FilterRels into a single program so we can convert the two LogicalFilter conditions to directly reference the bottom LogicalFilter's child
        RexBuilder rexBuilder = topFilter.getCluster().getRexBuilder();
        RexProgram bottomProgram = createProgram( bottomFilter );
        RexProgram topProgram = createProgram( topFilter );

        RexProgram mergedProgram = RexProgramBuilder.mergePrograms( topProgram, bottomProgram, rexBuilder );

        RexNode newCondition = mergedProgram.expandLocalRef( mergedProgram.getCondition() );

        final RelBuilder relBuilder = call.builder();
        relBuilder.push( bottomFilter.getInput() ).filter( newCondition );

        call.transformTo( relBuilder.build() );
    }


    /**
     * Creates a RexProgram corresponding to a LogicalFilter
     *
     * @param filterRel the LogicalFilter
     * @return created RexProgram
     */
    private RexProgram createProgram( Filter filterRel ) {
        RexProgramBuilder programBuilder = new RexProgramBuilder( filterRel.getRowType(), filterRel.getCluster().getRexBuilder() );
        programBuilder.addIdentity();
        programBuilder.addCondition( filterRel.getCondition() );
        return programBuilder.getProgram();
    }
}

