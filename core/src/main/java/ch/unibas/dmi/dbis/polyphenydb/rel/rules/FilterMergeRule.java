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


import ch.unibas.dmi.dbis.polyphenydb.plan.Contexts;
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


    @Deprecated // to be removed before 2.0
    public FilterMergeRule( RelFactories.FilterFactory filterFactory ) {
        this( RelBuilder.proto( Contexts.of( filterFactory ) ) );
    }


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

