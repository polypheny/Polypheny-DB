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
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Correlate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;


/**
 * Planner rule that pushes a {@link Filter} above a {@link Correlate} into the inputs of the Correlate.
 */
public class FilterCorrelateRule extends RelOptRule {

    public static final FilterCorrelateRule INSTANCE = new FilterCorrelateRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a FilterCorrelateRule.
     */
    public FilterCorrelateRule( RelBuilderFactory builderFactory ) {
        super(
                operand( Filter.class, operand( Correlate.class, RelOptRule.any() ) ),
                builderFactory, "FilterCorrelateRule" );
    }


    /**
     * Creates a FilterCorrelateRule with an explicit root operand and factories.
     */
    @Deprecated // to be removed before 2.0
    public FilterCorrelateRule( RelFactories.FilterFactory filterFactory, RelFactories.ProjectFactory projectFactory ) {
        this( RelBuilder.proto( filterFactory, projectFactory ) );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Filter filter = call.rel( 0 );
        final Correlate corr = call.rel( 1 );

        final List<RexNode> aboveFilters = RelOptUtil.conjunctions( filter.getCondition() );

        final List<RexNode> leftFilters = new ArrayList<>();
        final List<RexNode> rightFilters = new ArrayList<>();

        // Try to push down above filters. These are typically where clause filters. They can be pushed down if they are not on the NULL generating side.
        RelOptUtil.classifyFilters(
                corr,
                aboveFilters,
                JoinRelType.INNER,
                false,
                !corr.getJoinType().toJoinType().generatesNullsOnLeft(),
                !corr.getJoinType().toJoinType().generatesNullsOnRight(),
                aboveFilters,
                leftFilters,
                rightFilters );

        if ( leftFilters.isEmpty() && rightFilters.isEmpty() ) {
            // no filters got pushed
            return;
        }

        // Create Filters on top of the children if any filters were pushed to them.
        final RexBuilder rexBuilder = corr.getCluster().getRexBuilder();
        final RelBuilder relBuilder = call.builder();
        final RelNode leftRel = relBuilder.push( corr.getLeft() ).filter( leftFilters ).build();
        final RelNode rightRel = relBuilder.push( corr.getRight() ).filter( rightFilters ).build();

        // Create the new Correlate
        RelNode newCorrRel = corr.copy( corr.getTraitSet(), ImmutableList.of( leftRel, rightRel ) );

        call.getPlanner().onCopy( corr, newCorrRel );

        if ( !leftFilters.isEmpty() ) {
            call.getPlanner().onCopy( filter, leftRel );
        }
        if ( !rightFilters.isEmpty() ) {
            call.getPlanner().onCopy( filter, rightRel );
        }

        // Create a Filter on top of the join if needed
        relBuilder.push( newCorrRel );
        relBuilder.filter( RexUtil.fixUp( rexBuilder, aboveFilters, RelOptUtil.getFieldTypeList( relBuilder.peek().getRowType() ) ) );

        call.transformTo( relBuilder.build() );
    }
}

