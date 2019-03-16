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
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.SubstitutionVisitor;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate.Group;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.AggregateCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlAggFunction;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings.TargetMapping;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;


/**
 * Planner rule that matches an {@link Aggregate} on a {@link Filter} and transposes them, pushing the aggregate below the filter.
 *
 * In some cases, it is necessary to split the aggregate.
 *
 * This rule does not directly improve performance. The aggregate will have to process more rows, to produce aggregated rows that will be thrown away. The rule might be beneficial if the predicate is very expensive to
 * evaluate. The main use of the rule is to match a query that has a filter under an aggregate to an existing aggregate table.
 *
 * @see FilterAggregateTransposeRule
 */
public class AggregateFilterTransposeRule extends RelOptRule {

    public static final AggregateFilterTransposeRule INSTANCE = new AggregateFilterTransposeRule();


    private AggregateFilterTransposeRule() {
        this( operand( Aggregate.class, operand( Filter.class, any() ) ), RelFactories.LOGICAL_BUILDER );
    }


    /**
     * Creates an AggregateFilterTransposeRule.
     */
    public AggregateFilterTransposeRule( RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory ) {
        super( operand, relBuilderFactory, null );
    }


    public void onMatch( RelOptRuleCall call ) {
        final Aggregate aggregate = call.rel( 0 );
        final Filter filter = call.rel( 1 );

        // Do the columns used by the filter appear in the output of the aggregate?
        final ImmutableBitSet filterColumns = RelOptUtil.InputFinder.bits( filter.getCondition() );
        final ImmutableBitSet newGroupSet = aggregate.getGroupSet().union( filterColumns );
        final RelNode input = filter.getInput();
        final RelMetadataQuery mq = call.getMetadataQuery();
        final Boolean unique = mq.areColumnsUnique( input, newGroupSet );
        if ( unique != null && unique ) {
            // The input is already unique on the grouping columns, so there's little advantage of aggregating again. More important, without this check,
            // the rule fires forever: A-F => A-F-A => A-A-F-A => A-A-A-F-A => ...
            return;
        }
        boolean allColumnsInAggregate = aggregate.getGroupSet().contains( filterColumns );
        final Aggregate newAggregate = aggregate.copy(
                aggregate.getTraitSet(),
                input,
                false,
                newGroupSet,
                null,
                aggregate.getAggCallList() );
        final TargetMapping mapping = Mappings.target(
                newGroupSet::indexOf,
                input.getRowType().getFieldCount(),
                newGroupSet.cardinality() );
        final RexNode newCondition =
                RexUtil.apply( mapping, filter.getCondition() );
        final Filter newFilter = filter.copy( filter.getTraitSet(),
                newAggregate, newCondition );
        if ( allColumnsInAggregate && aggregate.getGroupType() == Group.SIMPLE ) {
            // Everything needed by the filter is returned by the aggregate.
            assert newGroupSet.equals( aggregate.getGroupSet() );
            call.transformTo( newFilter );
        } else {
            // If aggregate uses grouping sets, we always need to split it.
            // Otherwise, it means that grouping sets are not used, but the filter needs at least one extra column, and now aggregate it away.
            final ImmutableBitSet.Builder topGroupSet = ImmutableBitSet.builder();
            for ( int c : aggregate.getGroupSet() ) {
                topGroupSet.set( newGroupSet.indexOf( c ) );
            }
            ImmutableList<ImmutableBitSet> newGroupingSets = null;
            if ( aggregate.getGroupType() != Group.SIMPLE ) {
                ImmutableList.Builder<ImmutableBitSet> newGroupingSetsBuilder = ImmutableList.builder();
                for ( ImmutableBitSet groupingSet : aggregate.getGroupSets() ) {
                    final ImmutableBitSet.Builder newGroupingSet = ImmutableBitSet.builder();
                    for ( int c : groupingSet ) {
                        newGroupingSet.set( newGroupSet.indexOf( c ) );
                    }
                    newGroupingSetsBuilder.add( newGroupingSet.build() );
                }
                newGroupingSets = newGroupingSetsBuilder.build();
            }
            final List<AggregateCall> topAggCallList = new ArrayList<>();
            int i = newGroupSet.cardinality();
            for ( AggregateCall aggregateCall : aggregate.getAggCallList() ) {
                final SqlAggFunction rollup = SubstitutionVisitor.getRollup( aggregateCall.getAggregation() );
                if ( rollup == null ) {
                    // This aggregate cannot be rolled up.
                    return;
                }
                if ( aggregateCall.isDistinct() ) {
                    // Cannot roll up distinct.
                    return;
                }
                topAggCallList.add(
                        AggregateCall.create(
                                rollup,
                                aggregateCall.isDistinct(),
                                aggregateCall.isApproximate(),
                                ImmutableList.of( i++ ),
                                -1,
                                aggregateCall.collation,
                                aggregateCall.type,
                                aggregateCall.name ) );
            }
            final Aggregate topAggregate = aggregate.copy( aggregate.getTraitSet(), newFilter, aggregate.indicator, topGroupSet.build(), newGroupingSets, topAggCallList );
            call.transformTo( topAggregate );
        }
    }
}
