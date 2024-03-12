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


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Aggregate.Group;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.SubstitutionVisitor;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.mapping.Mappings;
import org.polypheny.db.util.mapping.Mappings.TargetMapping;


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
public class AggregateFilterTransposeRule extends AlgOptRule {

    public static final AggregateFilterTransposeRule INSTANCE = new AggregateFilterTransposeRule();


    private AggregateFilterTransposeRule() {
        this( operand( Aggregate.class, operand( Filter.class, any() ) ), AlgFactories.LOGICAL_BUILDER );
    }


    /**
     * Creates an AggregateFilterTransposeRule.
     */
    public AggregateFilterTransposeRule( AlgOptRuleOperand operand, AlgBuilderFactory algBuilderFactory ) {
        super( operand, algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Aggregate aggregate = call.alg( 0 );
        final Filter filter = call.alg( 1 );

        // Do the columns used by the filter appear in the output of the aggregate?
        final ImmutableBitSet filterColumns = AlgOptUtil.InputFinder.bits( filter.getCondition() );
        final ImmutableBitSet newGroupSet = aggregate.getGroupSet().union( filterColumns );
        final AlgNode input = filter.getInput();
        final AlgMetadataQuery mq = call.getMetadataQuery();
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
                input.getTupleType().getFieldCount(),
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
                final AggFunction rollup = SubstitutionVisitor.getRollup( aggregateCall.getAggregation() );
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
