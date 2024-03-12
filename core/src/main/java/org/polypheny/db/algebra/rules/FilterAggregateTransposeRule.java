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
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Planner rule that pushes a {@link Filter} past a {@link org.polypheny.db.algebra.core.Aggregate}.
 *
 * @see org.polypheny.db.algebra.rules.AggregateFilterTransposeRule
 */
public class FilterAggregateTransposeRule extends AlgOptRule {

    /**
     * The default instance of {@link FilterAggregateTransposeRule}.
     *
     * It matches any kind of agg. or filter
     */
    public static final FilterAggregateTransposeRule INSTANCE = new FilterAggregateTransposeRule( Filter.class, AlgFactories.LOGICAL_BUILDER, Aggregate.class );


    /**
     * Creates a FilterAggregateTransposeRule.
     *
     * If {@code filterFactory} is null, creates the same kind of filter as matched in the rule. Similarly {@code aggregateFactory}.
     */
    public FilterAggregateTransposeRule( Class<? extends Filter> filterClass, AlgBuilderFactory builderFactory, Class<? extends Aggregate> aggregateClass ) {
        this( operand( filterClass, operand( aggregateClass, any() ) ), builderFactory );
    }


    protected FilterAggregateTransposeRule( AlgOptRuleOperand operand, AlgBuilderFactory builderFactory ) {
        super( operand, builderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Filter filterRel = call.alg( 0 );
        final Aggregate aggRel = call.alg( 1 );

        final List<RexNode> conditions = AlgOptUtil.conjunctions( filterRel.getCondition() );
        final RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
        final List<AlgDataTypeField> origFields = aggRel.getTupleType().getFields();
        final int[] adjustments = new int[origFields.size()];
        int j = 0;
        for ( int i : aggRel.getGroupSet() ) {
            adjustments[j] = i - j;
            j++;
        }
        final List<RexNode> pushedConditions = new ArrayList<>();
        final List<RexNode> remainingConditions = new ArrayList<>();

        for ( RexNode condition : conditions ) {
            ImmutableBitSet rCols = AlgOptUtil.InputFinder.bits( condition );
            if ( canPush( aggRel, rCols ) ) {
                pushedConditions.add(
                        condition.accept(
                                new AlgOptUtil.RexInputConverter( rexBuilder, origFields, aggRel.getInput( 0 ).getTupleType().getFields(), adjustments ) ) );
            } else {
                remainingConditions.add( condition );
            }
        }

        final AlgBuilder builder = call.builder();
        AlgNode alg = builder.push( aggRel.getInput() ).filter( pushedConditions ).build();
        if ( alg == aggRel.getInput( 0 ) ) {
            return;
        }
        alg = aggRel.copy( aggRel.getTraitSet(), ImmutableList.of( alg ) );
        alg = builder.push( alg ).filter( remainingConditions ).build();
        call.transformTo( alg );
    }


    private boolean canPush( Aggregate aggregate, ImmutableBitSet rCols ) {
        // If the filter references columns not in the group key, we cannot push
        final ImmutableBitSet groupKeys = ImmutableBitSet.range( 0, aggregate.getGroupSet().cardinality() );
        if ( !groupKeys.contains( rCols ) ) {
            return false;
        }

        if ( aggregate.getGroupType() != Group.SIMPLE ) {
            // If grouping sets are used, the filter can be pushed if the columns referenced in the predicate are present in all the grouping sets.
            for ( ImmutableBitSet groupingSet : aggregate.getGroupSets() ) {
                if ( !groupingSet.contains( rCols ) ) {
                    return false;
                }
            }
        }
        return true;
    }

}

