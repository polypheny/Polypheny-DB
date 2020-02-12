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

package org.polypheny.db.rel.rules;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelOptRuleOperand;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Aggregate;
import org.polypheny.db.rel.core.Aggregate.Group;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Planner rule that pushes a {@link Filter} past a {@link org.polypheny.db.rel.core.Aggregate}.
 *
 * @see org.polypheny.db.rel.rules.AggregateFilterTransposeRule
 */
public class FilterAggregateTransposeRule extends RelOptRule {

    /**
     * The default instance of {@link FilterAggregateTransposeRule}.
     *
     * It matches any kind of agg. or filter
     */
    public static final FilterAggregateTransposeRule INSTANCE = new FilterAggregateTransposeRule( Filter.class, RelFactories.LOGICAL_BUILDER, Aggregate.class );


    /**
     * Creates a FilterAggregateTransposeRule.
     *
     * If {@code filterFactory} is null, creates the same kind of filter as matched in the rule. Similarly {@code aggregateFactory}.
     */
    public FilterAggregateTransposeRule( Class<? extends Filter> filterClass, RelBuilderFactory builderFactory, Class<? extends Aggregate> aggregateClass ) {
        this( operand( filterClass, operand( aggregateClass, any() ) ), builderFactory );
    }


    protected FilterAggregateTransposeRule( RelOptRuleOperand operand, RelBuilderFactory builderFactory ) {
        super( operand, builderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Filter filterRel = call.rel( 0 );
        final Aggregate aggRel = call.rel( 1 );

        final List<RexNode> conditions = RelOptUtil.conjunctions( filterRel.getCondition() );
        final RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
        final List<RelDataTypeField> origFields = aggRel.getRowType().getFieldList();
        final int[] adjustments = new int[origFields.size()];
        int j = 0;
        for ( int i : aggRel.getGroupSet() ) {
            adjustments[j] = i - j;
            j++;
        }
        final List<RexNode> pushedConditions = new ArrayList<>();
        final List<RexNode> remainingConditions = new ArrayList<>();

        for ( RexNode condition : conditions ) {
            ImmutableBitSet rCols = RelOptUtil.InputFinder.bits( condition );
            if ( canPush( aggRel, rCols ) ) {
                pushedConditions.add(
                        condition.accept(
                                new RelOptUtil.RexInputConverter( rexBuilder, origFields, aggRel.getInput( 0 ).getRowType().getFieldList(), adjustments ) ) );
            } else {
                remainingConditions.add( condition );
            }
        }

        final RelBuilder builder = call.builder();
        RelNode rel = builder.push( aggRel.getInput() ).filter( pushedConditions ).build();
        if ( rel == aggRel.getInput( 0 ) ) {
            return;
        }
        rel = aggRel.copy( aggRel.getTraitSet(), ImmutableList.of( rel ) );
        rel = builder.push( rel ).filter( remainingConditions ).build();
        call.transformTo( rel );
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

