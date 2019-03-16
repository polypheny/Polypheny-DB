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
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate.Group;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;


/**
 * Planner rule that pushes a {@link Filter} past a {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate}.
 *
 * @see ch.unibas.dmi.dbis.polyphenydb.rel.rules.AggregateFilterTransposeRule
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


    @Deprecated // to be removed before 2.0
    public FilterAggregateTransposeRule( Class<? extends Filter> filterClass, RelFactories.FilterFactory filterFactory, Class<? extends Aggregate> aggregateClass ) {
        this( filterClass, RelBuilder.proto( Contexts.of( filterFactory ) ), aggregateClass );
    }


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

