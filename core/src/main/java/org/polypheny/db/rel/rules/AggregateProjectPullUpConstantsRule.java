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


import org.polypheny.db.plan.RelOptPredicateList;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Aggregate;
import org.polypheny.db.rel.core.AggregateCall;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.logical.LogicalAggregate;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;


/**
 * Planner rule that removes constant keys from an {@link org.polypheny.db.rel.core.Aggregate}.
 *
 * Constant fields are deduced using {@link RelMetadataQuery#getPulledUpPredicates(RelNode)}; the input does not need to be a {@link org.polypheny.db.rel.core.Project}.
 *
 * This rules never removes the last column, because {@code Aggregate([])} returns 1 row even if its input is empty.
 *
 * Since the transformed relational expression has to match the original relational expression, the constants are placed in a projection above the reduced aggregate. If those constants are not used, another rule will remove
 * them from the project.
 */
public class AggregateProjectPullUpConstantsRule extends RelOptRule {

    /**
     * The singleton.
     */
    public static final AggregateProjectPullUpConstantsRule INSTANCE = new AggregateProjectPullUpConstantsRule( LogicalAggregate.class, LogicalProject.class, RelFactories.LOGICAL_BUILDER, "AggregateProjectPullUpConstantsRule" );

    /**
     * More general instance that matches any relational expression.
     */
    public static final AggregateProjectPullUpConstantsRule INSTANCE2 = new AggregateProjectPullUpConstantsRule( LogicalAggregate.class, RelNode.class, RelFactories.LOGICAL_BUILDER, "AggregatePullUpConstantsRule" );


    /**
     * Creates an AggregateProjectPullUpConstantsRule.
     *
     * @param aggregateClass Aggregate class
     * @param inputClass Input class, such as {@link LogicalProject}
     * @param relBuilderFactory Builder for relational expressions
     * @param description Description, or null to guess description
     */
    public AggregateProjectPullUpConstantsRule( Class<? extends Aggregate> aggregateClass, Class<? extends RelNode> inputClass, RelBuilderFactory relBuilderFactory, String description ) {
        super(
                operandJ( aggregateClass, null, Aggregate::isSimple, operand( inputClass, any() ) ),
                relBuilderFactory, description );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Aggregate aggregate = call.rel( 0 );
        final RelNode input = call.rel( 1 );

        assert !aggregate.indicator : "predicate ensured no grouping sets";
        final int groupCount = aggregate.getGroupCount();
        if ( groupCount == 1 ) {
            // No room for optimization since we cannot convert from non-empty
            // GROUP BY list to the empty one.
            return;
        }

        final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        final RelMetadataQuery mq = call.getMetadataQuery();
        final RelOptPredicateList predicates = mq.getPulledUpPredicates( aggregate.getInput() );
        if ( predicates == null ) {
            return;
        }
        final NavigableMap<Integer, RexNode> map = new TreeMap<>();
        for ( int key : aggregate.getGroupSet() ) {
            final RexInputRef ref = rexBuilder.makeInputRef( aggregate.getInput(), key );
            if ( predicates.constantMap.containsKey( ref ) ) {
                map.put( key, predicates.constantMap.get( ref ) );
            }
        }

        // None of the group expressions are constant. Nothing to do.
        if ( map.isEmpty() ) {
            return;
        }

        if ( groupCount == map.size() ) {
            // At least a single item in group by is required.
            // Otherwise "GROUP BY 1, 2" might be altered to "GROUP BY ()".
            // Removing of the first element is not optimal here, however it will allow us to use fast path below (just trim groupCount).
            map.remove( map.navigableKeySet().first() );
        }

        ImmutableBitSet newGroupSet = aggregate.getGroupSet();
        for ( int key : map.keySet() ) {
            newGroupSet = newGroupSet.clear( key );
        }
        final int newGroupCount = newGroupSet.cardinality();

        // If the constants are on the trailing edge of the group list, we just reduce the group count.
        final RelBuilder relBuilder = call.builder();
        relBuilder.push( input );

        // Clone aggregate calls.
        final List<AggregateCall> newAggCalls = new ArrayList<>();
        for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
            newAggCalls.add( aggCall.adaptTo( input, aggCall.getArgList(), aggCall.filterArg, groupCount, newGroupCount ) );
        }
        relBuilder.aggregate( relBuilder.groupKey( newGroupSet ), newAggCalls );

        // Create a projection back again.
        List<Pair<RexNode, String>> projects = new ArrayList<>();
        int source = 0;
        for ( RelDataTypeField field : aggregate.getRowType().getFieldList() ) {
            RexNode expr;
            final int i = field.getIndex();
            if ( i >= groupCount ) {
                // Aggregate expressions' names and positions are unchanged.
                expr = relBuilder.field( i - map.size() );
            } else {
                int pos = aggregate.getGroupSet().nth( i );
                if ( map.containsKey( pos ) ) {
                    // Re-generate the constant expression in the project.
                    RelDataType originalType = aggregate.getRowType().getFieldList().get( projects.size() ).getType();
                    if ( !originalType.equals( map.get( pos ).getType() ) ) {
                        expr = rexBuilder.makeCast( originalType, map.get( pos ), true );
                    } else {
                        expr = map.get( pos );
                    }
                } else {
                    // Project the aggregation expression, in its original position.
                    expr = relBuilder.field( source );
                    ++source;
                }
            }
            projects.add( Pair.of( expr, field.getName() ) );
        }
        relBuilder.project( Pair.left( projects ), Pair.right( projects ) ); // inverse
        call.transformTo( relBuilder.build() );
    }

}

