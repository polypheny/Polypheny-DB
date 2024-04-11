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


import static org.polypheny.db.plan.AlgOptRule.any;
import static org.polypheny.db.plan.AlgOptRule.none;
import static org.polypheny.db.plan.AlgOptRule.operand;
import static org.polypheny.db.plan.AlgOptRule.some;
import static org.polypheny.db.plan.AlgOptRule.unordered;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Collection of rules which remove sections of a query plan known never to produce any rows.
 * <p>
 * Conventionally, the way to represent an empty relational expression is with a {@link Values} that has no tuples.
 *
 * @see LogicalRelValues#createEmpty
 */
public abstract class PruneEmptyRules {

    /**
     * Rule that removes empty children of a {@link LogicalRelUnion}.
     * <p>
     * Examples:
     *
     * <ul>
     * <li>Union(Rel, Empty, Rel2) becomes Union(Rel, Rel2)</li>
     * <li>Union(Rel, Empty, Empty) becomes Rel</li>
     * <li>Union(Empty, Empty) becomes Empty</li>
     * </ul>
     */
    public static final AlgOptRule UNION_INSTANCE =
            new AlgOptRule(
                    operand( LogicalRelUnion.class, unordered( AlgOptRule.operand( Values.class, null, Values::isEmpty, none() ) ) ),
                    "Union" ) {
                @Override
                public void onMatch( AlgOptRuleCall call ) {
                    final LogicalRelUnion union = call.alg( 0 );
                    final List<AlgNode> inputs = call.getChildAlgs( union );
                    assert inputs != null;
                    final List<AlgNode> newInputs = new ArrayList<>();
                    for ( AlgNode input : inputs ) {
                        if ( !isEmpty( input ) ) {
                            newInputs.add( input );
                        }
                    }
                    assert newInputs.size() < inputs.size() : "planner promised us at least one Empty child";
                    final AlgBuilder builder = call.builder();
                    switch ( newInputs.size() ) {
                        case 0:
                            builder.push( union ).empty();
                            break;
                        case 1:
                            builder.push( AlgOptUtil.createCastAlg( newInputs.get( 0 ), union.getTupleType(), true ) );
                            break;
                        default:
                            builder.push( LogicalRelUnion.create( newInputs, union.all ) );
                            break;
                    }
                    call.transformTo( builder.build() );
                }
            };

    /**
     * Rule that removes empty children of a {@link LogicalRelMinus}.
     * <p>
     * Examples:
     *
     * <ul>
     * <li>Minus(Rel, Empty, Rel2) becomes Minus(Rel, Rel2)</li>
     * <li>Minus(Empty, Rel) becomes Empty</li>
     * </ul>
     */
    public static final AlgOptRule MINUS_INSTANCE =
            new AlgOptRule(
                    operand( LogicalRelMinus.class, unordered( AlgOptRule.operand( Values.class, null, Values::isEmpty, none() ) ) ),
                    "Minus" ) {
                @Override
                public void onMatch( AlgOptRuleCall call ) {
                    final LogicalRelMinus minus = call.alg( 0 );
                    final List<AlgNode> inputs = call.getChildAlgs( minus );
                    assert inputs != null;
                    final List<AlgNode> newInputs = new ArrayList<>();
                    for ( AlgNode input : inputs ) {
                        if ( !isEmpty( input ) ) {
                            newInputs.add( input );
                        } else if ( newInputs.isEmpty() ) {
                            // If the first input of Minus is empty, the whole thing is empty.
                            break;
                        }
                    }
                    assert newInputs.size() < inputs.size() : "planner promised us at least one Empty child";
                    final AlgBuilder builder = call.builder();
                    switch ( newInputs.size() ) {
                        case 0:
                            builder.push( minus ).empty();
                            break;
                        case 1:
                            builder.push( AlgOptUtil.createCastAlg( newInputs.get( 0 ), minus.getTupleType(), true ) );
                            break;
                        default:
                            builder.push( LogicalRelMinus.create( newInputs, minus.all ) );
                            break;
                    }
                    call.transformTo( builder.build() );
                }
            };

    /**
     * Rule that converts a {@link LogicalRelIntersect} to empty if any of its children are empty.
     * <p>
     * Examples:
     *
     * <ul>
     * <li>Intersect(Rel, Empty, Rel2) becomes Empty</li>
     * <li>Intersect(Empty, Rel) becomes Empty</li>
     * </ul>
     */
    public static final AlgOptRule INTERSECT_INSTANCE =
            new AlgOptRule(
                    operand( LogicalRelIntersect.class, unordered( AlgOptRule.operand( Values.class, null, Values::isEmpty, none() ) ) ),
                    "Intersect" ) {
                @Override
                public void onMatch( AlgOptRuleCall call ) {
                    LogicalRelIntersect intersect = call.alg( 0 );
                    final AlgBuilder builder = call.builder();
                    builder.push( intersect ).empty();
                    call.transformTo( builder.build() );
                }
            };


    private static boolean isEmpty( AlgNode node ) {
        return node instanceof Values && ((Values) node).getTuples().isEmpty();
    }


    /**
     * Rule that converts a {@link LogicalRelProject} to empty if its child is empty.
     * <p>
     * Examples:
     *
     * <ul>
     * <li>Project(Empty) becomes Empty</li>
     * </ul>
     */
    public static final AlgOptRule PROJECT_INSTANCE =
            new RemoveEmptySingleRule( Project.class,
                    project -> true, AlgFactories.LOGICAL_BUILDER,
                    "PruneEmptyProject" );

    /**
     * Rule that converts a {@link LogicalRelFilter}
     * to empty if its child is empty.
     *
     * <p>Examples:
     *
     * <ul>
     * <li>Filter(Empty) becomes Empty
     * </ul>
     */
    public static final AlgOptRule FILTER_INSTANCE = new RemoveEmptySingleRule( Filter.class, "PruneEmptyFilter" );

    /**
     * Rule that converts a {@link Sort} to empty if its child is empty.
     *
     * <p>Examples:
     *
     * <ul>
     * <li>Sort(Empty) becomes Empty
     * </ul>
     */
    public static final AlgOptRule SORT_INSTANCE = new RemoveEmptySingleRule( Sort.class, "PruneEmptySort" );

    /**
     * Rule that converts a {@link Sort} to empty if it has {@code LIMIT 0}.
     * <p>
     * Examples:
     *
     * <ul>
     * <li>Sort(Empty) becomes Empty</li>
     * </ul>
     */
    public static final AlgOptRule SORT_FETCH_ZERO_INSTANCE =
            new AlgOptRule( operand( Sort.class, any() ), "PruneSortLimit0" ) {
                @Override
                public void onMatch( AlgOptRuleCall call ) {
                    Sort sort = call.alg( 0 );
                    if ( sort.fetch != null && !(sort.fetch instanceof RexDynamicParam) && RexLiteral.intValue( sort.fetch ) == 0 ) {
                        call.transformTo( call.builder().push( sort ).empty().build() );
                    }
                }
            };

    /**
     * Rule that converts an {@link org.polypheny.db.algebra.core.Aggregate} to empty if its child is empty.
     * <p>
     * Examples:
     *
     * <ul>
     * <li>{@code Aggregate(key: [1, 3], Empty)} &rarr; {@code Empty}</li>
     * <li>{@code Aggregate(key: [], Empty)} is unchanged, because an aggregate without a GROUP BY key always returns 1 row, even over empty input</li>
     * </ul>
     *
     * @see AggregateValuesRule
     */
    public static final AlgOptRule AGGREGATE_INSTANCE =
            new RemoveEmptySingleRule(
                    Aggregate.class,
                    Aggregate::isNotGrandTotal,
                    AlgFactories.LOGICAL_BUILDER,
                    "PruneEmptyAggregate" );

    /**
     * Rule that converts a {@link Join} to empty if its left child is empty.
     * <p>
     * Examples:
     *
     * <ul>
     * <li>Join(Empty, Scan(Dept), INNER) becomes Empty</li>
     * </ul>
     */
    public static final AlgOptRule JOIN_LEFT_INSTANCE =
            new AlgOptRule(
                    operand(
                            Join.class,
                            some(
                                    AlgOptRule.operand( Values.class, null, Values::isEmpty, none() ),
                                    operand( AlgNode.class, any() ) ) ),
                    "PruneEmptyJoin(left)" ) {
                @Override
                public void onMatch( AlgOptRuleCall call ) {
                    Join join = call.alg( 0 );
                    if ( join.getJoinType().generatesNullsOnLeft() ) {
                        // "select * from emp right join dept" is not necessarily empty if emp is empty
                        return;
                    }
                    call.transformTo( call.builder().push( join ).empty().build() );
                }
            };

    /**
     * Rule that converts a {@link Join} to empty if its right child is empty.
     * <p>
     * Examples:
     *
     * <ul>
     * <li>Join(Scan(Emp), Empty, INNER) becomes Empty</li>
     * </ul>
     */
    public static final AlgOptRule JOIN_RIGHT_INSTANCE =
            new AlgOptRule(
                    operand(
                            Join.class,
                            some(
                                    operand( AlgNode.class, any() ),
                                    AlgOptRule.operand( Values.class, null, Values::isEmpty, none() ) ) ),
                    "PruneEmptyJoin(right)" ) {
                @Override
                public void onMatch( AlgOptRuleCall call ) {
                    Join join = call.alg( 0 );
                    if ( join.getJoinType().generatesNullsOnRight() ) {
                        // "select * from emp left join dept" is not necessarily empty if dept is empty
                        return;
                    }
                    call.transformTo( call.builder().push( join ).empty().build() );
                }
            };


    /**
     * Planner rule that converts a single-rel (e.g. project, sort, aggregate or filter) on top of the empty relational expression into empty.
     */
    public static class RemoveEmptySingleRule extends AlgOptRule {

        /**
         * Creates a simple RemoveEmptySingleRule.
         */
        public <R extends SingleAlg> RemoveEmptySingleRule( Class<R> clazz, String description ) {
            this( clazz, (Predicate<R>) project -> true, AlgFactories.LOGICAL_BUILDER, description );
        }


        /**
         * Creates a RemoveEmptySingleRule.
         */
        public <R extends SingleAlg> RemoveEmptySingleRule( Class<R> clazz, Predicate<R> predicate, AlgBuilderFactory algBuilderFactory, String description ) {
            super(
                    operand(
                            clazz,
                            null,
                            predicate,
                            operand( Values.class, null, Values::isEmpty, none() ) ),
                    algBuilderFactory, description );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            SingleAlg single = call.alg( 0 );
            call.transformTo( call.builder().push( single ).empty().build() );
        }

    }

}

