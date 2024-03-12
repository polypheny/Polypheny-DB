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


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that matches {@link Aggregate}s beneath a {@link org.polypheny.db.algebra.core.Union} and pulls them up, so that a single
 * {@link Aggregate} removes duplicates.
 *
 * This rule only handles cases where the {@link org.polypheny.db.algebra.core.Union}s still have only two inputs.
 */
public class AggregateUnionAggregateRule extends AlgOptRule {

    /**
     * Instance that matches an {@code Aggregate} as the left input of {@code Union}.
     */
    public static final AggregateUnionAggregateRule AGG_ON_FIRST_INPUT =
            new AggregateUnionAggregateRule(
                    LogicalRelAggregate.class,
                    LogicalRelUnion.class,
                    LogicalRelAggregate.class,
                    AlgNode.class,
                    AlgFactories.LOGICAL_BUILDER,
                    "AggregateUnionAggregateRule:first-input-agg" );

    /**
     * Instance that matches an {@code Aggregate} as the right input of {@code Union}.
     */
    public static final AggregateUnionAggregateRule AGG_ON_SECOND_INPUT =
            new AggregateUnionAggregateRule(
                    LogicalRelAggregate.class,
                    LogicalRelUnion.class,
                    AlgNode.class,
                    LogicalRelAggregate.class,
                    AlgFactories.LOGICAL_BUILDER,
                    "AggregateUnionAggregateRule:second-input-agg" );

    /**
     * Instance that matches an {@code Aggregate} as either input of {@link Union}.
     *
     * Because it matches {@link AlgNode} for each input of {@code Union}, it will create O(N ^ 2) matches, which may cost too much during the popMatch phase in VolcanoPlanner.
     * If efficiency is a concern, we recommend that you use {@link #AGG_ON_FIRST_INPUT} and {@link #AGG_ON_SECOND_INPUT} instead.
     */
    public static final AggregateUnionAggregateRule INSTANCE =
            new AggregateUnionAggregateRule(
                    LogicalRelAggregate.class,
                    LogicalRelUnion.class,
                    AlgNode.class,
                    AlgNode.class,
                    AlgFactories.LOGICAL_BUILDER,
                    "AggregateUnionAggregateRule" );


    /**
     * Creates a AggregateUnionAggregateRule.
     */
    public AggregateUnionAggregateRule( Class<? extends Aggregate> aggregateClass, Class<? extends Union> unionClass, Class<? extends AlgNode> firstUnionInputClass, Class<? extends AlgNode> secondUnionInputClass, AlgBuilderFactory algBuilderFactory, String desc ) {
        super(
                operand(
                        aggregateClass,
                        null,
                        Aggregate::isSimple,
                        operand( unionClass, operand( firstUnionInputClass, any() ), operand( secondUnionInputClass, any() ) ) ),
                algBuilderFactory, desc );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Aggregate topAggAlg = call.alg( 0 );
        final Union union = call.alg( 1 );

        // If distincts haven't been removed yet, defer invoking this rule
        if ( !union.all ) {
            return;
        }

        final AlgBuilder algBuilder = call.builder();
        final Aggregate bottomAggAlg;
        if ( call.alg( 3 ) instanceof Aggregate ) {
            // Aggregate is the second input
            bottomAggAlg = call.alg( 3 );
            algBuilder.push( call.alg( 2 ) ).push( call.alg( 3 ).getInput( 0 ) );
        } else if ( call.alg( 2 ) instanceof Aggregate ) {
            // Aggregate is the first input
            bottomAggAlg = call.alg( 2 );
            algBuilder.push( call.alg( 2 ).getInput( 0 ) ).push( call.alg( 3 ) );
        } else {
            return;
        }

        // Only pull up aggregates if they are there just to remove distincts
        if ( !topAggAlg.getAggCallList().isEmpty() || !bottomAggAlg.getAggCallList().isEmpty() ) {
            return;
        }

        algBuilder.union( true );
        algBuilder.aggregate( algBuilder.groupKey( topAggAlg.getGroupSet() ), topAggAlg.getAggCallList() );
        call.transformTo( algBuilder.build() );
    }

}

