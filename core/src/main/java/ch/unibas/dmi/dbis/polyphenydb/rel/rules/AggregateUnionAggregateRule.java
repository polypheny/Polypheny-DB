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
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Union;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalAggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalUnion;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Planner rule that matches {@link Aggregate}s beneath a {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Union} and pulls them up, so that a single
 * {@link Aggregate} removes duplicates.
 *
 * This rule only handles cases where the {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Union}s still have only two inputs.
 */
public class AggregateUnionAggregateRule extends RelOptRule {

    /**
     * Instance that matches an {@code Aggregate} as the left input of {@code Union}.
     */
    public static final AggregateUnionAggregateRule AGG_ON_FIRST_INPUT =
            new AggregateUnionAggregateRule(
                    LogicalAggregate.class,
                    LogicalUnion.class,
                    LogicalAggregate.class,
                    RelNode.class,
                    RelFactories.LOGICAL_BUILDER,
                    "AggregateUnionAggregateRule:first-input-agg" );

    /**
     * Instance that matches an {@code Aggregate} as the right input of {@code Union}.
     */
    public static final AggregateUnionAggregateRule AGG_ON_SECOND_INPUT =
            new AggregateUnionAggregateRule(
                    LogicalAggregate.class,
                    LogicalUnion.class,
                    RelNode.class,
                    LogicalAggregate.class,
                    RelFactories.LOGICAL_BUILDER,
                    "AggregateUnionAggregateRule:second-input-agg" );

    /**
     * Instance that matches an {@code Aggregate} as either input of {@link Union}.
     *
     * Because it matches {@link RelNode} for each input of {@code Union}, it will create O(N ^ 2) matches, which may cost too much during the popMatch phase in VolcanoPlanner.
     * If efficiency is a concern, we recommend that you use {@link #AGG_ON_FIRST_INPUT} and {@link #AGG_ON_SECOND_INPUT} instead.
     */
    public static final AggregateUnionAggregateRule INSTANCE =
            new AggregateUnionAggregateRule(
                    LogicalAggregate.class,
                    LogicalUnion.class,
                    RelNode.class,
                    RelNode.class,
                    RelFactories.LOGICAL_BUILDER,
                    "AggregateUnionAggregateRule" );


    /**
     * Creates a AggregateUnionAggregateRule.
     */
    public AggregateUnionAggregateRule( Class<? extends Aggregate> aggregateClass, Class<? extends Union> unionClass, Class<? extends RelNode> firstUnionInputClass, Class<? extends RelNode> secondUnionInputClass, RelBuilderFactory relBuilderFactory, String desc ) {
        super(
                operandJ(
                        aggregateClass,
                        null,
                        Aggregate::isSimple,
                        operand( unionClass, operand( firstUnionInputClass, any() ), operand( secondUnionInputClass, any() ) ) ),
                relBuilderFactory, desc );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Aggregate topAggRel = call.rel( 0 );
        final Union union = call.rel( 1 );

        // If distincts haven't been removed yet, defer invoking this rule
        if ( !union.all ) {
            return;
        }

        final RelBuilder relBuilder = call.builder();
        final Aggregate bottomAggRel;
        if ( call.rel( 3 ) instanceof Aggregate ) {
            // Aggregate is the second input
            bottomAggRel = call.rel( 3 );
            relBuilder.push( call.rel( 2 ) ).push( call.rel( 3 ).getInput( 0 ) );
        } else if ( call.rel( 2 ) instanceof Aggregate ) {
            // Aggregate is the first input
            bottomAggRel = call.rel( 2 );
            relBuilder.push( call.rel( 2 ).getInput( 0 ) ).push( call.rel( 3 ) );
        } else {
            return;
        }

        // Only pull up aggregates if they are there just to remove distincts
        if ( !topAggRel.getAggCallList().isEmpty() || !bottomAggRel.getAggCallList().isEmpty() ) {
            return;
        }

        relBuilder.union( true );
        relBuilder.aggregate( relBuilder.groupKey( topAggRel.getGroupSet() ), topAggRel.getAggCallList() );
        call.transformTo( relBuilder.build() );
    }
}

