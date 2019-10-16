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
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalAggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.runtime.SqlFunctions;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Planner rule that removes a {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate} if it computes no aggregate functions (that is, it is implementing {@code SELECT DISTINCT}) and the
 * underlying relational expression is already distinct.
 */
public class AggregateRemoveRule extends RelOptRule {

    public static final AggregateRemoveRule INSTANCE = new AggregateRemoveRule( LogicalAggregate.class, RelFactories.LOGICAL_BUILDER );


    @Deprecated // to be removed before 2.0
    public AggregateRemoveRule( Class<? extends Aggregate> aggregateClass ) {
        this( aggregateClass, RelFactories.LOGICAL_BUILDER );
    }


    /**
     * Creates an AggregateRemoveRule.
     */
    public AggregateRemoveRule( Class<? extends Aggregate> aggregateClass, RelBuilderFactory relBuilderFactory ) {
        // REVIEW jvs 14-Mar-2006: We have to explicitly mention the child here to make sure the rule re-fires after the child changes (e.g. via
        // ProjectRemoveRule), since that may change our information about whether the child is distinct.  If we clean up the inference of
        // distinct to make it correct up-front, we can get rid of the reference to the child here.
        super(
                operand( aggregateClass, operand( RelNode.class, any() ) ),
                relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Aggregate aggregate = call.rel( 0 );
        final RelNode input = call.rel( 1 );
        if ( !aggregate.getAggCallList().isEmpty() || aggregate.indicator ) {
            return;
        }
        final RelMetadataQuery mq = call.getMetadataQuery();
        if ( !SqlFunctions.isTrue( mq.areColumnsUnique( input, aggregate.getGroupSet() ) ) ) {
            return;
        }
        // Distinct is "GROUP BY c1, c2" (where c1, c2 are a set of columns on which the input is unique, i.e. contain a key) and has no aggregate functions. It can be removed.
        final RelNode newInput = convert( input, aggregate.getTraitSet().simplify() );

        // If aggregate was projecting a subset of columns, add a project for the same effect.
        final RelBuilder relBuilder = call.builder();
        relBuilder.push( newInput );
        if ( newInput.getRowType().getFieldCount() > aggregate.getRowType().getFieldCount() ) {
            relBuilder.project( relBuilder.fields( aggregate.getGroupSet().asList() ) );
        }
        call.transformTo( relBuilder.build() );
    }
}

