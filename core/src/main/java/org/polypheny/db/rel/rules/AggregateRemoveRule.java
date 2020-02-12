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


    /**
     * Creates an AggregateRemoveRule.
     */
    public AggregateRemoveRule( Class<? extends Aggregate> aggregateClass, RelBuilderFactory relBuilderFactory ) {
        // REVIEW jvs: We have to explicitly mention the child here to make sure the rule re-fires after the child changes (e.g. via
        // ProjectRemoveRule), since that may change our information about whether the child is distinct.  If we clean up the inference of
        // distinct to make it correct up-front, we can get rid of the reference to the child here.
        super( operand( aggregateClass, operand( RelNode.class, any() ) ), relBuilderFactory, null );
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

