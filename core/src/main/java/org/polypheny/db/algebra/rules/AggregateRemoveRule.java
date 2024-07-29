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
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that removes a {@link org.polypheny.db.algebra.core.Aggregate} if it computes no aggregate functions (that is, it is implementing {@code SELECT DISTINCT}) and the
 * underlying relational expression is already distinct.
 */
public class AggregateRemoveRule extends AlgOptRule {

    public static final AggregateRemoveRule INSTANCE = new AggregateRemoveRule( LogicalRelAggregate.class, AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates an AggregateRemoveRule.
     */
    public AggregateRemoveRule( Class<? extends Aggregate> aggregateClass, AlgBuilderFactory algBuilderFactory ) {
        // REVIEW jvs: We have to explicitly mention the child here to make sure the rule re-fires after the child changes (e.g. via
        // ProjectRemoveRule), since that may change our information about whether the child is distinct.  If we clean up the inference of
        // distinct to make it correct up-front, we can get rid of the reference to the child here.
        super( operand( aggregateClass, operand( AlgNode.class, any() ) ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Aggregate aggregate = call.alg( 0 );
        final AlgNode input = call.alg( 1 );
        if ( !aggregate.getAggCallList().isEmpty() || aggregate.indicator ) {
            return;
        }
        final AlgMetadataQuery mq = call.getMetadataQuery();
        if ( !Functions.isTrue( mq.areColumnsUnique( input, aggregate.getGroupSet() ) ).value ) {
            return;
        }
        // Distinct is "GROUP BY c1, c2" (where c1, c2 are a set of columns on which the input is unique, i.e. contain a key) and has no aggregate functions. It can be removed.
        final AlgNode newInput = convert( input, aggregate.getTraitSet().simplify() );

        // If aggregate was projecting a subset of columns, add a project for the same effect.
        final AlgBuilder algBuilder = call.builder();
        algBuilder.push( newInput );
        if ( newInput.getTupleType().getFieldCount() > aggregate.getTupleType().getFieldCount() ) {
            algBuilder.project( algBuilder.fields( aggregate.getGroupSet().asList() ) );
        }
        call.transformTo( algBuilder.build() );
    }

}

