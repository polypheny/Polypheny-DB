/*
 * Copyright 2019-2021 The Polypheny Project
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
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.metadata.AlgMdPredicates;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptPredicateList;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that infers predicates from on a {@link Join} and creates {@link Filter}s if those predicates can be pushed to its inputs.
 *
 * Uses {@link AlgMdPredicates} to infer the predicates, returns them in a {@link AlgOptPredicateList} and applies them appropriately.
 */
public class JoinPushTransitivePredicatesRule extends AlgOptRule {

    /**
     * The singleton.
     */
    public static final JoinPushTransitivePredicatesRule INSTANCE = new JoinPushTransitivePredicatesRule( Join.class, AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a JoinPushTransitivePredicatesRule.
     */
    public JoinPushTransitivePredicatesRule( Class<? extends Join> clazz, AlgBuilderFactory algBuilderFactory ) {
        super( operand( clazz, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        Join join = call.alg( 0 );
        final AlgMetadataQuery mq = call.getMetadataQuery();
        AlgOptPredicateList preds = mq.getPulledUpPredicates( join );

        if ( preds.leftInferredPredicates.isEmpty() && preds.rightInferredPredicates.isEmpty() ) {
            return;
        }

        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final AlgBuilder algBuilder = call.builder();

        AlgNode lChild = join.getLeft();
        if ( preds.leftInferredPredicates.size() > 0 ) {
            AlgNode curr = lChild;
            lChild = algBuilder.push( lChild ).filter( preds.leftInferredPredicates ).build();
            call.getPlanner().onCopy( curr, lChild );
        }

        AlgNode rChild = join.getRight();
        if ( preds.rightInferredPredicates.size() > 0 ) {
            AlgNode curr = rChild;
            rChild = algBuilder.push( rChild ).filter( preds.rightInferredPredicates ).build();
            call.getPlanner().onCopy( curr, rChild );
        }

        AlgNode newRel = join.copy( join.getTraitSet(), join.getCondition(), lChild, rChild, join.getJoinType(), join.isSemiJoinDone() );
        call.getPlanner().onCopy( join, newRel );

        call.transformTo( newRel );
    }

}

