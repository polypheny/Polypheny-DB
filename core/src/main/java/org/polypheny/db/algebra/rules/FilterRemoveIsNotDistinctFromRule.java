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
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that replaces {@code IS NOT DISTINCT FROM} in a {@link Filter} with logically equivalent operations.
 *
 * @see OperatorRegistry IS_NOT_DISTINCT_FROM
 */
public final class FilterRemoveIsNotDistinctFromRule extends AlgOptRule {

    /**
     * The singleton.
     */
    public static final FilterRemoveIsNotDistinctFromRule INSTANCE = new FilterRemoveIsNotDistinctFromRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a FilterRemoveIsNotDistinctFromRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public FilterRemoveIsNotDistinctFromRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( Filter.class, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        Filter oldFilter = call.alg( 0 );
        RexNode oldFilterCond = oldFilter.getCondition();

        if ( RexUtil.findOperatorCall( OperatorRegistry.get( OperatorName.IS_NOT_DISTINCT_FROM ), oldFilterCond ) == null ) {
            // no longer contains isNotDistinctFromOperator
            return;
        }

        // Now replace all the "a isNotDistinctFrom b" with the RexNode given by RelOptUtil.isDistinctFrom() method

        RemoveIsNotDistinctFromRexShuttle rewriteShuttle = new RemoveIsNotDistinctFromRexShuttle( oldFilter.getCluster().getRexBuilder() );

        final AlgBuilder algBuilder = call.builder();
        final AlgNode newFilterRel = algBuilder
                .push( oldFilter.getInput() )
                .filter( oldFilterCond.accept( rewriteShuttle ) )
                .build();

        call.transformTo( newFilterRel );
    }


    /**
     * Shuttle that removes 'x IS NOT DISTINCT FROM y' and converts it to
     * 'CASE WHEN x IS NULL THEN y IS NULL WHEN y IS NULL THEN x IS NULL ELSE x = y END'.
     */
    private class RemoveIsNotDistinctFromRexShuttle extends RexShuttle {

        RexBuilder rexBuilder;


        RemoveIsNotDistinctFromRexShuttle( RexBuilder rexBuilder ) {
            this.rexBuilder = rexBuilder;
        }


        // override RexShuttle
        @Override
        public RexNode visitCall( RexCall call ) {
            RexNode newCall = super.visitCall( call );

            if ( call.getOperator().equals( OperatorRegistry.get( OperatorName.IS_NOT_DISTINCT_FROM ) ) ) {
                RexCall tmpCall = (RexCall) newCall;
                newCall = AlgOptUtil.isDistinctFrom(
                        rexBuilder,
                        tmpCall.operands.get( 0 ),
                        tmpCall.operands.get( 1 ),
                        true );
            }
            return newCall;
        }

    }

}

