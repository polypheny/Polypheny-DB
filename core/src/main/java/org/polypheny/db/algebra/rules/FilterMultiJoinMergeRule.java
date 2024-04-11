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


import java.util.Arrays;
import java.util.List;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that merges a {@link LogicalRelFilter} into a {@link MultiJoin}, creating a richer {@code MultiJoin}.
 *
 * @see ProjectMultiJoinMergeRule
 */
public class FilterMultiJoinMergeRule extends AlgOptRule {

    public static final FilterMultiJoinMergeRule INSTANCE = new FilterMultiJoinMergeRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a FilterMultiJoinMergeRule.
     */
    public FilterMultiJoinMergeRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( LogicalRelFilter.class, operand( MultiJoin.class, any() ) ),
                algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LogicalRelFilter filter = call.alg( 0 );
        MultiJoin multiJoin = call.alg( 1 );

        // Create a new post-join filter condition
        // Conditions are nullable, so ImmutableList can't be used here
        List<RexNode> filters = Arrays.asList( filter.getCondition(), multiJoin.getPostJoinFilter() );

        final RexBuilder rexBuilder = multiJoin.getCluster().getRexBuilder();
        MultiJoin newMultiJoin =
                new MultiJoin(
                        multiJoin.getCluster(),
                        multiJoin.getInputs(),
                        multiJoin.getJoinFilter(),
                        multiJoin.getTupleType(),
                        multiJoin.isFullOuterJoin(),
                        multiJoin.getOuterJoinConditions(),
                        multiJoin.getJoinTypes(),
                        multiJoin.getProjFields(),
                        multiJoin.getJoinFieldRefCountsMap(),
                        RexUtil.composeConjunction( rexBuilder, filters, true ) );

        call.transformTo( newMultiJoin );
    }

}

