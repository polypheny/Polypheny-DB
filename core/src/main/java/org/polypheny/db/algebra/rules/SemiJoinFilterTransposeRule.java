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
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes {@link SemiJoin}s down in a tree past a {@link Filter}.
 *
 * The intention is to trigger other rules that will convert {@code SemiJoin}s.
 *
 * SemiJoin(LogicalFilter(X), Y) &rarr; LogicalFilter(SemiJoin(X, Y))
 *
 * @see SemiJoinProjectTransposeRule
 */
public class SemiJoinFilterTransposeRule extends AlgOptRule {

    public static final SemiJoinFilterTransposeRule INSTANCE = new SemiJoinFilterTransposeRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a SemiJoinFilterTransposeRule.
     */
    public SemiJoinFilterTransposeRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( SemiJoin.class, some( operand( LogicalRelFilter.class, any() ) ) ),
                algBuilderFactory, null );
    }


    // implement RelOptRule
    @Override
    public void onMatch( AlgOptRuleCall call ) {
        SemiJoin semiJoin = call.alg( 0 );
        LogicalRelFilter filter = call.alg( 1 );

        AlgNode newSemiJoin =
                SemiJoin.create(
                        filter.getInput(),
                        semiJoin.getRight(),
                        semiJoin.getCondition(),
                        semiJoin.getLeftKeys(),
                        semiJoin.getRightKeys() );

        final AlgFactories.FilterFactory factory = AlgFactories.DEFAULT_FILTER_FACTORY;
        AlgNode newFilter = factory.createFilter( newSemiJoin, filter.getCondition() );

        call.transformTo( newFilter );
    }

}

