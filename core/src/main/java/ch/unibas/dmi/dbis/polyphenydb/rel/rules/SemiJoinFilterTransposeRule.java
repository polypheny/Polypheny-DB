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
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.SemiJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Planner rule that pushes {@link SemiJoin}s down in a tree past a {@link Filter}.
 *
 * The intention is to trigger other rules that will convert {@code SemiJoin}s.
 *
 * SemiJoin(LogicalFilter(X), Y) &rarr; LogicalFilter(SemiJoin(X, Y))
 *
 * @see SemiJoinProjectTransposeRule
 */
public class SemiJoinFilterTransposeRule extends RelOptRule {

    public static final SemiJoinFilterTransposeRule INSTANCE = new SemiJoinFilterTransposeRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a SemiJoinFilterTransposeRule.
     */
    public SemiJoinFilterTransposeRule( RelBuilderFactory relBuilderFactory ) {
        super(
                operand( SemiJoin.class, some( operand( LogicalFilter.class, any() ) ) ),
                relBuilderFactory, null );
    }


    // implement RelOptRule
    @Override
    public void onMatch( RelOptRuleCall call ) {
        SemiJoin semiJoin = call.rel( 0 );
        LogicalFilter filter = call.rel( 1 );

        RelNode newSemiJoin =
                SemiJoin.create(
                        filter.getInput(),
                        semiJoin.getRight(),
                        semiJoin.getCondition(),
                        semiJoin.getLeftKeys(),
                        semiJoin.getRightKeys() );

        final RelFactories.FilterFactory factory = RelFactories.DEFAULT_FILTER_FACTORY;
        RelNode newFilter = factory.createFilter( newSemiJoin, filter.getCondition() );

        call.transformTo( newFilter );
    }
}

