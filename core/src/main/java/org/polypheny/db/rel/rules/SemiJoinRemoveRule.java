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

package org.polypheny.db.rel.rules;


import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.SemiJoin;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Planner rule that removes a {@link SemiJoin}s from a join tree.
 *
 * It is invoked after attempts have been made to convert a SemiJoin to an indexed scan on a join factor have failed. Namely, if the join factor does not reduce to a single table that can be scanned using an index.
 *
 * It should only be enabled if all SemiJoins in the plan are advisory; that is, they can be safely dropped without affecting the semantics of the query.
 */
public class SemiJoinRemoveRule extends RelOptRule {

    public static final SemiJoinRemoveRule INSTANCE = new SemiJoinRemoveRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a SemiJoinRemoveRule.
     */
    public SemiJoinRemoveRule( RelBuilderFactory relBuilderFactory ) {
        super( operand( SemiJoin.class, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        call.transformTo( call.rel( 0 ).getInput( 0 ) );
    }
}

