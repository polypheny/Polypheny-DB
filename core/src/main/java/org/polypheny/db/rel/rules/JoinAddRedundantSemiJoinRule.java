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
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Join;
import org.polypheny.db.rel.core.JoinInfo;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.SemiJoin;
import org.polypheny.db.rel.logical.LogicalJoin;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Rule to add a semi-join into a join. Transformation is as follows:
 *
 * LogicalJoin(X, Y) &rarr; LogicalJoin(SemiJoin(X, Y), Y)
 *
 * The constructor is parameterized to allow any sub-class of {@link org.polypheny.db.rel.core.Join}, not just {@link org.polypheny.db.rel.logical.LogicalJoin}.
 */
public class JoinAddRedundantSemiJoinRule extends RelOptRule {

    public static final JoinAddRedundantSemiJoinRule INSTANCE = new JoinAddRedundantSemiJoinRule( LogicalJoin.class, RelFactories.LOGICAL_BUILDER );


    /**
     * Creates an JoinAddRedundantSemiJoinRule.
     */
    public JoinAddRedundantSemiJoinRule( Class<? extends Join> clazz, RelBuilderFactory relBuilderFactory ) {
        super( operand( clazz, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        Join origJoinRel = call.rel( 0 );
        if ( origJoinRel.isSemiJoinDone() ) {
            return;
        }

        // can't process outer joins using semijoins
        if ( origJoinRel.getJoinType() != JoinRelType.INNER ) {
            return;
        }

        // determine if we have a valid join condition
        final JoinInfo joinInfo = origJoinRel.analyzeCondition();
        if ( joinInfo.leftKeys.size() == 0 ) {
            return;
        }

        RelNode semiJoin =
                SemiJoin.create(
                        origJoinRel.getLeft(),
                        origJoinRel.getRight(),
                        origJoinRel.getCondition(),
                        joinInfo.leftKeys,
                        joinInfo.rightKeys );

        RelNode newJoinRel =
                origJoinRel.copy(
                        origJoinRel.getTraitSet(),
                        origJoinRel.getCondition(),
                        semiJoin,
                        origJoinRel.getRight(),
                        JoinRelType.INNER,
                        true );

        call.transformTo( newJoinRel );
    }
}

