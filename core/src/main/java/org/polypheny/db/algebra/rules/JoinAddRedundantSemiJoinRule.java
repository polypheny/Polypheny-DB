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
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.JoinInfo;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Rule to add a semi-join into a join. Transformation is as follows:
 *
 * LogicalJoin(X, Y) &rarr; LogicalJoin(SemiJoin(X, Y), Y)
 *
 * The constructor is parameterized to allow any sub-class of {@link org.polypheny.db.algebra.core.Join}, not just {@link LogicalRelJoin}.
 */
public class JoinAddRedundantSemiJoinRule extends AlgOptRule {

    public static final JoinAddRedundantSemiJoinRule INSTANCE = new JoinAddRedundantSemiJoinRule( LogicalRelJoin.class, AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates an JoinAddRedundantSemiJoinRule.
     */
    public JoinAddRedundantSemiJoinRule( Class<? extends Join> clazz, AlgBuilderFactory algBuilderFactory ) {
        super( operand( clazz, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        Join origJoinAlg = call.alg( 0 );
        if ( origJoinAlg.isSemiJoinDone() ) {
            return;
        }

        // can't process outer joins using semijoins
        if ( origJoinAlg.getJoinType() != JoinAlgType.INNER ) {
            return;
        }

        // determine if we have a valid join condition
        final JoinInfo joinInfo = origJoinAlg.analyzeCondition();
        if ( joinInfo.leftKeys.isEmpty() ) {
            return;
        }

        AlgNode semiJoin =
                SemiJoin.create(
                        origJoinAlg.getLeft(),
                        origJoinAlg.getRight(),
                        origJoinAlg.getCondition(),
                        joinInfo.leftKeys,
                        joinInfo.rightKeys );

        AlgNode newJoinAlg =
                origJoinAlg.copy(
                        origJoinAlg.getTraitSet(),
                        origJoinAlg.getCondition(),
                        semiJoin,
                        origJoinAlg.getRight(),
                        JoinAlgType.INNER,
                        true );

        call.transformTo( newJoinAlg );
    }

}

