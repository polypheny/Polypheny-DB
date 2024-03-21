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
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Rule to convert an {@link Join inner join} to a {@link Filter filter} on top of a {@link Join cartesian inner join}.
 * <p>
 * One benefit of this transformation is that after it, the join condition can be combined with conditions and expressions above the join. It also makes the <code>FennelCartesianJoinRule</code> applicable.
 * <p>
 * The constructor is parameterized to allow any sub-class of {@link Join}.
 */
public abstract class AbstractJoinExtractFilterRule extends AlgOptRule {

    /**
     * Creates an AbstractJoinExtractFilterRule.
     */
    protected AbstractJoinExtractFilterRule( AlgOptRuleOperand operand, AlgBuilderFactory algBuilderFactory, String description ) {
        super( operand, algBuilderFactory, description );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Join join = call.alg( 0 );

        if ( join.getJoinType() != JoinAlgType.INNER ) {
            return;
        }

        if ( join.getCondition().isAlwaysTrue() ) {
            return;
        }

        final AlgBuilder builder = call.builder();

        // NOTE jvs 14-Mar-2006:  See JoinCommuteRule for why we preserve attribute semiJoinDone here.

        final AlgNode cartesianJoin =
                join.copy(
                        join.getTraitSet(),
                        builder.literal( true ),
                        join.getLeft(),
                        join.getRight(),
                        join.getJoinType(),
                        join.isSemiJoinDone() );

        builder.push( cartesianJoin ).filter( join.getCondition() );

        call.transformTo( builder.build() );
    }

}

