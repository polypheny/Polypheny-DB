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
import org.polypheny.db.plan.RelOptRuleOperand;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.core.Join;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Rule to convert an {@link Join inner join} to a {@link Filter filter} on top of a {@link Join cartesian inner join}.
 *
 * One benefit of this transformation is that after it, the join condition can be combined with conditions and expressions above the join. It also makes the <code>FennelCartesianJoinRule</code> applicable.
 *
 * The constructor is parameterized to allow any sub-class of {@link Join}.
 */
public abstract class AbstractJoinExtractFilterRule extends RelOptRule {

    /**
     * Creates an AbstractJoinExtractFilterRule.
     */
    protected AbstractJoinExtractFilterRule( RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description ) {
        super( operand, relBuilderFactory, description );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Join join = call.rel( 0 );

        if ( join.getJoinType() != JoinRelType.INNER ) {
            return;
        }

        if ( join.getCondition().isAlwaysTrue() ) {
            return;
        }

        if ( !join.getSystemFieldList().isEmpty() ) {
            // FIXME Enable this rule for joins with system fields
            return;
        }

        final RelBuilder builder = call.builder();

        // NOTE jvs 14-Mar-2006:  See JoinCommuteRule for why we preserve attribute semiJoinDone here.

        final RelNode cartesianJoin =
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

