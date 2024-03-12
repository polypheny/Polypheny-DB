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


import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.JoinInfo;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.metadata.AlgMdUtil;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes a {@link Sort} past a {@link org.polypheny.db.algebra.core.Join}.
 *
 * At the moment, we only consider left/right outer joins. However, an extension for full outer joins for this rule could be envisioned.
 * Special attention should be paid to null values for correctness issues.
 */
public class SortJoinTransposeRule extends AlgOptRule {

    public static final SortJoinTransposeRule INSTANCE = new SortJoinTransposeRule( LogicalRelSort.class, LogicalRelJoin.class, AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a SortJoinTransposeRule.
     */
    public SortJoinTransposeRule( Class<? extends Sort> sortClass, Class<? extends Join> joinClass, AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( sortClass, operand( joinClass, any() ) ),
                algBuilderFactory, null );
    }


    @Override
    public boolean matches( AlgOptRuleCall call ) {
        final Sort sort = call.alg( 0 );
        final Join join = call.alg( 1 );
        final AlgMetadataQuery mq = call.getMetadataQuery();
        final JoinInfo joinInfo = JoinInfo.of( join.getLeft(), join.getRight(), join.getCondition() );

        // 1) If join is not a left or right outer, we bail out
        // 2) If sort is not a trivial order-by, and if there is any sort column that is not part of the input where the sort is pushed, we bail out
        // 3) If sort has an offset, and if the non-preserved side of the join is not count-preserving against the join condition, we bail out
        if ( join.getJoinType() == JoinAlgType.LEFT ) {
            if ( sort.getCollation() != AlgCollations.EMPTY ) {
                for ( AlgFieldCollation algFieldCollation : sort.getCollation().getFieldCollations() ) {
                    if ( algFieldCollation.getFieldIndex() >= join.getLeft().getTupleType().getFieldCount() ) {
                        return false;
                    }
                }
            }
            if ( sort.offset != null && !AlgMdUtil.areColumnsDefinitelyUnique( mq, join.getRight(), joinInfo.rightSet() ) ) {
                return false;
            }
        } else if ( join.getJoinType() == JoinAlgType.RIGHT ) {
            if ( sort.getCollation() != AlgCollations.EMPTY ) {
                for ( AlgFieldCollation algFieldCollation : sort.getCollation().getFieldCollations() ) {
                    if ( algFieldCollation.getFieldIndex() < join.getLeft().getTupleType().getFieldCount() ) {
                        return false;
                    }
                }
            }
            if ( sort.offset != null && !AlgMdUtil.areColumnsDefinitelyUnique( mq, join.getLeft(), joinInfo.leftSet() ) ) {
                return false;
            }
        } else {
            return false;
        }

        return true;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Sort sort = call.alg( 0 );
        final Join join = call.alg( 1 );

        // We create a new sort operator on the corresponding input
        final AlgNode newLeftInput;
        final AlgNode newRightInput;
        final AlgMetadataQuery mq = call.getMetadataQuery();
        if ( join.getJoinType() == JoinAlgType.LEFT ) {
            // If the input is already sorted and we are not reducing the number of tuples, we bail out
            if ( AlgMdUtil.checkInputForCollationAndLimit( mq, join.getLeft(), sort.getCollation(), sort.offset, sort.fetch ) ) {
                return;
            }
            newLeftInput = sort.copy( sort.getTraitSet(), join.getLeft(), sort.getCollation(), null, sort.offset, sort.fetch );
            newRightInput = join.getRight();
        } else {
            final AlgCollation rightCollation = AlgCollationTraitDef.INSTANCE.canonize( AlgCollations.shift( sort.getCollation(), -join.getLeft().getTupleType().getFieldCount() ) );
            // If the input is already sorted and we are not reducing the number of tuples, we bail out
            if ( AlgMdUtil.checkInputForCollationAndLimit( mq, join.getRight(), rightCollation, sort.offset, sort.fetch ) ) {
                return;
            }
            newLeftInput = join.getLeft();
            newRightInput =
                    sort.copy(
                            sort.getTraitSet().replace( rightCollation ),
                            join.getRight(),
                            rightCollation,
                            null,
                            sort.offset,
                            sort.fetch );
        }
        // We copy the join and the top sort operator
        final AlgNode joinCopy = join.copy( join.getTraitSet(), join.getCondition(), newLeftInput, newRightInput, join.getJoinType(), join.isSemiJoinDone() );
        final AlgNode sortCopy = sort.copy( sort.getTraitSet(), joinCopy, sort.getCollation(), null, sort.offset, sort.fetch );

        call.transformTo( sortCopy );
    }

}

