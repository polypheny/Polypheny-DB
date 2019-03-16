/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.rules;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollations;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinInfo;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalSort;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMdUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Planner rule that pushes a {@link Sort} past a {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Join}.
 *
 * At the moment, we only consider left/right outer joins. However, an extension for full outer joins for this rule could be envisioned.
 * Special attention should be paid to null values for correctness issues.
 */
public class SortJoinTransposeRule extends RelOptRule {

    public static final SortJoinTransposeRule INSTANCE = new SortJoinTransposeRule( LogicalSort.class, LogicalJoin.class, RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a SortJoinTransposeRule.
     */
    @Deprecated // to be removed before 2.0
    public SortJoinTransposeRule( Class<? extends Sort> sortClass, Class<? extends Join> joinClass ) {
        this( sortClass, joinClass, RelFactories.LOGICAL_BUILDER );
    }


    /**
     * Creates a SortJoinTransposeRule.
     */
    public SortJoinTransposeRule( Class<? extends Sort> sortClass, Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory ) {
        super(
                operand( sortClass, operand( joinClass, any() ) ),
                relBuilderFactory, null );
    }


    @Override
    public boolean matches( RelOptRuleCall call ) {
        final Sort sort = call.rel( 0 );
        final Join join = call.rel( 1 );
        final RelMetadataQuery mq = call.getMetadataQuery();
        final JoinInfo joinInfo = JoinInfo.of( join.getLeft(), join.getRight(), join.getCondition() );

        // 1) If join is not a left or right outer, we bail out
        // 2) If sort is not a trivial order-by, and if there is any sort column that is not part of the input where the sort is pushed, we bail out
        // 3) If sort has an offset, and if the non-preserved side of the join is not count-preserving against the join condition, we bail out
        if ( join.getJoinType() == JoinRelType.LEFT ) {
            if ( sort.getCollation() != RelCollations.EMPTY ) {
                for ( RelFieldCollation relFieldCollation : sort.getCollation().getFieldCollations() ) {
                    if ( relFieldCollation.getFieldIndex() >= join.getLeft().getRowType().getFieldCount() ) {
                        return false;
                    }
                }
            }
            if ( sort.offset != null && !RelMdUtil.areColumnsDefinitelyUnique( mq, join.getRight(), joinInfo.rightSet() ) ) {
                return false;
            }
        } else if ( join.getJoinType() == JoinRelType.RIGHT ) {
            if ( sort.getCollation() != RelCollations.EMPTY ) {
                for ( RelFieldCollation relFieldCollation : sort.getCollation().getFieldCollations() ) {
                    if ( relFieldCollation.getFieldIndex() < join.getLeft().getRowType().getFieldCount() ) {
                        return false;
                    }
                }
            }
            if ( sort.offset != null && !RelMdUtil.areColumnsDefinitelyUnique( mq, join.getLeft(), joinInfo.leftSet() ) ) {
                return false;
            }
        } else {
            return false;
        }

        return true;
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Sort sort = call.rel( 0 );
        final Join join = call.rel( 1 );

        // We create a new sort operator on the corresponding input
        final RelNode newLeftInput;
        final RelNode newRightInput;
        final RelMetadataQuery mq = call.getMetadataQuery();
        if ( join.getJoinType() == JoinRelType.LEFT ) {
            // If the input is already sorted and we are not reducing the number of tuples, we bail out
            if ( RelMdUtil.checkInputForCollationAndLimit( mq, join.getLeft(), sort.getCollation(), sort.offset, sort.fetch ) ) {
                return;
            }
            newLeftInput = sort.copy( sort.getTraitSet(), join.getLeft(), sort.getCollation(), sort.offset, sort.fetch );
            newRightInput = join.getRight();
        } else {
            final RelCollation rightCollation = RelCollationTraitDef.INSTANCE.canonize( RelCollations.shift( sort.getCollation(), -join.getLeft().getRowType().getFieldCount() ) );
            // If the input is already sorted and we are not reducing the number of tuples, we bail out
            if ( RelMdUtil.checkInputForCollationAndLimit( mq, join.getRight(), rightCollation, sort.offset, sort.fetch ) ) {
                return;
            }
            newLeftInput = join.getLeft();
            newRightInput =
                    sort.copy(
                            sort.getTraitSet().replace( rightCollation ),
                            join.getRight(),
                            rightCollation,
                            sort.offset,
                            sort.fetch );
        }
        // We copy the join and the top sort operator
        final RelNode joinCopy = join.copy( join.getTraitSet(), join.getCondition(), newLeftInput, newRightInput, join.getJoinType(), join.isSemiJoinDone() );
        final RelNode sortCopy = sort.copy( sort.getTraitSet(), joinCopy, sort.getCollation(), sort.offset, sort.fetch );

        call.transformTo( sortCopy );
    }

}

