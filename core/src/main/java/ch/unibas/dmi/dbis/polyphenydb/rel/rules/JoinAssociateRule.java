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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.RelSubset;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexPermuteInputsShuttle;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings;
import java.util.ArrayList;
import java.util.List;


/**
 * Planner rule that changes a join based on the associativity rule.
 *
 * ((a JOIN b) JOIN c) &rarr; (a JOIN (b JOIN c))
 *
 * We do not need a rule to convert (a JOIN (b JOIN c)) &rarr; ((a JOIN b) JOIN c) because we have {@link JoinCommuteRule}.
 *
 * @see JoinCommuteRule
 */
public class JoinAssociateRule extends RelOptRule {

    /**
     * The singleton.
     */
    public static final JoinAssociateRule INSTANCE = new JoinAssociateRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a JoinAssociateRule.
     */
    public JoinAssociateRule( RelBuilderFactory relBuilderFactory ) {
        super(
                operand( Join.class, operand( Join.class, any() ), operand( RelSubset.class, any() ) ),
                relBuilderFactory, null );
    }


    public void onMatch( final RelOptRuleCall call ) {
        final Join topJoin = call.rel( 0 );
        final Join bottomJoin = call.rel( 1 );
        final RelNode relA = bottomJoin.getLeft();
        final RelNode relB = bottomJoin.getRight();
        final RelSubset relC = call.rel( 2 );
        final RelOptCluster cluster = topJoin.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        if ( relC.getConvention() != relA.getConvention() ) {
            // relC could have any trait-set. But if we're matching say EnumerableConvention, we're only interested in enumerable subsets.
            return;
        }

        //        topJoin
        //        /     \
        //   bottomJoin  C
        //    /    \
        //   A      B

        final int aCount = relA.getRowType().getFieldCount();
        final int bCount = relB.getRowType().getFieldCount();
        final int cCount = relC.getRowType().getFieldCount();
        final ImmutableBitSet aBitSet = ImmutableBitSet.range( 0, aCount );
        final ImmutableBitSet bBitSet = ImmutableBitSet.range( aCount, aCount + bCount );

        if ( !topJoin.getSystemFieldList().isEmpty() ) {
            // FIXME Enable this rule for joins with system fields
            return;
        }

        // If either join is not inner, we cannot proceed. (Is this too strict?)
        if ( topJoin.getJoinType() != JoinRelType.INNER || bottomJoin.getJoinType() != JoinRelType.INNER ) {
            return;
        }

        // Goal is to transform to
        //
        //       newTopJoin
        //        /     \
        //       A   newBottomJoin
        //               /    \
        //              B      C

        // Split the condition of topJoin and bottomJoin into a conjunctions. A condition can be pushed down if it does not use columns from A.
        final List<RexNode> top = new ArrayList<>();
        final List<RexNode> bottom = new ArrayList<>();
        JoinPushThroughJoinRule.split( topJoin.getCondition(), aBitSet, top, bottom );
        JoinPushThroughJoinRule.split( bottomJoin.getCondition(), aBitSet, top, bottom );

        // Mapping for moving conditions from topJoin or bottomJoin to newBottomJoin.
        // target: | B | C      |
        // source: | A       | B | C      |
        final Mappings.TargetMapping bottomMapping =
                Mappings.createShiftMapping(
                        aCount + bCount + cCount,
                        0,
                        aCount,
                        bCount,
                        bCount,
                        aCount + bCount,
                        cCount );
        final List<RexNode> newBottomList = new ArrayList<>();
        new RexPermuteInputsShuttle( bottomMapping, relB, relC ).visitList( bottom, newBottomList );
        RexNode newBottomCondition = RexUtil.composeConjunction( rexBuilder, newBottomList );

        final Join newBottomJoin =
                bottomJoin.copy(
                        bottomJoin.getTraitSet(),
                        newBottomCondition,
                        relB,
                        relC,
                        JoinRelType.INNER,
                        false );

        // Condition for newTopJoin consists of pieces from bottomJoin and topJoin. Field ordinals do not need to be changed.
        RexNode newTopCondition = RexUtil.composeConjunction( rexBuilder, top );
        @SuppressWarnings("SuspiciousNameCombination") final Join newTopJoin = topJoin.copy( topJoin.getTraitSet(), newTopCondition, relA, newBottomJoin, JoinRelType.INNER, false );

        call.transformTo( newTopJoin );
    }
}

