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
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexPermuteInputsShuttle;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings;
import java.util.ArrayList;
import java.util.List;


/**
 * Rule that pushes the right input of a join into through the left input of the join, provided that the left input is also a join.
 *
 * Thus, {@code (A join B) join C} becomes {@code (A join C) join B}. The advantage of applying this rule is that it may be possible to apply conditions earlier. For instance,
 *
 * <blockquote>
 * <pre>(sales as s join product_class as pc on true)
 * join product as p
 * on s.product_id = p.product_id
 * and p.product_class_id = pc.product_class_id</pre></blockquote>
 *
 * becomes
 *
 * <blockquote>
 * <pre>(sales as s join product as p on s.product_id = p.product_id)
 * join product_class as pc
 * on p.product_class_id = pc.product_class_id</pre></blockquote>
 *
 * Before the rule, one join has two conditions and the other has none ({@code ON TRUE}). After the rule, each join has one condition.
 */
public class JoinPushThroughJoinRule extends RelOptRule {

    /**
     * Instance of the rule that works on logical joins only, and pushes to the right.
     */
    public static final RelOptRule RIGHT = new JoinPushThroughJoinRule( "JoinPushThroughJoinRule:right", true, LogicalJoin.class, RelFactories.LOGICAL_BUILDER );

    /**
     * Instance of the rule that works on logical joins only, and pushes to the left.
     */
    public static final RelOptRule LEFT = new JoinPushThroughJoinRule( "JoinPushThroughJoinRule:left", false, LogicalJoin.class, RelFactories.LOGICAL_BUILDER );

    private final boolean right;


    /**
     * Creates a JoinPushThroughJoinRule.
     */
    public JoinPushThroughJoinRule( String description, boolean right, Class<? extends Join> clazz, RelBuilderFactory relBuilderFactory ) {
        super(
                operand( clazz, operand( clazz, any() ), operand( RelNode.class, any() ) ),
                relBuilderFactory, description );
        this.right = right;
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        if ( right ) {
            onMatchRight( call );
        } else {
            onMatchLeft( call );
        }
    }


    private void onMatchRight( RelOptRuleCall call ) {
        final Join topJoin = call.rel( 0 );
        final Join bottomJoin = call.rel( 1 );
        final RelNode relC = call.rel( 2 );
        final RelNode relA = bottomJoin.getLeft();
        final RelNode relB = bottomJoin.getRight();
        final RelOptCluster cluster = topJoin.getCluster();

        //        topJoin
        //        /     \
        //   bottomJoin  C
        //    /    \
        //   A      B

        final int aCount = relA.getRowType().getFieldCount();
        final int bCount = relB.getRowType().getFieldCount();
        final int cCount = relC.getRowType().getFieldCount();
        final ImmutableBitSet bBitSet = ImmutableBitSet.range( aCount, aCount + bCount );

        // becomes
        //
        //        newTopJoin
        //        /        \
        //   newBottomJoin  B
        //    /    \
        //   A      C

        // If either join is not inner, we cannot proceed. (Is this too strict?)
        if ( topJoin.getJoinType() != JoinRelType.INNER || bottomJoin.getJoinType() != JoinRelType.INNER ) {
            return;
        }

        // Split the condition of topJoin into a conjunction. Each of the parts that does not use columns from B can be pushed down.
        final List<RexNode> intersecting = new ArrayList<>();
        final List<RexNode> nonIntersecting = new ArrayList<>();
        split( topJoin.getCondition(), bBitSet, intersecting, nonIntersecting );

        // If there's nothing to push down, it's not worth proceeding.
        if ( nonIntersecting.isEmpty() ) {
            return;
        }

        // Split the condition of bottomJoin into a conjunction. Each of the
        // parts that use columns from B will need to be pulled up.
        final List<RexNode> bottomIntersecting = new ArrayList<>();
        final List<RexNode> bottomNonIntersecting = new ArrayList<>();
        split( bottomJoin.getCondition(), bBitSet, bottomIntersecting, bottomNonIntersecting );

        // target: | A       | C      |
        // source: | A       | B | C      |
        final Mappings.TargetMapping bottomMapping = Mappings.createShiftMapping( aCount + bCount + cCount, 0, 0, aCount, aCount, aCount + bCount, cCount );
        final List<RexNode> newBottomList = new ArrayList<>();
        new RexPermuteInputsShuttle( bottomMapping, relA, relC ).visitList( nonIntersecting, newBottomList );
        new RexPermuteInputsShuttle( bottomMapping, relA, relC ).visitList( bottomNonIntersecting, newBottomList );
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode newBottomCondition = RexUtil.composeConjunction( rexBuilder, newBottomList );
        final Join newBottomJoin = bottomJoin.copy( bottomJoin.getTraitSet(), newBottomCondition, relA, relC, bottomJoin.getJoinType(), bottomJoin.isSemiJoinDone() );

        // target: | A       | C      | B |
        // source: | A       | B | C      |
        final Mappings.TargetMapping topMapping =
                Mappings.createShiftMapping(
                        aCount + bCount + cCount,
                        0,
                        0,
                        aCount,
                        aCount + cCount,
                        aCount,
                        bCount,
                        aCount,
                        aCount + bCount,
                        cCount );
        final List<RexNode> newTopList = new ArrayList<>();
        new RexPermuteInputsShuttle( topMapping, newBottomJoin, relB ).visitList( intersecting, newTopList );
        new RexPermuteInputsShuttle( topMapping, newBottomJoin, relB ).visitList( bottomIntersecting, newTopList );
        RexNode newTopCondition = RexUtil.composeConjunction( rexBuilder, newTopList );
        @SuppressWarnings("SuspiciousNameCombination") final Join newTopJoin = topJoin.copy( topJoin.getTraitSet(), newTopCondition, newBottomJoin, relB, topJoin.getJoinType(), topJoin.isSemiJoinDone() );

        assert !Mappings.isIdentity( topMapping );
        final RelBuilder relBuilder = call.builder();
        relBuilder.push( newTopJoin );
        relBuilder.project( relBuilder.fields( topMapping ) );
        call.transformTo( relBuilder.build() );
    }


    /**
     * Similar to {@link #onMatch}, but swaps the upper sibling with the left of the two lower siblings, rather than the right.
     */
    private void onMatchLeft( RelOptRuleCall call ) {
        final Join topJoin = call.rel( 0 );
        final Join bottomJoin = call.rel( 1 );
        final RelNode relC = call.rel( 2 );
        final RelNode relA = bottomJoin.getLeft();
        final RelNode relB = bottomJoin.getRight();
        final RelOptCluster cluster = topJoin.getCluster();

        //        topJoin
        //        /     \
        //   bottomJoin  C
        //    /    \
        //   A      B

        final int aCount = relA.getRowType().getFieldCount();
        final int bCount = relB.getRowType().getFieldCount();
        final int cCount = relC.getRowType().getFieldCount();
        final ImmutableBitSet aBitSet = ImmutableBitSet.range( aCount );

        // becomes
        //
        //        newTopJoin
        //        /        \
        //   newBottomJoin  A
        //    /    \
        //   C      B

        // If either join is not inner, we cannot proceed. (Is this too strict?)
        if ( topJoin.getJoinType() != JoinRelType.INNER || bottomJoin.getJoinType() != JoinRelType.INNER ) {
            return;
        }

        // Split the condition of topJoin into a conjunction. Each of the parts that does not use columns from A can be pushed down.
        final List<RexNode> intersecting = new ArrayList<>();
        final List<RexNode> nonIntersecting = new ArrayList<>();
        split( topJoin.getCondition(), aBitSet, intersecting, nonIntersecting );

        // If there's nothing to push down, it's not worth proceeding.
        if ( nonIntersecting.isEmpty() ) {
            return;
        }

        // Split the condition of bottomJoin into a conjunction. Each of the parts that use columns from A will need to be pulled up.
        final List<RexNode> bottomIntersecting = new ArrayList<>();
        final List<RexNode> bottomNonIntersecting = new ArrayList<>();
        split( bottomJoin.getCondition(), aBitSet, bottomIntersecting, bottomNonIntersecting );

        // target: | C      | B |
        // source: | A       | B | C      |
        final Mappings.TargetMapping bottomMapping =
                Mappings.createShiftMapping(
                        aCount + bCount + cCount,
                        cCount,
                        aCount,
                        bCount,
                        0,
                        aCount + bCount,
                        cCount );
        final List<RexNode> newBottomList = new ArrayList<>();
        new RexPermuteInputsShuttle( bottomMapping, relC, relB ).visitList( nonIntersecting, newBottomList );
        new RexPermuteInputsShuttle( bottomMapping, relC, relB ).visitList( bottomNonIntersecting, newBottomList );
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode newBottomCondition = RexUtil.composeConjunction( rexBuilder, newBottomList );
        final Join newBottomJoin = bottomJoin.copy( bottomJoin.getTraitSet(), newBottomCondition, relC, relB, bottomJoin.getJoinType(), bottomJoin.isSemiJoinDone() );

        // target: | C      | B | A       |
        // source: | A       | B | C      |
        final Mappings.TargetMapping topMapping =
                Mappings.createShiftMapping(
                        aCount + bCount + cCount,
                        cCount + bCount,
                        0,
                        aCount,
                        cCount,
                        aCount,
                        bCount,
                        0,
                        aCount + bCount,
                        cCount );
        final List<RexNode> newTopList = new ArrayList<>();
        new RexPermuteInputsShuttle( topMapping, newBottomJoin, relA ).visitList( intersecting, newTopList );
        new RexPermuteInputsShuttle( topMapping, newBottomJoin, relA ).visitList( bottomIntersecting, newTopList );
        RexNode newTopCondition = RexUtil.composeConjunction( rexBuilder, newTopList );
        @SuppressWarnings("SuspiciousNameCombination") final Join newTopJoin = topJoin.copy( topJoin.getTraitSet(), newTopCondition, newBottomJoin, relA, topJoin.getJoinType(), topJoin.isSemiJoinDone() );

        final RelBuilder relBuilder = call.builder();
        relBuilder.push( newTopJoin );
        relBuilder.project( relBuilder.fields( topMapping ) );
        call.transformTo( relBuilder.build() );
    }


    /**
     * Splits a condition into conjunctions that do or do not intersect with a given bit set.
     */
    static void split( RexNode condition, ImmutableBitSet bitSet, List<RexNode> intersecting, List<RexNode> nonIntersecting ) {
        for ( RexNode node : RelOptUtil.conjunctions( condition ) ) {
            ImmutableBitSet inputBitSet = RelOptUtil.InputFinder.bits( node );
            if ( bitSet.intersects( inputBitSet ) ) {
                intersecting.add( node );
            } else {
                nonIntersecting.add( node );
            }
        }
    }
}
