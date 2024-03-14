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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexPermuteInputsShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Rule that pushes the right input of a join into through the left input of the join, provided that the left input is also a join.
 *
 * Thus, {@code (A join B) join C} becomes {@code (A join C) join B}. The advantage of applying this rule is that it may be
 * possible to apply conditions earlier. For instance,
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
 * Before the rule, one join has two conditions and the other has none ({@code ON TRUE}). After the rule, each join
 * has one condition.
 */
public class JoinPushThroughJoinRule extends AlgOptRule {

    /**
     * Instance of the rule that works on logical joins only, and pushes to the right.
     */
    public static final AlgOptRule RIGHT = new JoinPushThroughJoinRule(
            "JoinPushThroughJoinRule:right",
            true, LogicalRelJoin.class,
            AlgFactories.LOGICAL_BUILDER );

    /**
     * Instance of the rule that works on logical joins only, and pushes to the left.
     */
    public static final AlgOptRule LEFT = new JoinPushThroughJoinRule(
            "JoinPushThroughJoinRule:left",
            false, LogicalRelJoin.class,
            AlgFactories.LOGICAL_BUILDER );

    private final boolean right;


    /**
     * Creates a JoinPushThroughJoinRule.
     */
    public JoinPushThroughJoinRule( String description, boolean right, Class<? extends Join> clazz, AlgBuilderFactory algBuilderFactory ) {
        super( operand( clazz, operand( clazz, any() ), operand( AlgNode.class, any() ) ), algBuilderFactory, description );
        this.right = right;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        if ( right ) {
            onMatchRight( call );
        } else {
            onMatchLeft( call );
        }
    }


    private void onMatchRight( AlgOptRuleCall call ) {
        final Join topJoin = call.alg( 0 );
        final Join bottomJoin = call.alg( 1 );
        final AlgNode algC = call.alg( 2 );
        final AlgNode algA = bottomJoin.getLeft();
        final AlgNode algB = bottomJoin.getRight();
        final AlgCluster cluster = topJoin.getCluster();

        //        topJoin
        //        /     \
        //   bottomJoin  C
        //    /    \
        //   A      B

        final int aCount = algA.getTupleType().getFieldCount();
        final int bCount = algB.getTupleType().getFieldCount();
        final int cCount = algC.getTupleType().getFieldCount();
        final ImmutableBitSet bBitSet = ImmutableBitSet.range( aCount, aCount + bCount );

        // becomes
        //
        //        newTopJoin
        //        /        \
        //   newBottomJoin  B
        //    /    \
        //   A      C

        // If either join is not inner, we cannot proceed. (Is this too strict?)
        if ( topJoin.getJoinType() != JoinAlgType.INNER || bottomJoin.getJoinType() != JoinAlgType.INNER ) {
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
        new RexPermuteInputsShuttle( bottomMapping, algA, algC ).visitList( nonIntersecting, newBottomList );
        new RexPermuteInputsShuttle( bottomMapping, algA, algC ).visitList( bottomNonIntersecting, newBottomList );
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode newBottomCondition = RexUtil.composeConjunction( rexBuilder, newBottomList );
        final Join newBottomJoin = bottomJoin.copy( bottomJoin.getTraitSet(), newBottomCondition, algA, algC, bottomJoin.getJoinType(), bottomJoin.isSemiJoinDone() );

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
        new RexPermuteInputsShuttle( topMapping, newBottomJoin, algB ).visitList( intersecting, newTopList );
        new RexPermuteInputsShuttle( topMapping, newBottomJoin, algB ).visitList( bottomIntersecting, newTopList );
        RexNode newTopCondition = RexUtil.composeConjunction( rexBuilder, newTopList );
        @SuppressWarnings("SuspiciousNameCombination") final Join newTopJoin = topJoin.copy( topJoin.getTraitSet(), newTopCondition, newBottomJoin, algB, topJoin.getJoinType(), topJoin.isSemiJoinDone() );

        assert !Mappings.isIdentity( topMapping );
        final AlgBuilder algBuilder = call.builder();
        algBuilder.push( newTopJoin );
        algBuilder.project( algBuilder.fields( topMapping ) );
        call.transformTo( algBuilder.build() );
    }


    /**
     * Similar to {@link #onMatch}, but swaps the upper sibling with the left of the two lower siblings, rather than the right.
     */
    private void onMatchLeft( AlgOptRuleCall call ) {
        final Join topJoin = call.alg( 0 );
        final Join bottomJoin = call.alg( 1 );
        final AlgNode algC = call.alg( 2 );
        final AlgNode algA = bottomJoin.getLeft();
        final AlgNode algB = bottomJoin.getRight();
        final AlgCluster cluster = topJoin.getCluster();

        //        topJoin
        //        /     \
        //   bottomJoin  C
        //    /    \
        //   A      B

        final int aCount = algA.getTupleType().getFieldCount();
        final int bCount = algB.getTupleType().getFieldCount();
        final int cCount = algC.getTupleType().getFieldCount();
        final ImmutableBitSet aBitSet = ImmutableBitSet.range( aCount );

        // becomes
        //
        //        newTopJoin
        //        /        \
        //   newBottomJoin  A
        //    /    \
        //   C      B

        // If either join is not inner, we cannot proceed. (Is this too strict?)
        if ( topJoin.getJoinType() != JoinAlgType.INNER || bottomJoin.getJoinType() != JoinAlgType.INNER ) {
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
        new RexPermuteInputsShuttle( bottomMapping, algC, algB ).visitList( nonIntersecting, newBottomList );
        new RexPermuteInputsShuttle( bottomMapping, algC, algB ).visitList( bottomNonIntersecting, newBottomList );
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode newBottomCondition = RexUtil.composeConjunction( rexBuilder, newBottomList );
        final Join newBottomJoin = bottomJoin.copy( bottomJoin.getTraitSet(), newBottomCondition, algC, algB, bottomJoin.getJoinType(), bottomJoin.isSemiJoinDone() );

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
        new RexPermuteInputsShuttle( topMapping, newBottomJoin, algA ).visitList( intersecting, newTopList );
        new RexPermuteInputsShuttle( topMapping, newBottomJoin, algA ).visitList( bottomIntersecting, newTopList );
        RexNode newTopCondition = RexUtil.composeConjunction( rexBuilder, newTopList );
        @SuppressWarnings("SuspiciousNameCombination") final Join newTopJoin = topJoin.copy( topJoin.getTraitSet(), newTopCondition, newBottomJoin, algA, topJoin.getJoinType(), topJoin.isSemiJoinDone() );

        final AlgBuilder algBuilder = call.builder();
        algBuilder.push( newTopJoin );
        algBuilder.project( algBuilder.fields( topMapping ) );
        call.transformTo( algBuilder.build() );
    }


    /**
     * Splits a condition into conjunctions that do or do not intersect with a given bit set.
     */
    static void split( RexNode condition, ImmutableBitSet bitSet, List<RexNode> intersecting, List<RexNode> nonIntersecting ) {
        for ( RexNode node : AlgOptUtil.conjunctions( condition ) ) {
            ImmutableBitSet inputBitSet = AlgOptUtil.InputFinder.bits( node );
            if ( bitSet.intersects( inputBitSet ) ) {
                intersecting.add( node );
            } else {
                nonIntersecting.add( node );
            }
        }
    }

}
