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


import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.volcano.RelSubset;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Join;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexPermuteInputsShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.mapping.Mappings;
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


    @Override
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

