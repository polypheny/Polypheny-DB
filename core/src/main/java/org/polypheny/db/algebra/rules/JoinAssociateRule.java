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
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexPermuteInputsShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Planner rule that changes a join based on the associativity rule.
 *
 * ((a JOIN b) JOIN c) &rarr; (a JOIN (b JOIN c))
 *
 * We do not need a rule to convert (a JOIN (b JOIN c)) &rarr; ((a JOIN b) JOIN c) because we have {@link JoinCommuteRule}.
 *
 * @see JoinCommuteRule
 */
public class JoinAssociateRule extends AlgOptRule {

    /**
     * The singleton.
     */
    public static final JoinAssociateRule INSTANCE = new JoinAssociateRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a JoinAssociateRule.
     */
    public JoinAssociateRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( Join.class, operand( Join.class, any() ), operand( AlgSubset.class, any() ) ),
                algBuilderFactory, null );
    }


    @Override
    public void onMatch( final AlgOptRuleCall call ) {
        final Join topJoin = call.alg( 0 );
        final Join bottomJoin = call.alg( 1 );
        final AlgNode algA = bottomJoin.getLeft();
        final AlgNode algB = bottomJoin.getRight();
        final AlgSubset algC = call.alg( 2 );
        final AlgCluster cluster = topJoin.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        if ( algC.getConvention() != algA.getConvention() ) {
            // relC could have any trait-set. But if we're matching say EnumerableConvention, we're only interested in enumerable subsets.
            return;
        }

        //        topJoin
        //        /     \
        //   bottomJoin  C
        //    /    \
        //   A      B

        final int aCount = algA.getTupleType().getFieldCount();
        final int bCount = algB.getTupleType().getFieldCount();
        final int cCount = algC.getTupleType().getFieldCount();
        final ImmutableBitSet aBitSet = ImmutableBitSet.range( 0, aCount );
        final ImmutableBitSet bBitSet = ImmutableBitSet.range( aCount, aCount + bCount );

        // If either join is not inner, we cannot proceed. (Is this too strict?)
        if ( topJoin.getJoinType() != JoinAlgType.INNER || bottomJoin.getJoinType() != JoinAlgType.INNER ) {
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
        new RexPermuteInputsShuttle( bottomMapping, algB, algC ).visitList( bottom, newBottomList );
        RexNode newBottomCondition = RexUtil.composeConjunction( rexBuilder, newBottomList );

        final Join newBottomJoin =
                bottomJoin.copy(
                        bottomJoin.getTraitSet(),
                        newBottomCondition,
                        algB,
                        algC,
                        JoinAlgType.INNER,
                        false );

        // Condition for newTopJoin consists of pieces from bottomJoin and topJoin. Field ordinals do not need to be changed.
        RexNode newTopCondition = RexUtil.composeConjunction( rexBuilder, top );
        @SuppressWarnings("SuspiciousNameCombination") final Join newTopJoin = topJoin.copy( topJoin.getTraitSet(), newTopCondition, algA, newBottomJoin, JoinAlgType.INNER, false );

        call.transformTo( newTopJoin );
    }

}

