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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes a
 * {@link org.polypheny.db.algebra.core.Join}
 * past a non-distinct {@link org.polypheny.db.algebra.core.Union}.
 */
public class JoinUnionTransposeRule extends AlgOptRule {

    public static final JoinUnionTransposeRule LEFT_UNION =
            new JoinUnionTransposeRule(
                    operand( Join.class, operand( Union.class, any() ), operand( AlgNode.class, any() ) ),
                    AlgFactories.LOGICAL_BUILDER,
                    "JoinUnionTransposeRule(Union-Other)" );

    public static final JoinUnionTransposeRule RIGHT_UNION =
            new JoinUnionTransposeRule(
                    operand( Join.class, operand( AlgNode.class, any() ), operand( Union.class, any() ) ),
                    AlgFactories.LOGICAL_BUILDER,
                    "JoinUnionTransposeRule(Other-Union)" );


    /**
     * Creates a JoinUnionTransposeRule.
     *
     * @param operand root operand, must not be null
     * @param description Description, or null to guess description
     * @param algBuilderFactory Builder for relational expressions
     */
    public JoinUnionTransposeRule( AlgOptRuleOperand operand, AlgBuilderFactory algBuilderFactory, String description ) {
        super( operand, algBuilderFactory, description );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Join join = call.alg( 0 );
        final Union unionAlg;
        AlgNode otherInput;
        boolean unionOnLeft;
        if ( call.alg( 1 ) instanceof Union ) {
            unionAlg = call.alg( 1 );
            otherInput = call.alg( 2 );
            unionOnLeft = true;
        } else {
            otherInput = call.alg( 1 );
            unionAlg = call.alg( 2 );
            unionOnLeft = false;
        }
        if ( !unionAlg.all ) {
            return;
        }
        if ( !join.getVariablesSet().isEmpty() ) {
            return;
        }
        // The UNION ALL cannot be on the null generating side of an outer join (otherwise we might generate incorrect rows for the other side for join keys which lack a match in one or both branches of the union)
        if ( unionOnLeft ) {
            if ( join.getJoinType().generatesNullsOnLeft() ) {
                return;
            }
        } else {
            if ( join.getJoinType().generatesNullsOnRight() ) {
                return;
            }
        }
        List<AlgNode> newUnionInputs = new ArrayList<>();
        for ( AlgNode input : unionAlg.getInputs() ) {
            AlgNode joinLeft;
            AlgNode joinRight;
            if ( unionOnLeft ) {
                joinLeft = input;
                joinRight = otherInput;
            } else {
                joinLeft = otherInput;
                joinRight = input;
            }
            newUnionInputs.add(
                    join.copy(
                            join.getTraitSet(),
                            join.getCondition(),
                            joinLeft,
                            joinRight,
                            join.getJoinType(),
                            join.isSemiJoinDone() ) );
        }
        final SetOp newUnionRel = unionAlg.copy( unionAlg.getTraitSet(), newUnionInputs, true );
        call.transformTo( newUnionRel );
    }

}

