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
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes down expressions in "equal" join condition.
 *
 * For example, given "emp JOIN dept ON emp.deptno + 1 = dept.deptno", adds a project above "emp" that computes the expression
 * "emp.deptno + 1". The resulting join condition is a simple combination of AND, equals, and input fields, plus the remaining non-equal conditions.
 */
public class JoinPushExpressionsRule extends AlgOptRule {

    public static final JoinPushExpressionsRule INSTANCE = new JoinPushExpressionsRule( Join.class, AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a JoinPushExpressionsRule.
     */
    public JoinPushExpressionsRule( Class<? extends Join> clazz, AlgBuilderFactory algBuilderFactory ) {
        super( operand( clazz, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        Join join = call.alg( 0 );

        // Push expression in join condition into Project below Join.
        AlgNode newJoin = AlgOptUtil.pushDownJoinConditions( join, call.builder() );

        // If the join is the same, we bail out
        if ( newJoin instanceof Join ) {
            final RexNode newCondition = ((Join) newJoin).getCondition();
            if ( join.getCondition().equals( newCondition ) ) {
                return;
            }
        }

        call.transformTo( newJoin );
    }

}
