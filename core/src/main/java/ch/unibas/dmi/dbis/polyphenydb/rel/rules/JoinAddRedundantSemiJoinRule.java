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
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinInfo;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.SemiJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Rule to add a semi-join into a join. Transformation is as follows:
 *
 * LogicalJoin(X, Y) &rarr; LogicalJoin(SemiJoin(X, Y), Y)
 *
 * The constructor is parameterized to allow any sub-class of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Join}, not just {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin}.
 */
public class JoinAddRedundantSemiJoinRule extends RelOptRule {

    public static final JoinAddRedundantSemiJoinRule INSTANCE = new JoinAddRedundantSemiJoinRule( LogicalJoin.class, RelFactories.LOGICAL_BUILDER );


    /**
     * Creates an JoinAddRedundantSemiJoinRule.
     */
    public JoinAddRedundantSemiJoinRule( Class<? extends Join> clazz, RelBuilderFactory relBuilderFactory ) {
        super( operand( clazz, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        Join origJoinRel = call.rel( 0 );
        if ( origJoinRel.isSemiJoinDone() ) {
            return;
        }

        // can't process outer joins using semijoins
        if ( origJoinRel.getJoinType() != JoinRelType.INNER ) {
            return;
        }

        // determine if we have a valid join condition
        final JoinInfo joinInfo = origJoinRel.analyzeCondition();
        if ( joinInfo.leftKeys.size() == 0 ) {
            return;
        }

        RelNode semiJoin =
                SemiJoin.create(
                        origJoinRel.getLeft(),
                        origJoinRel.getRight(),
                        origJoinRel.getCondition(),
                        joinInfo.leftKeys,
                        joinInfo.rightKeys );

        RelNode newJoinRel =
                origJoinRel.copy(
                        origJoinRel.getTraitSet(),
                        origJoinRel.getCondition(),
                        semiJoin,
                        origJoinRel.getRight(),
                        JoinRelType.INNER,
                        true );

        call.transformTo( newJoinRel );
    }
}

