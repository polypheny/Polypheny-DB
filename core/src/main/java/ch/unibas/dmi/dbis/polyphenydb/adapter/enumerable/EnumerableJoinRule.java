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

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.rel.InvalidRelException;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinInfo;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin;
import java.util.ArrayList;
import java.util.List;


/**
 * Planner rule that converts a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin} relational expression {@link ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 */
class EnumerableJoinRule extends ConverterRule {

    EnumerableJoinRule() {
        super( LogicalJoin.class, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableJoinRule" );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        LogicalJoin join = (LogicalJoin) rel;
        List<RelNode> newInputs = new ArrayList<>();
        for ( RelNode input : join.getInputs() ) {
            if ( !(input.getConvention() instanceof EnumerableConvention) ) {
                input = convert( input, input.getTraitSet().replace( EnumerableConvention.INSTANCE ) );
            }
            newInputs.add( input );
        }
        final RelOptCluster cluster = join.getCluster();
        final RelNode left = newInputs.get( 0 );
        final RelNode right = newInputs.get( 1 );
        final JoinInfo info = JoinInfo.of( left, right, join.getCondition() );
        if ( !info.isEqui() && join.getJoinType() != JoinRelType.INNER ) {
            // EnumerableJoinRel only supports equi-join. We can put a filter on top if it is an inner join.
            try {
                return EnumerableThetaJoin.create( left, right, join.getCondition(), join.getVariablesSet(), join.getJoinType() );
            } catch ( InvalidRelException e ) {
                EnumerableRules.LOGGER.debug( e.toString() );
                return null;
            }
        }
        RelNode newRel;
        try {
            newRel = EnumerableJoin.create( left, right, info.getEquiCondition( left, right, cluster.getRexBuilder() ), info.leftKeys, info.rightKeys, join.getVariablesSet(), join.getJoinType() );
        } catch ( InvalidRelException e ) {
            EnumerableRules.LOGGER.debug( e.toString() );
            return null;
        }
        if ( !info.isEqui() ) {
            newRel = new EnumerableFilter( cluster, newRel.getTraitSet(), newRel, info.getRemaining( cluster.getRexBuilder() ) );
        }
        return newRel;
    }
}

