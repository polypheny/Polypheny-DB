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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.InvalidRelException;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinInfo;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.SemiJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMdCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;


/**
 * Implementation of {@link SemiJoin} in {@link ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 */
public class EnumerableSemiJoin extends SemiJoin implements EnumerableRel {

    /**
     * Creates an EnumerableSemiJoin.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    EnumerableSemiJoin( RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, ImmutableIntList leftKeys, ImmutableIntList rightKeys ) throws InvalidRelException {
        super( cluster, traits, left, right, condition, leftKeys, rightKeys );
    }


    /**
     * Creates an EnumerableSemiJoin.
     */
    public static EnumerableSemiJoin create( RelNode left, RelNode right, RexNode condition, ImmutableIntList leftKeys, ImmutableIntList rightKeys ) {
        final RelOptCluster cluster = left.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSetOf( EnumerableConvention.INSTANCE ).replaceIfs( RelCollationTraitDef.INSTANCE, () -> RelMdCollation.enumerableSemiJoin( mq, left, right ) );
        try {
            return new EnumerableSemiJoin( cluster, traitSet, left, right, condition, leftKeys, rightKeys );
        } catch ( InvalidRelException e ) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError( e );
        }
    }


    @Override
    public SemiJoin copy( RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone ) {
        assert joinType == JoinRelType.INNER;
        final JoinInfo joinInfo = JoinInfo.of( left, right, condition );
        assert joinInfo.isEqui();
        try {
            return new EnumerableSemiJoin( getCluster(), traitSet, left, right, condition, joinInfo.leftKeys, joinInfo.rightKeys );
        } catch ( InvalidRelException e ) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError( e );
        }
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        double rowCount = mq.getRowCount( this );

        // Right-hand input is the "build", and hopefully small, input.
        final double rightRowCount = right.estimateRowCount( mq );
        final double leftRowCount = left.estimateRowCount( mq );
        if ( Double.isInfinite( leftRowCount ) ) {
            rowCount = leftRowCount;
        } else {
            rowCount += Util.nLogN( leftRowCount );
        }
        if ( Double.isInfinite( rightRowCount ) ) {
            rowCount = rightRowCount;
        } else {
            rowCount += rightRowCount;
        }
        return planner.getCostFactory().makeCost( rowCount, 0, 0 ).multiplyBy( .01d );
    }


    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        BlockBuilder builder = new BlockBuilder();
        final Result leftResult = implementor.visitChild( this, 0, (EnumerableRel) left, pref );
        Expression leftExpression = builder.append( "left", leftResult.block );
        final Result rightResult = implementor.visitChild( this, 1, (EnumerableRel) right, pref );
        Expression rightExpression = builder.append( "right", rightResult.block );
        final PhysType physType = leftResult.physType;
        return implementor.result(
                physType,
                builder.append(
                        Expressions.call(
                                BuiltInMethod.SEMI_JOIN.method,
                                Expressions.list(
                                        leftExpression,
                                        rightExpression,
                                        leftResult.physType.generateAccessor( leftKeys ),
                                        rightResult.physType.generateAccessor( rightKeys ) ) ) )
                        .toBlock() );
    }
}

