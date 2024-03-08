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

package org.polypheny.db.algebra.enumerable;


import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.JoinInfo;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link SemiJoin} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableSemiJoin extends SemiJoin implements EnumerableAlg {

    /**
     * Creates an EnumerableSemiJoin.
     * <p>
     * Use {@link #create} unless you know what you're doing.
     */
    EnumerableSemiJoin( AlgCluster cluster, AlgTraitSet traits, AlgNode left, AlgNode right, RexNode condition, ImmutableList<Integer> leftKeys, ImmutableList<Integer> rightKeys ) throws InvalidAlgException {
        super( cluster, traits, left, right, condition, leftKeys, rightKeys );
    }


    /**
     * Creates an EnumerableSemiJoin.
     */
    public static EnumerableSemiJoin create( AlgNode left, AlgNode right, RexNode condition, ImmutableList<Integer> leftKeys, ImmutableList<Integer> rightKeys ) {
        final AlgCluster cluster = left.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet = cluster.traitSetOf( EnumerableConvention.INSTANCE ).replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.enumerableSemiJoin( mq, left, right ) );
        try {
            return new EnumerableSemiJoin( cluster, traitSet, left, right, condition, leftKeys, rightKeys );
        } catch ( InvalidAlgException e ) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError( e );
        }
    }


    @Override
    public SemiJoin copy( AlgTraitSet traitSet, RexNode condition, AlgNode left, AlgNode right, JoinAlgType joinType, boolean semiJoinDone ) {
        assert joinType == JoinAlgType.INNER;
        final JoinInfo joinInfo = JoinInfo.of( left, right, condition );
        assert joinInfo.isEqui();
        try {
            return new EnumerableSemiJoin( getCluster(), traitSet, left, right, condition, joinInfo.leftKeys, joinInfo.rightKeys );
        } catch ( InvalidAlgException e ) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError( e );
        }
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double rowCount = mq.getTupleCount( this );

        // Right-hand input is the "build", and hopefully small, input.
        final double rightRowCount = right.estimateTupleCount( mq );
        final double leftRowCount = left.estimateTupleCount( mq );
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
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        BlockBuilder builder = new BlockBuilder();
        final Result leftResult = implementor.visitChild( this, 0, (EnumerableAlg) left, pref );
        Expression leftExpression = builder.append( "left" + System.nanoTime(), leftResult.block() );
        final Result rightResult = implementor.visitChild( this, 1, (EnumerableAlg) right, pref );
        Expression rightExpression = builder.append( "right" + System.nanoTime(), rightResult.block() );
        final PhysType physType = leftResult.physType();
        return implementor.result(
                physType,
                builder.append(
                                Expressions.call(
                                        BuiltInMethod.SEMI_JOIN.method,
                                        Expressions.list(
                                                leftExpression,
                                                rightExpression,
                                                leftResult.physType().generateAccessor( leftKeys ),
                                                rightResult.physType().generateAccessor( rightKeys ) ) ) )
                        .toBlock() );
    }

}

