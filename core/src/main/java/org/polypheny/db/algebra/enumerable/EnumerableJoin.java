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
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgNodes;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.EquiJoin;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.JoinInfo;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link org.polypheny.db.algebra.core.Join} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableJoin extends EquiJoin implements EnumerableAlg {

    /**
     * Creates an EnumerableJoin.
     * <p>
     * Use {@link #create} unless you know what you're doing.
     */
    protected EnumerableJoin( AlgCluster cluster, AlgTraitSet traits, AlgNode left, AlgNode right, RexNode condition, ImmutableList<Integer> leftKeys, ImmutableList<Integer> rightKeys, Set<CorrelationId> variablesSet, JoinAlgType joinType ) throws InvalidAlgException {
        super( cluster, traits, left, right, condition, leftKeys, rightKeys, variablesSet, joinType );
    }


    /**
     * Creates an EnumerableJoin.
     */
    public static EnumerableJoin create( AlgNode left, AlgNode right, RexNode condition, ImmutableList<Integer> leftKeys, ImmutableList<Integer> rightKeys, Set<CorrelationId> variablesSet, JoinAlgType joinType ) throws InvalidAlgException {
        final AlgCluster cluster = left.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet = cluster.traitSetOf( EnumerableConvention.INSTANCE )
                .replace( ModelTrait.RELATIONAL )
                .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.enumerableJoin( mq, left, right, joinType ) );
        return new EnumerableJoin( cluster, traitSet, left, right, condition, leftKeys, rightKeys, variablesSet, joinType );
    }


    @Override
    public EnumerableJoin copy( AlgTraitSet traitSet, RexNode condition, AlgNode left, AlgNode right, JoinAlgType joinType, boolean semiJoinDone ) {
        final JoinInfo joinInfo = JoinInfo.of( left, right, condition );
        assert joinInfo.isEqui();
        try {
            return new EnumerableJoin( getCluster(), traitSet, left, right, condition, joinInfo.leftKeys, joinInfo.rightKeys, variablesSet, joinType );
        } catch ( InvalidAlgException e ) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError( e );
        }
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double rowCount = mq.getTupleCount( this );

        // Joins can be flipped, and for many algorithms, both versions are viable and have the same cost.
        // To make the results stable between versions of the planner, make one of the versions slightly more expensive.
        if ( Objects.requireNonNull( joinType ) == JoinAlgType.RIGHT ) {
            rowCount = addEpsilon( rowCount );
        } else {
            if ( AlgNodes.COMPARATOR.compare( left, right ) > 0 ) {
                rowCount = addEpsilon( rowCount );
            }
        }

        // Cheaper if the smaller number of rows is coming from the LHS. Model this by adding L log L to the cost.
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
        return planner.getCostFactory().makeCost( rowCount, 0, 0 ).multiplyBy( 100 ); // de-courage usage of enumerable
    }


    private double addEpsilon( double d ) {
        assert d >= 0d;
        final double d0 = d;
        if ( d < 10 ) {
            // For small d, adding 1 would change the value significantly.
            d *= 1.001d;
            if ( d != d0 ) {
                return d;
            }
        }
        // For medium d, add 1. Keeps integral values integral.
        ++d;
        if ( d != d0 ) {
            return d;
        }
        // For large d, adding 1 might not change the value. Add .1%.
        // If d is NaN, this still will probably not change the value. That's OK.
        d *= 1.001d;
        return d;
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        BlockBuilder builder = new BlockBuilder();
        final Result leftResult = implementor.visitChild( this, 0, (EnumerableAlg) left, pref );
        Expression leftExpression = builder.append( "left" + System.nanoTime(), leftResult.block() );
        final Result rightResult = implementor.visitChild( this, 1, (EnumerableAlg) right, pref );
        // we need this false flag to avoid that the enumerables are reused which would lead to the same enumerable being accessed from both sides
        Expression rightExpression = builder.append( "right" + System.nanoTime(), rightResult.block(), false );
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), pref.preferArray() );
        final PhysType keyPhysType = leftResult.physType().project( leftKeys, JavaTupleFormat.LIST );
        return implementor.result(
                physType,
                builder.append(
                                Expressions.call(
                                        leftExpression,
                                        BuiltInMethod.JOIN.method,
                                        Expressions.list(
                                                        rightExpression,
                                                        leftResult.physType().generateAccessor( leftKeys ),
                                                        rightResult.physType().generateAccessor( rightKeys ),
                                                        EnumUtils.joinSelector( joinType, physType, ImmutableList.of( leftResult.physType(), rightResult.physType() ) ) )
                                                .append( Util.first( keyPhysType.comparer(), Expressions.constant( null ) ) )
                                                .append( Expressions.constant( joinType.generatesNullsOnLeft() ) )
                                                .append( Expressions.constant( joinType.generatesNullsOnRight() ) )
                                                .append( Expressions.constant( null ) ) ) )
                        .toBlock() );
    }

}

