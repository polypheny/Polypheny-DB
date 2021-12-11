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

package org.polypheny.db.adapter.enumerable;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.EquiJoin;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.JoinInfo;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;


/**
 * Implementation of {@link Join} in {@link EnumerableConvention enumerable calling convention} using a merge algorithm.
 */
public class EnumerableMergeJoin extends EquiJoin implements EnumerableAlg {

    EnumerableMergeJoin( AlgOptCluster cluster, AlgTraitSet traits, AlgNode left, AlgNode right, RexNode condition, ImmutableIntList leftKeys, ImmutableIntList rightKeys, Set<CorrelationId> variablesSet, JoinAlgType joinType ) throws InvalidAlgException {
        super( cluster, traits, left, right, condition, leftKeys, rightKeys, variablesSet, joinType );
        final List<AlgCollation> collations = traits.getTraits( AlgCollationTraitDef.INSTANCE );
        assert collations == null || AlgCollations.contains( collations, leftKeys );
    }


    public static EnumerableMergeJoin create( AlgNode left, AlgNode right, RexLiteral condition, ImmutableIntList leftKeys, ImmutableIntList rightKeys, JoinAlgType joinType ) throws InvalidAlgException {
        final AlgOptCluster cluster = right.getCluster();
        AlgTraitSet traitSet = cluster.traitSet();
        if ( traitSet.isEnabled( AlgCollationTraitDef.INSTANCE ) ) {
            final AlgMetadataQuery mq = cluster.getMetadataQuery();
            final List<AlgCollation> collations = AlgMdCollation.mergeJoin( mq, left, right, leftKeys, rightKeys );
            traitSet = traitSet.replace( collations );
        }
        return new EnumerableMergeJoin( cluster, traitSet, left, right, condition, leftKeys, rightKeys, ImmutableSet.of(), joinType );
    }


    @Override
    public EnumerableMergeJoin copy( AlgTraitSet traitSet, RexNode condition, AlgNode left, AlgNode right, JoinAlgType joinType, boolean semiJoinDone ) {
        final JoinInfo joinInfo = JoinInfo.of( left, right, condition );
        assert joinInfo.isEqui();
        try {
            return new EnumerableMergeJoin( getCluster(), traitSet, left, right, condition, joinInfo.leftKeys, joinInfo.rightKeys, variablesSet, joinType );
        } catch ( InvalidAlgException e ) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError( e );
        }
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        // We assume that the inputs are sorted. The price of sorting them has already been paid. The cost of the join is therefore proportional to the input and output size.
        final double rightRowCount = right.estimateRowCount( mq );
        final double leftRowCount = left.estimateRowCount( mq );
        final double rowCount = mq.getRowCount( this );
        final double d = leftRowCount + rightRowCount + rowCount;
        return planner.getCostFactory().makeCost( d, 0, 0 );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        BlockBuilder builder = new BlockBuilder();
        final Result leftResult = implementor.visitChild( this, 0, (EnumerableAlg) left, pref );
        final Expression leftExpression = builder.append( "left" + System.nanoTime(), leftResult.block );
        final ParameterExpression left_ = Expressions.parameter( leftResult.physType.getJavaRowType(), "left" );
        final Result rightResult = implementor.visitChild( this, 1, (EnumerableAlg) right, pref );
        final Expression rightExpression = builder.append( "right" + System.nanoTime(), rightResult.block );
        final ParameterExpression right_ = Expressions.parameter( rightResult.physType.getJavaRowType(), "right" );
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();
        final PhysType physType = PhysTypeImpl.of( typeFactory, getRowType(), pref.preferArray() );
        final List<Expression> leftExpressions = new ArrayList<>();
        final List<Expression> rightExpressions = new ArrayList<>();
        for ( Pair<Integer, Integer> pair : Pair.zip( leftKeys, rightKeys ) ) {
            final AlgDataType keyType = typeFactory.leastRestrictive( ImmutableList.of( left.getRowType().getFieldList().get( pair.left ).getType(), right.getRowType().getFieldList().get( pair.right ).getType() ) );
            final Type keyClass = typeFactory.getJavaClass( keyType );
            leftExpressions.add( Types.castIfNecessary( keyClass, leftResult.physType.fieldReference( left_, pair.left ) ) );
            rightExpressions.add( Types.castIfNecessary( keyClass, rightResult.physType.fieldReference( right_, pair.right ) ) );
        }
        final PhysType leftKeyPhysType = leftResult.physType.project( leftKeys, JavaRowFormat.LIST );
        final PhysType rightKeyPhysType = rightResult.physType.project( rightKeys, JavaRowFormat.LIST );
        return implementor.result(
                physType,
                builder.append(
                        Expressions.call(
                                BuiltInMethod.MERGE_JOIN.method,
                                Expressions.list(
                                        leftExpression,
                                        rightExpression,
                                        Expressions.lambda( leftKeyPhysType.record( leftExpressions ), left_ ),
                                        Expressions.lambda( rightKeyPhysType.record( rightExpressions ), right_ ),
                                        EnumUtils.joinSelector( joinType, physType, ImmutableList.of( leftResult.physType, rightResult.physType ) ),
                                        Expressions.constant( joinType.generatesNullsOnLeft() ),
                                        Expressions.constant( joinType.generatesNullsOnRight() ) ) ) ).toBlock() );
    }

}

