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
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.SemiJoinType;
import org.polypheny.db.algebra.core.Correlate;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Implementation of {@link Correlate} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableCorrelate extends Correlate implements EnumerableAlg {

    public EnumerableCorrelate(
            AlgCluster cluster,
            AlgTraitSet traits,
            AlgNode left,
            AlgNode right,
            CorrelationId correlationId,
            ImmutableBitSet requiredColumns,
            SemiJoinType joinType ) {
        super( cluster, traits, left, right, correlationId, requiredColumns, joinType );
    }


    /**
     * Creates an EnumerableCorrelate.
     */
    public static EnumerableCorrelate create(
            AlgNode left,
            AlgNode right,
            CorrelationId correlationId,
            ImmutableBitSet requiredColumns,
            SemiJoinType joinType ) {
        final AlgCluster cluster = left.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet = cluster.traitSetOf( EnumerableConvention.INSTANCE ).replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.enumerableCorrelate( mq, left, right, joinType ) );
        return new EnumerableCorrelate( cluster, traitSet, left, right, correlationId, requiredColumns, joinType );
    }


    @Override
    public EnumerableCorrelate copy(
            AlgTraitSet traitSet,
            AlgNode left,
            AlgNode right,
            CorrelationId correlationId,
            ImmutableBitSet requiredColumns,
            SemiJoinType joinType ) {
        return new EnumerableCorrelate( getCluster(), traitSet, left, right, correlationId, requiredColumns, joinType );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final Result leftResult = implementor.visitChild( this, 0, (EnumerableAlg) left, pref );
        Expression leftExpression = builder.append( "left", leftResult.block() );

        final BlockBuilder corrBlock = new BlockBuilder();
        Type corrVarType = leftResult.physType().getJavaTupleType();
        ParameterExpression corrRef; // correlate to be used in inner loop
        ParameterExpression corrArg; // argument to correlate lambda (must be boxed)
        if ( !Primitive.is( corrVarType ) ) {
            corrArg = Expressions.parameter( Modifier.FINAL, corrVarType, getCorrelVariable() );
            corrRef = corrArg;
        } else {
            corrArg = Expressions.parameter( Modifier.FINAL, Primitive.box( corrVarType ), "$box" + getCorrelVariable() );
            corrRef = (ParameterExpression) corrBlock.append( getCorrelVariable(), Expressions.unbox( corrArg ) );
        }

        implementor.registerCorrelVariable( getCorrelVariable(), corrRef, corrBlock, leftResult.physType() );

        final Result rightResult = implementor.visitChild( this, 1, (EnumerableAlg) right, pref );

        implementor.clearCorrelVariable( getCorrelVariable() );

        corrBlock.add( rightResult.block() );

        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getTupleType(),
                        pref.prefer( JavaTupleFormat.CUSTOM ) );

        Expression selector =
                EnumUtils.joinSelector(
                        joinType,
                        physType,
                        ImmutableList.of( leftResult.physType(), rightResult.physType() ) );

        builder.append(
                Expressions.call(
                        leftExpression,
                        BuiltInMethod.CORRELATE_JOIN.method,
                        Expressions.constant( joinType.toLinq4j() ),
                        Expressions.lambda( corrBlock.toBlock(), corrArg ),
                        selector ) );

        return implementor.result( physType, builder.toBlock() );
    }

}

