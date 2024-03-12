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
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


/**
 * Implementation of {@link org.polypheny.db.algebra.core.Sort} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableSort extends Sort implements EnumerableAlg {

    /**
     * Creates an EnumerableSort.
     * <p>
     * Use {@link #create} unless you know what you're doing.
     */
    public EnumerableSort( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, AlgCollation collation, RexNode offset, RexNode fetch ) {
        super( cluster, traitSet, input, collation, null, offset, fetch );
        assert getConvention() instanceof EnumerableConvention;
        assert getConvention() == input.getConvention();
    }


    /**
     * Creates an EnumerableSort.
     */
    public static EnumerableSort create( AlgNode child, AlgCollation collation, RexNode offset, RexNode fetch ) {
        final AlgCluster cluster = child.getCluster();
        final AlgTraitSet traitSet = child.getTraitSet().replace( collation );
        return new EnumerableSort( cluster, traitSet, child, collation, offset, fetch );
    }


    @Override
    public EnumerableSort copy( AlgTraitSet traitSet, AlgNode newInput, AlgCollation newCollation, ImmutableList<RexNode> nodes, RexNode offset, RexNode fetch ) {
        return new EnumerableSort( getCluster(), traitSet, newInput, newCollation, offset, fetch );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableAlg child = (EnumerableAlg) getInput();
        final Result result = implementor.visitChild( this, 0, child, pref );
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), result.format() );
        Expression childExp = builder.append( "child", result.block() );

        PhysType inputPhysType = result.physType();
        final Pair<Expression, Expression> pair = inputPhysType.generateCollationKey( collation.getFieldCollations() );

        builder.add(
                Expressions.return_(
                        null,
                        Expressions.call(
                                childExp,
                                BuiltInMethod.ORDER_BY.method,
                                Expressions.list( builder.append( "keySelector", pair.left ) )
                                        .appendIfNotNull( builder.appendIfNotNull( "comparator", pair.right ) ) ) ) );
        return implementor.result( physType, builder.toBlock() );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 10 );
    }

}

