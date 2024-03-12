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


import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.interpreter.Interpreter;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Relational expression that executes its children using an interpreter.
 * <p>
 * Although quite a few kinds of {@link AlgNode} can be interpreted, this is only created by default for {@link org.polypheny.db.schema.types.FilterableEntity} and
 * {@link org.polypheny.db.schema.types.ProjectableFilterableEntity}.
 */
public class EnumerableInterpreter extends SingleAlg implements EnumerableAlg {

    private final double factor;


    /**
     * Creates an EnumerableInterpreter.
     * <p>
     * Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster
     * @param traitSet Traits
     * @param input Input relation
     * @param factor Cost multiply factor
     */
    public EnumerableInterpreter( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, double factor ) {
        super( cluster, traitSet, input );
        assert getConvention() instanceof EnumerableConvention;
        this.factor = factor;
    }


    /**
     * Creates an EnumerableInterpreter.
     *
     * @param input Input relation
     * @param factor Cost multiply factor
     */
    public static EnumerableInterpreter create( AlgNode input, double factor ) {
        final AlgTraitSet traitSet = input.getTraitSet().replace( EnumerableConvention.INSTANCE );
        return new EnumerableInterpreter( input.getCluster(), traitSet, input, factor );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( factor );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableInterpreter( getCluster(), traitSet, sole( inputs ), factor );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$"
                + input.algCompareString() + "$"
                + factor + "&";
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType = PhysTypeImpl.of( typeFactory, getTupleType(), JavaTupleFormat.ARRAY );
        final Expression interpreter_ = builder.append( builder.newName( "interpreter" + System.nanoTime() ), Expressions.new_( Interpreter.class, implementor.getRootExpression(), implementor.stash( getInput(), AlgNode.class ) ) );
        final Expression sliced_ =
                getTupleType().getFieldCount() == 1
                        ? Expressions.call( BuiltInMethod.SLICE0.method, interpreter_ )
                        : interpreter_;
        builder.add( sliced_ );
        return implementor.result( physType, builder.toBlock() );
    }

}

