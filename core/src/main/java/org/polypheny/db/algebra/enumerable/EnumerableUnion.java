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
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Implementation of {@link org.polypheny.db.algebra.core.Union} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableUnion extends Union implements EnumerableAlg {

    public EnumerableUnion( AlgCluster cluster, AlgTraitSet traitSet, List<AlgNode> inputs, boolean all ) {
        super( cluster, traitSet, inputs, all );
    }


    @Override
    public EnumerableUnion copy( AlgTraitSet traitSet, List<AlgNode> inputs, boolean all ) {
        return new EnumerableUnion( getCluster(), traitSet, inputs, all );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        Expression unionExp = null;
        for ( Ord<AlgNode> ord : Ord.zip( inputs ) ) {
            EnumerableAlg input = (EnumerableAlg) ord.e;
            final Result result = implementor.visitChild( this, ord.i, input, pref );
            Expression childExp = builder.append( "child" + ord.i, result.block() );

            if ( unionExp == null ) {
                unionExp = childExp;
            } else {
                unionExp = all
                        ? Expressions.call( unionExp, BuiltInMethod.CONCAT.method, childExp )
                        : Expressions.call( unionExp, BuiltInMethod.UNION.method, Expressions.list( childExp ).appendIfNotNull( result.physType().comparer() ) );
            }
        }

        builder.add( unionExp );
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getTupleType(),
                        pref.prefer( JavaTupleFormat.CUSTOM ) );
        return implementor.result( physType, builder.toBlock() );
    }

}

