/*
 * Copyright 2019-2020 The Polypheny Project
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


import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Intersect;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Implementation of {@link org.polypheny.db.rel.core.Intersect} in {@link org.polypheny.db.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 */
public class EnumerableIntersect extends Intersect implements EnumerableRel {

    public EnumerableIntersect( RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
        super( cluster, traitSet, inputs, all );
        assert !all;
    }


    @Override
    public EnumerableIntersect copy( RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
        return new EnumerableIntersect( getCluster(), traitSet, inputs, all );
    }


    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        Expression intersectExp = null;
        for ( Ord<RelNode> ord : Ord.zip( inputs ) ) {
            EnumerableRel input = (EnumerableRel) ord.e;
            final Result result = implementor.visitChild( this, ord.i, input, pref );
            Expression childExp = builder.append( "child" + ord.i, result.block );

            if ( intersectExp == null ) {
                intersectExp = childExp;
            } else {
                intersectExp =
                        Expressions.call(
                                intersectExp,
                                BuiltInMethod.INTERSECT.method,
                                Expressions.list( childExp ).appendIfNotNull( result.physType.comparer() ) );
            }

            // Once the first input has chosen its format, ask for the same for other inputs.
            pref = pref.of( result.format );
        }

        builder.add( intersectExp );
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getRowType(), pref.prefer( JavaRowFormat.CUSTOM ) );
        return implementor.result( physType, builder.toBlock() );
    }
}
