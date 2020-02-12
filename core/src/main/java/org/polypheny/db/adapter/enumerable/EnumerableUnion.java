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

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Union;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;


/**
 * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Union} in {@link ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 */
public class EnumerableUnion extends Union implements EnumerableRel {

    public EnumerableUnion( RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
        super( cluster, traitSet, inputs, all );
    }


    @Override
    public EnumerableUnion copy( RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
        return new EnumerableUnion( getCluster(), traitSet, inputs, all );
    }


    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        Expression unionExp = null;
        for ( Ord<RelNode> ord : Ord.zip( inputs ) ) {
            EnumerableRel input = (EnumerableRel) ord.e;
            final Result result = implementor.visitChild( this, ord.i, input, pref );
            Expression childExp = builder.append( "child" + ord.i, result.block );

            if ( unionExp == null ) {
                unionExp = childExp;
            } else {
                unionExp = all
                        ? Expressions.call( unionExp, BuiltInMethod.CONCAT.method, childExp )
                        : Expressions.call( unionExp, BuiltInMethod.UNION.method, Expressions.list( childExp ).appendIfNotNull( result.physType.comparer() ) );
            }
        }

        builder.add( unionExp );
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.prefer( JavaRowFormat.CUSTOM ) );
        return implementor.result( physType, builder.toBlock() );
    }
}

