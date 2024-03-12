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
 */

package org.polypheny.db.algebra.enumerable.common;


import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Collect;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.JavaTupleFormat;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Implementation of {@link Collect} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableCollect extends Collect implements EnumerableAlg {

    public EnumerableCollect( AlgCluster cluster, AlgTraitSet traitSet, AlgNode child, String fieldName ) {
        super( cluster, traitSet, child, fieldName );
        assert getConvention() instanceof EnumerableConvention;
        assert getConvention() == child.getConvention();
    }


    @Override
    public EnumerableCollect copy( AlgTraitSet traitSet, AlgNode newInput ) {
        return new EnumerableCollect( getCluster(), traitSet, newInput, fieldName );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableAlg child = (EnumerableAlg) getInput();
        final Result result = implementor.visitChild( this, 0, child, Prefer.ARRAY );
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), JavaTupleFormat.LIST );

        Expression child_ = builder.append( "child", result.block() );
        Expression list_ = builder.append( "list", Expressions.call( child_, BuiltInMethod.ENUMERABLE_TO_LIST.method ) );

        builder.add( Expressions.return_( null, Expressions.call( BuiltInMethod.SINGLETON_ENUMERABLE.method, list_ ) ) );
        return implementor.result( physType, builder.toBlock() );
    }

}

