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
import org.polypheny.db.algebra.core.common.Identifier;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.BuiltInMethod;

public class EnumerableIdentifier extends Identifier implements EnumerableAlg {

    protected EnumerableIdentifier( AlgCluster cluster, AlgTraitSet traits, Entity entity, AlgNode input ) {
        super( cluster, traits, entity, input );
        assert getConvention() instanceof EnumerableConvention;
        assert getConvention() == input.getConvention();
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableAlg input = (EnumerableAlg) getInput();
        final Result result = implementor.visitChild( this, 0, input, Prefer.ANY );
        final PhysType physType = result.physType();

        Expression input_ = builder.append( "input", result.block() );
        Expression identification_ = builder.append( "identification", Expressions.call( input_, BuiltInMethod.ADD_IDENTIFIERS.method ) );

        builder.add( Expressions.return_( null, identification_ ) );
        return implementor.result( physType, builder.toBlock() );

    }

}
