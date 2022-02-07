/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adapter.enumerable;

import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Transformer;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;

public class EnumerableTransformer extends Transformer implements EnumerableAlg {

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     */
    protected EnumerableTransformer( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, List<PolyType> unsupportedTypes, PolyType substituteType ) {
        super( cluster, traits, input, unsupportedTypes, substituteType );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        Result orig = null;//implementor.visitChild( this, 0, (EnumerableAlg) getConvertedScan(), pref );

        Expression childExp = builder.append( builder.newName( "child_" + System.nanoTime() ), orig.block );

        Expression call = Expressions.call( BuiltInMethod.DESERIALIZE.method, childExp );

        builder.add( call );
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.prefer( JavaRowFormat.CUSTOM ) );
        return implementor.result( physType, builder.toBlock() );
    }

}
