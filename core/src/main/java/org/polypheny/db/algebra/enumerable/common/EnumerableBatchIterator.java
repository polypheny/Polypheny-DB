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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.BatchIterator;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.BuiltInMethod;


public class EnumerableBatchIterator extends BatchIterator implements EnumerableAlg {

    /**
     * Creates a <code>EnumerableBatchIterator</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits The trait set
     * @param input Input relational expression
     */
    protected EnumerableBatchIterator( AlgCluster cluster, AlgTraitSet traits, AlgNode input ) {
        super( cluster, traits, input );
    }


    public static EnumerableBatchIterator create( AlgNode input ) {
        return new EnumerableBatchIterator( input.getCluster(), input.getTraitSet(), input );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableBatchIterator( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ) );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        Result result = implementor.visitChild( this, 0, (EnumerableAlg) getInput(), pref );

        final ParameterExpression _baz = Expressions.parameter( Enumerable.class, "_baz" );

        builder.add( Expressions.declare( 0, _baz, Expressions.new_(
                BuiltInMethod.BATCH_ITERATOR_CTOR.constructor,
                EnumUtils.NO_EXPRS,
                ImmutableList.<MemberDeclaration>of(
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.BATCH_ITERATOR_GET_ENUM.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( result.block() ) ) ) ) ) );

        builder.add( Expressions.return_( null, builder.append( "test", Expressions.call( BuiltInMethod.BATCH.method, Expressions.constant( DataContext.ROOT ), _baz ) ) ) );
        return implementor.result( result.physType(), builder.toBlock() );
    }

}
