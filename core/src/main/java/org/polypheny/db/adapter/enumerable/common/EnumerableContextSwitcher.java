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

package org.polypheny.db.adapter.enumerable.common;

import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.enumerable.EnumerableAlg;
import org.polypheny.db.adapter.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.ContextSwitcher;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.BuiltInMethod;

public class EnumerableContextSwitcher extends ContextSwitcher implements EnumerableAlg {

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param input Input relational expression
     */
    public EnumerableContextSwitcher( AlgNode input ) {
        super( input );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final BlockBuilder lambdaBuilder = new BlockBuilder();
        final Result query = implementor.visitChild( this, 0, (EnumerableAlg) input, pref );

        lambdaBuilder.add( Expressions.statement( Expressions.call( DataContext.ROOT, BuiltInMethod.SWITCH_CONTEXT.method ) ) );
        Expression executor = lambdaBuilder.append( lambdaBuilder.newName( "executor" + System.nanoTime() ), query.block );
        lambdaBuilder.add( Expressions.return_( null, builder.append( "ex" + System.nanoTime(), executor ) ) );

        builder.add( Expressions.return_( null, Expressions.convert_( Expressions.call( Expressions.lambda( lambdaBuilder.toBlock() ), BuiltInMethod.FUNCTION0_APPLY.method ), Enumerable.class ) ) );

        return implementor.result( query.physType, builder.toBlock() );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableContextSwitcher( inputs.get( 0 ) );
    }

}
