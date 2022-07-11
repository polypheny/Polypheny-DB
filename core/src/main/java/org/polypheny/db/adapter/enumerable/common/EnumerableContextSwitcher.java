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
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.enumerable.EnumerableAlg;
import org.polypheny.db.adapter.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.ContextSwitcher;
import org.polypheny.db.plan.AlgTraitSet;

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
        final Result query = implementor.visitChild( this, 0, (EnumerableAlg) input, pref );

        Expression executor = builder.append( builder.newName( "executor" + System.nanoTime() ), query.block );

        builder.add( Expressions.return_( null, Expressions.new_( ContextSwitcherEnumerable.class, executor, DataContext.ROOT ) ) );

        return implementor.result( query.physType, builder.toBlock() );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableContextSwitcher( inputs.get( 0 ) );
    }


    public static class ContextSwitcherEnumerable<T> extends AbstractEnumerable<T> {

        private final Enumerable<T> enumerable;
        private final DataContext context;


        public ContextSwitcherEnumerable( Enumerable<T> enumerable, DataContext context ) {
            super();
            this.enumerable = enumerable;
            this.context = context;
        }


        @Override
        public Enumerator<T> enumerator() {
            context.switchContext();
            return enumerable.enumerator();
        }

    }

}
