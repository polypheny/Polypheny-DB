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

package org.polypheny.db.algebra.enumerable;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.logical.relational.LogicalRelIdentifierInjection;
import org.polypheny.db.plan.AlgTraitSet;

/**
 * Planner rule that converts a {@link LogicalRelIdentifierInjection} relational expression {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableIdentifierInjectionRule extends ConverterRule {

    EnumerableIdentifierInjectionRule() {
        super( LogicalRelIdentifierInjection.class, EnumerableConvention.NONE, EnumerableConvention.INSTANCE, "EnumerableIdentifierInjectionRule");
    }

    @Override
    public AlgNode convert( AlgNode alg ) {
        LogicalRelIdentifierInjection injection = (LogicalRelIdentifierInjection) alg;
        final AlgNode provider = convert( injection.getLeft(), injection.getLeft().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        final AlgNode collector = convert( injection.getRight(), injection.getRight().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        final AlgTraitSet traits = alg.getTraitSet().replace( EnumerableConvention.INSTANCE );
        return EnumerableIdentifierInjection.create( traits, injection.getEntity(), provider, collector );
    }
}
