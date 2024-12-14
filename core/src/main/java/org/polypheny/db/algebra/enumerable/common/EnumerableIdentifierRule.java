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

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.common.Identifier;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.relational.LogicalRelIdentifier;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;

public class EnumerableIdentifierRule extends ConverterRule {

    public static final EnumerableIdentifierRule REL_INSTANCE = new EnumerableIdentifierRule( LogicalRelIdentifier.class );


    private EnumerableIdentifierRule( Class<? extends Identifier> identifier ) {
        super( identifier, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableIdentifierRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final Identifier identifier = (Identifier) alg;
        final AlgTraitSet traits = identifier.getTraitSet().replace( EnumerableConvention.INSTANCE );
        final AlgNode input = identifier.getInput();
        return new EnumerableIdentifier( identifier.getCluster(), traits, identifier.getEntity(), input );
    }

}
