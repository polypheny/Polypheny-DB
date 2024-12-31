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
import org.polypheny.db.algebra.core.common.IdentifierCollector;
import org.polypheny.db.algebra.logical.document.LogicalDocIdCollector;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgIdCollector;
import org.polypheny.db.algebra.logical.relational.LogicalRelIdCollector;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;

public class EnumerableIdCollectorRule extends ConverterRule {

    public static final EnumerableIdCollectorRule REL_INSTANCE = new EnumerableIdCollectorRule( LogicalRelIdCollector.class );
    public static final EnumerableIdCollectorRule DOC_INSTANCE = new EnumerableIdCollectorRule( LogicalDocIdCollector.class );
    public static final EnumerableIdCollectorRule GRAPH_INSTANCE = new EnumerableIdCollectorRule( LogicalLpgIdCollector.class );


    private EnumerableIdCollectorRule( Class<? extends IdentifierCollector> collector ) {
        super( collector, Convention.NONE, EnumerableConvention.INSTANCE, "Enumerable" + collector.getSimpleName() + "Rule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final IdentifierCollector identifier = (IdentifierCollector) alg;
        final AlgTraitSet traits = identifier.getTraitSet().replace( EnumerableConvention.INSTANCE );
        final AlgNode input = convert( identifier.getInput(), identifier.getInput().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        return new EnumerableIdCollector( identifier.getCluster(), traits, identifier.getTransaction(), identifier.getEntity(), input );
    }

}
