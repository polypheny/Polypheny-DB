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

package org.polypheny.db.adapter.neo4j;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.tools.AlgBuilderFactory;

/**
 * {@link ConverterRule}, which registers the Neo4j converter operator to push the algebra into the Neo4j adapter.
 */
public class NeoToEnumerableConverterRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new NeoToEnumerableConverterRule( AlgFactories.LOGICAL_BUILDER );


    public NeoToEnumerableConverterRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                AlgNode.class,
                r -> true,
                NeoConvention.INSTANCE,
                EnumerableConvention.INSTANCE,
                algBuilderFactory,
                "NeoToEnumerableConverterRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        AlgTraitSet newTraitSet = alg.getTraitSet().replace( getOutTrait() );
        return new NeoToEnumerableConverter( alg.getCluster(), newTraitSet, alg );
    }

}
