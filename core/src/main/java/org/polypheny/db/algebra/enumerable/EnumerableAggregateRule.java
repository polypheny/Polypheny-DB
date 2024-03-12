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
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Rule to convert a {@link LogicalRelAggregate} to an {@link EnumerableAggregate}.
 */
public class EnumerableAggregateRule extends ConverterRule {

    EnumerableAggregateRule() {
        super( LogicalRelAggregate.class, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableAggregateRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final LogicalRelAggregate agg = (LogicalRelAggregate) alg;
        final AlgTraitSet traitSet = agg.getTraitSet().replace( EnumerableConvention.INSTANCE );
        try {
            return new EnumerableAggregate( alg.getCluster(), traitSet, convert( agg.getInput(), EnumerableConvention.INSTANCE ), agg.indicator, agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList() );
        } catch ( InvalidAlgException e ) {
            EnumerableRules.LOGGER.debug( e.toString() );
            return null;
        }
    }

}

