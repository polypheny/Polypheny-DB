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

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.logical.graph.LogicalGraphMatch;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.tools.AlgBuilderFactory;

public class EnumerableGraphMatchRule extends ConverterRule {


    public EnumerableGraphMatchRule( AlgBuilderFactory builder ) {
        super( LogicalGraphMatch.class, r -> true, Convention.NONE, EnumerableConvention.INSTANCE, builder, "EnumerableGraphMatch" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        LogicalGraphMatch match = (LogicalGraphMatch) alg;
        AlgNode input = AlgOptRule.convert( match.getInput(), EnumerableConvention.INSTANCE );
        return new EnumerableGraphMatch( alg.getCluster(), alg.getTraitSet().replace( EnumerableConvention.INSTANCE ), input, match.getMatches(), match.getNames() );
    }

}
