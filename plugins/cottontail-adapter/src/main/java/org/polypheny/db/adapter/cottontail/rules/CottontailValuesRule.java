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

package org.polypheny.db.adapter.cottontail.rules;

import org.polypheny.db.adapter.cottontail.CottontailConvention;
import org.polypheny.db.adapter.cottontail.algebra.CottontailValues;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.tools.AlgBuilderFactory;


public class CottontailValuesRule extends CottontailConverterRule {

    CottontailValuesRule( AlgBuilderFactory algBuilderFactory ) {
        super( Values.class, r -> true, Convention.NONE, CottontailConvention.INSTANCE, algBuilderFactory, "CottontailValuesRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        Values values = (Values) alg;

        return new CottontailValues(
                values.getCluster(),
                values.getTupleType(),
                values.getTuples(),
                values.getTraitSet().replace( out ) );
    }

}
