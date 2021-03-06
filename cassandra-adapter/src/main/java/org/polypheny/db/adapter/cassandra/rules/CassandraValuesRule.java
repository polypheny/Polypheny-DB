/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.cassandra.rules;


import org.polypheny.db.adapter.cassandra.CassandraConvention;
import org.polypheny.db.adapter.cassandra.CassandraValues;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.tools.RelBuilderFactory;


public class CassandraValuesRule extends CassandraConverterRule {

    CassandraValuesRule( CassandraConvention out, RelBuilderFactory relBuilderFactory ) {
        super( Values.class, r -> true, Convention.NONE, out, relBuilderFactory, "CassandraValuesRule:" + out.getName() );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        Values values = (Values) rel;
        return new CassandraValues(
                values.getCluster(),
                values.getRowType(),
                values.getTuples(),
                values.getTraitSet().replace( out ) );
    }
}
