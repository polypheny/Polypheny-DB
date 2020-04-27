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
import org.polypheny.db.adapter.cassandra.CassandraLimit;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableLimit;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Rule to convert a {@link EnumerableLimit} to a {@link CassandraLimit}.
 */
public class CassandraLimitRule extends CassandraConverterRule {

    CassandraLimitRule( CassandraConvention out, RelBuilderFactory relBuilderFactory ) {
        super( EnumerableLimit.class, r -> true, EnumerableConvention.INSTANCE, out, relBuilderFactory, "CassandraLimitRule:" + out.getName() );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        final EnumerableLimit limit = (EnumerableLimit) rel;
        final RelTraitSet traitSet = limit.getTraitSet().replace( out );
        return new CassandraLimit(
                limit.getCluster(),
                traitSet,
                convert( limit.getInput(), limit.getInput().getTraitSet().replace( out ) ),
                limit.offset,
                limit.fetch );
    }
}
