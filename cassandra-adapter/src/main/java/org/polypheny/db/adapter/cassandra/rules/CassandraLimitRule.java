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

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.rules;


import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraLimit;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableLimit;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Rule to convert a {@link EnumerableLimit} to a {@link CassandraLimit}.
 */
public class CassandraLimitRule extends CassandraConverterRule {

    CassandraLimitRule( CassandraConvention out, RelBuilderFactory relBuilderFactory ) {
        super( EnumerableLimit.class, r -> true, EnumerableConvention.INSTANCE, out, relBuilderFactory, "CassandraLimitRule" );
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
