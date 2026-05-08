/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.relational.planning;

import static org.polypheny.db.adapter.parquet.relational.planning.ParquetRelJoin.supportedDirection;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.tools.AlgBuilderFactory;

/**
 * Rewrites supported parent/child joins between normalized Parquet tables into
 * a Parquet adapter join.
 */
public class ParquetRelJoinRule extends ConverterRule {

    public static final ParquetRelJoinRule INSTANCE = new ParquetRelJoinRule( AlgFactories.LOGICAL_BUILDER );


    public ParquetRelJoinRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                LogicalRelJoin.class,
                join -> true,
                Convention.NONE,
                EnumerableConvention.INSTANCE,
                algBuilderFactory,
                ParquetRelJoinRule.class.getSimpleName() );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        LogicalRelJoin join = (LogicalRelJoin) alg;
        ParquetRelScan left = findRelScan( convert( join.getLeft(), join.getLeft().getTraitSet().replace( EnumerableConvention.INSTANCE ) ) );
        ParquetRelScan right = findRelScan( convert( join.getRight(), join.getRight().getTraitSet().replace( EnumerableConvention.INSTANCE ) ) );
        if ( left == null || right == null ) {
            return null;
        }

        JoinDirection direction = supportedDirection( join, left, right );
        if ( direction == null ) {
            return null;
        }

        return ParquetRelJoin.create( left, right, join.getCondition(), join.getVariablesSet(), join.getJoinType(), direction.leftIsParent() );
    }


    private ParquetRelScan findRelScan( AlgNode alg ) {
        if ( alg instanceof ParquetRelScan relScan ) {
            return relScan;
        }
        if ( alg instanceof AlgSubset subset && subset.getBest() instanceof ParquetRelScan relScan ) {
            return relScan;
        }
        return null;
    }

}
