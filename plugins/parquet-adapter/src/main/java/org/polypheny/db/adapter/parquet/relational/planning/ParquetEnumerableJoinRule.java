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

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.EnumerableJoin;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilderFactory;

import static org.polypheny.db.adapter.parquet.relational.planning.ParquetRelJoin.supportedDirection;

/**
 * Rewrites a generic enumerable join over two Parquet scans into a Parquet
 * adapter join when the join is a supported normalized parent/child join.
 */
public class ParquetEnumerableJoinRule extends AlgOptRule {

    public static final ParquetEnumerableJoinRule INSTANCE = new ParquetEnumerableJoinRule( AlgFactories.LOGICAL_BUILDER );


    public ParquetEnumerableJoinRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( EnumerableJoin.class, operand( AlgNode.class, any() ), operand( AlgNode.class, any() ) ),
                algBuilderFactory,
                ParquetEnumerableJoinRule.class.getSimpleName() );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        EnumerableJoin join = call.alg( 0 );
        ParquetRelScan left = ParquetRelScanRuleSupport.findProjectedRelScan( call.alg( 1 ) );
        ParquetRelScan right = ParquetRelScanRuleSupport.findProjectedRelScan( call.alg( 2 ) );
        if ( left == null || right == null ) {
            return;
        }
        JoinDirection direction = supportedDirection( join, left, right );
        if ( direction == null ) {
            return;
        }
        call.transformTo( ParquetRelJoin.create( join.getLeft(), join.getRight(), left, right, join.getCondition(), join.getVariablesSet(), join.getJoinType(), direction.leftIsParent(), JoinInputLimit.NONE ) );
    }
}
