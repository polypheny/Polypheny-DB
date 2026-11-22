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

package org.polypheny.db.algebra.logical.lpg;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.SemiJoinType;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Correlate;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.EnumerableCorrelate;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.util.ImmutableBitSet;


/** Evaluates a returning Cypher CALL for each input row. */
public final class LogicalLpgCall extends Correlate implements LpgAlg {

    public static final ConverterRule TO_ENUMERABLE = new ConverterRule(
            LogicalLpgCall.class, node -> true, Convention.NONE, EnumerableConvention.INSTANCE,
            AlgFactories.LOGICAL_BUILDER, "LpgCallToEnumerable" ) {
        @Override
        public AlgNode convert( AlgNode node ) {
            LogicalLpgCall call = (LogicalLpgCall) node;
            return EnumerableCorrelate.create(
                    convert( call.getLeft(), call.getLeft().getTraitSet().replace( EnumerableConvention.INSTANCE ) ),
                    convert( call.getRight(), call.getRight().getTraitSet().replace( EnumerableConvention.INSTANCE ) ),
                    call.getCorrelationId(), call.getRequiredColumns(), call.getJoinType() );
        }
    };


    public LogicalLpgCall( AlgNode outer, AlgNode body, CorrelationId correlation, ImmutableBitSet requiredColumns ) {
        this( outer.getTraitSet(), outer, body, correlation, requiredColumns, SemiJoinType.INNER );
    }


    private LogicalLpgCall( AlgTraitSet traits, AlgNode outer, AlgNode body, CorrelationId correlation,
            ImmutableBitSet requiredColumns, SemiJoinType joinType ) {
        super( outer.getCluster(), traits, outer, body, correlation, requiredColumns, joinType );
    }


    @Override
    public LogicalLpgCall copy( AlgTraitSet traits, AlgNode left, AlgNode right, CorrelationId correlation,
            ImmutableBitSet requiredColumns, SemiJoinType joinType ) {
        return new LogicalLpgCall( traits, left, right, correlation, requiredColumns, joinType );
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.JOIN;
    }
}
