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

package org.polypheny.db.adapter.parquet.relational.optimization.aggregate;

import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.enumerable.EnumerableAggregate;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Used for the partial aggregate over UNION ALL optimization.
 * The class describes what each union child should compute, and how the top aggregate should combine those partial results.
 * <pre>
 * For example:
 * - COUNT: child computes COUNT, final combines with SUM0
 * - SUM, MIN, MAX: child computes the same aggregate, final combines with the same aggregate
 * - metadata-supported aggregates: COUNT, MIN, MAX, with COUNT combined as SUM0
 * </pre>
 *
 * @param partialAggregates
 */
public record AggregateDecomposition( List<PartialAggregate> partialAggregates ) {

    public List<AggregateCall> partialCalls() {
        return partialAggregates.stream()
                .map( PartialAggregate::partialCall )
                .toList();
    }


    /**
     * Creates a list of aggregate calls from previously collected partial aggregates.
     *
     * @param aggregate an original aggregate.
     * @param input an input node.
     * @return a list of newly created aggregate calls.
     */
    public List<AggregateCall> buildFinalCalls( EnumerableAggregate aggregate, AlgNode input ) {
        int groupCount = aggregate.getGroupSet().cardinality();
        return IntStream.range( 0, partialAggregates.size() )
                .mapToObj( index -> {
                    PartialAggregate partialAggregate = partialAggregates.get( index );
                    AggregateCall partialCall = partialAggregate.partialCall();
                    return AggregateCall.create(
                            partialAggregate.finalFunction(),
                            false,
                            false,
                            List.of( groupCount + index ),
                            -1,
                            AlgCollations.EMPTY,
                            groupCount,
                            input,
                            null,
                            partialCall.getName() );
                } )
                .toList();
    }

}
