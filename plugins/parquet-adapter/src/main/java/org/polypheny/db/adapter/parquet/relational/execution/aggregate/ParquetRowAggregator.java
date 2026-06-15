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

package org.polypheny.db.adapter.parquet.relational.execution.aggregate;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateCallDescriptor;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateGroupState;
import org.polypheny.db.adapter.parquet.shared.aggregate.GroupKey;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Generic row-by-row aggregate fallback shared by direct and Calc projections.
 */
public final class ParquetRowAggregator {

    private ParquetRowAggregator() {
    }


    /**
     * Aggregates data from the provided enumerator.
     *
     * @param rows an enumerator.
     * @param aggregateCalls an array of aggregation calls to be applied.
     * @param projector a row projector that customizes values reading.
     * @return a grouped aggregations.
     */
    public static Map<GroupKey, AggregateGroupState> aggregate( Enumerator<PolyValue[]> rows, AggregateCallDescriptor[] aggregateCalls, AggregateRowProjector projector ) {
        Map<GroupKey, AggregateGroupState> aggregates = new LinkedHashMap<>();
        try ( rows ) {
            while ( rows.moveNext() ) {
                add( aggregates, aggregateCalls, projector, rows.current() );
            }
        }
        return aggregates;
    }


    /**
     * Adds a row values to aggregated state via projector.
     *
     * @param aggregates a grouped aggregations that keeps the state.
     * @param aggregateCalls an array of aggregation calls to be applied.
     * @param projector a row projector that customizes the row access.
     * @param row a row.
     */
    private static void add( Map<GroupKey, AggregateGroupState> aggregates, AggregateCallDescriptor[] aggregateCalls, AggregateRowProjector projector, PolyValue[] row ) {
        if ( !projector.accepts( row ) ) {
            return;
        }
        GroupKey key = projector.groupKey( row );
        AggregateGroupState values = aggregates.computeIfAbsent( key, ignored -> new AggregateGroupState( aggregateCalls ) );
        projector.add( values, row );
    }

}
