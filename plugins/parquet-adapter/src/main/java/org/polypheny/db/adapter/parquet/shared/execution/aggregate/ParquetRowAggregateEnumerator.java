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

package org.polypheny.db.adapter.parquet.shared.execution.aggregate;

import java.util.Map;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.polypheny.db.adapter.parquet.relational.execution.aggregate.DirectAggregateRowProjector;
import org.polypheny.db.adapter.parquet.relational.execution.aggregate.ParquetRowAggregator;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateCallDescriptor;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateGroupState;
import org.polypheny.db.adapter.parquet.shared.aggregate.GroupKey;
import org.polypheny.db.adapter.parquet.shared.execution.AbstractAggregateEnumerator;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Generic row-by-row aggregate enumerator for {@link PolyValue[]} rows.
 */
public class ParquetRowAggregateEnumerator extends AbstractAggregateEnumerator {

    public ParquetRowAggregateEnumerator( Enumerator<PolyValue[]> rows, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        super( () -> buildEnumerator( rows, groupFields, aggregateKinds, aggregateArgs ) );
    }


    /**
     * Creates a new instance of the enumerator.
     *
     * @param rows an original enumerator to parquet files.
     * @param groupFields a list of group by field indexes.
     * @param aggregateKinds an array of aggregation function kinds.
     * @param aggregateArgs an array of aggregation function arguments. There should be one argument per aggregation function.
     */
    private static Enumerator<PolyValue[]> buildEnumerator( Enumerator<PolyValue[]> rows, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        AggregateCallDescriptor[] aggregateCalls = aggregateCalls( aggregateKinds, aggregateArgs );
        Map<GroupKey, AggregateGroupState> aggregates = ParquetRowAggregator.aggregate( rows, aggregateCalls, new DirectAggregateRowProjector( groupFields ) );
        return Linq4j.asEnumerable( buildRows( groupFields.length, aggregates, aggregateCalls ) ).enumerator();
    }

}
