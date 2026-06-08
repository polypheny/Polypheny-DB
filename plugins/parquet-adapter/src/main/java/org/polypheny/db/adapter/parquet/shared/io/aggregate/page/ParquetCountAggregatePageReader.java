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

package org.polypheny.db.adapter.parquet.shared.io.aggregate.page;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.page.PageReadStore;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateCallDescriptor;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateGroupState;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetPrimitivePredicate;
import org.polypheny.db.adapter.parquet.shared.aggregate.GroupKey;
import org.polypheny.db.type.entity.PolyValue;

/**
 * This is the no-GROUP BY COUNT(*) page reader.
 */
public class ParquetCountAggregatePageReader extends ParquetPageReader {

    public ParquetCountAggregatePageReader( PageReadStore pageStore, ColumnReadStore columnStore, ColumnDescriptor[] columns, ParquetPrimitivePredicate predicate ) {
        super( pageStore, columnStore, columns, predicate );
    }


    public void read( Map<GroupKey, AggregateGroupState> aggregates, AggregateCallDescriptor[] aggregateCalls, List<ParquetAdapterFilter<PolyValue>> filters, AtomicBoolean cancelFlag ) {
        long count = 0;
        ColumnReader[] readers = readers();

        if ( predicate != null ) {
            Object[] values = new Object[readers.length];
            boolean[] consumed = new boolean[readers.length];
            for ( long row = 0; row < pageStore.getRowCount(); row++ ) {
                if ( shouldStop( row, cancelFlag ) ) {
                    break;
                }
                Arrays.fill( consumed, false );
                if ( predicate.matches( readers, consumed, values ) ) {
                    count++;
                }
                consumeRemaining( readers, consumed );
            }
            aggregates.computeIfAbsent( GroupKey.Empty, ignored -> new AggregateGroupState( aggregateCalls ) ).increment( count );
            return;
        }

        Object[] values = new Object[readers.length];
        for ( long row = 0; row < pageStore.getRowCount(); row++ ) {
            if ( shouldStop( row, cancelFlag ) ) {
                break;
            }
            for ( int i = 0; i < readers.length; i++ ) {
                values[i] = currentValue( readers[i], columns[i] );
            }
            if ( matchesFilters( values, filters ) ) {
                count++;
            }
        }

        aggregates.computeIfAbsent( GroupKey.Empty, ignored -> new AggregateGroupState( aggregateCalls ) ).increment( count );
    }

}
