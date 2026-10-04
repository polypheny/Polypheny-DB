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
 * This is a generic grouped page reader that also supports filters.
 */
public class ParquetGroupedAggregatePageReader extends ParquetPageReader {

    public ParquetGroupedAggregatePageReader( PageReadStore pageStore, ColumnReadStore columnStore, ColumnDescriptor[] columns, ParquetPrimitivePredicate predicate ) {
        super( pageStore, columnStore, columns, predicate );
    }


    public void read( Map<GroupKey, AggregateGroupState> aggregates, AggregateCallDescriptor[] aggregateCalls, List<ParquetAdapterFilter<PolyValue>> filters, int groupFieldCount, AtomicBoolean cancelFlag ) {
        ColumnReader[] readers = readers();
        Object[] values = new Object[readers.length];

        if ( predicate != null && !filters.isEmpty() ) {
            boolean[] consumed = new boolean[readers.length];
            for ( long row = 0; row < pageStore.getRowCount(); row++ ) {
                if ( shouldStop( row, cancelFlag ) ) {
                    break;
                }
                Arrays.fill( consumed, false );
                if ( !predicate.matches( readers, consumed, values ) ) {
                    consumeRemaining( readers, consumed );
                    continue;
                }
                for ( int i = 0; i < readers.length; i++ ) {
                    if ( !consumed[i] ) {
                        values[i] = currentValue( readers[i], columns[i] );
                    }
                }
                aggregates.computeIfAbsent( groupKey( values, groupFieldCount ), ignored -> new AggregateGroupState( aggregateCalls ) ).add( values );
            }
        } else {
            for ( long row = 0; row < pageStore.getRowCount(); row++ ) {
                if ( shouldStop( row, cancelFlag ) ) {
                    break;
                }
                for ( int i = 0; i < readers.length; i++ ) {
                    values[i] = currentValue( readers[i], columns[i] );
                }
                if ( !matchesFilters( values, filters ) ) {
                    continue;
                }
                aggregates.computeIfAbsent( groupKey( values, groupFieldCount ), ignored -> new AggregateGroupState( aggregateCalls ) ).add( values );
            }
        }
    }


    private GroupKey groupKey( Object[] values, int groupFieldCount ) {
        if ( groupFieldCount <= 0 ) {
            return GroupKey.Empty;
        }
        Object[] key = new Object[groupFieldCount];
        System.arraycopy( values, 0, key, 0, groupFieldCount );
        return GroupKey.of( key );
    }


}
