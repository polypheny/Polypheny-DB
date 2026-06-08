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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.page.PageReadStore;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetPrimitiveValueFilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.io.ParquetCancellation;
import org.polypheny.db.adapter.parquet.shared.io.ParquetPrimitivePredicate;
import org.polypheny.db.type.entity.PolyValue;

public abstract class ParquetPageReader {

    protected final PageReadStore pageStore;
    protected final ColumnReadStore columnStore;
    protected final ColumnDescriptor[] columns;
    protected final ParquetPrimitivePredicate predicate;

    private final ParquetPrimitiveValueFilterEvaluator filterEvaluator;


    protected ParquetPageReader( PageReadStore pageStore, ColumnReadStore columnStore, ColumnDescriptor[] columns, ParquetPrimitivePredicate predicate ) {
        this.pageStore = pageStore;
        this.columnStore = columnStore;
        this.columns = columns;
        this.predicate = predicate;
        this.filterEvaluator = new ParquetPrimitiveValueFilterEvaluator();
    }


    protected ColumnReader[] readers() {
        if ( columnStore == null ) {
            return new ColumnReader[0];
        }
        ColumnReader[] readers = new ColumnReader[columns.length];
        for ( int i = 0; i < columns.length; i++ ) {
            readers[i] = columnStore.getColumnReader( columns[i] );
        }
        return readers;
    }


    protected boolean matchesFilters( Object[] values, List<ParquetAdapterFilter<PolyValue>> filters ) {
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            if ( !filterEvaluator.matches( values, filter ) ) {
                return false;
            }
        }
        return true;
    }


    protected Object currentValue( ColumnReader reader, ColumnDescriptor descriptor ) {
        try {
            return descriptor.getMaxDefinitionLevel() == 0 || reader.getCurrentDefinitionLevel() == descriptor.getMaxDefinitionLevel()
                    ? ParquetPrimitivePredicate.readValue( reader, descriptor.getPrimitiveType() )
                    : null;
        } finally {
            reader.consume();
        }
    }


    protected void consumeRemaining( ColumnReader[] readers, boolean[] consumed ) {
        for ( int i = 0; i < readers.length; i++ ) {
            if ( !consumed[i] ) {
                readers[i].consume();
            }
        }
    }


    protected boolean shouldStop( long row, AtomicBoolean cancelFlag ) {
        return ParquetCancellation.shouldStop( row, cancelFlag );
    }

}
