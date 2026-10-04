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

package org.polypheny.db.adapter.parquet.relational.execution;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetFilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.io.ParquetPrimitiveRowReader;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Relational enumerator for flat primitive projections.
 * It supports only flat primitive non-repeated projections. No nested structures.
 * It avoids GroupRecordConverter and materializes directly into PolyValue[].
 * This enumerator is faster than regular ParquetRelEnumerator.
 */
public class ParquetRowRelEnumerator implements Enumerator<PolyValue[]> {

    private final ParquetPrimitiveRowReader reader;
    private final List<ParquetAdapterFilter<PolyValue>> filters;
    private final int[] outputIndexes;
    private final PolyValue[] outputConstants;
    private final PrimitiveRowFilterEvaluator filterEvaluator = new PrimitiveRowFilterEvaluator();
    private PolyValue[] current;


    public ParquetRowRelEnumerator( ParquetPrimitiveRowReader reader, List<ParquetAdapterFilter<PolyValue>> filters, int[] outputIndexes, PolyValue[] outputConstants ) {
        this.reader = reader;
        this.filters = filters == null ? List.of() : List.copyOf( filters );
        this.outputIndexes = outputIndexes == null ? null : Arrays.copyOf( outputIndexes, outputIndexes.length );
        this.outputConstants = outputConstants == null ? null : Arrays.copyOf( outputConstants, outputConstants.length );
    }


    @Override
    public PolyValue[] current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        for ( ; ; ) {
            PolyValue[] row = reader.next();
            if ( row == null ) {
                current = null;
                return false;
            }
            if ( !accept( row ) ) {
                continue;
            }
            current = project( row );
            return true;
        }
    }


    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {
        try {
            reader.close();
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error closing parquet reader", e );
        }
    }


    private boolean accept( PolyValue[] row ) {
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            if ( !filterEvaluator.matches( row, filter ) ) {
                return false;
            }
        }
        return true;
    }


    private PolyValue[] project( PolyValue[] row ) {
        if ( outputIndexes == null ) {
            return row;
        }
        PolyValue[] projected = new PolyValue[outputIndexes.length];
        for ( int i = 0; i < outputIndexes.length; i++ ) {
            projected[i] = outputIndexes[i] < 0 ? outputConstants[i] : row[outputIndexes[i]];
        }
        return projected;
    }


    private static class PrimitiveRowFilterEvaluator extends ParquetFilterEvaluator<PolyValue[], PolyValue> {

        @Override
        protected Boolean evaluateLeaf( PolyValue[] row, ParquetAdapterFilter<PolyValue> filter ) {
            if ( filter.columnIndex() < 0 || filter.columnIndex() >= row.length ) {
                return null;
            }
            PolyValue value = row[filter.columnIndex()];
            return matchesValue( value == null ? PolyNull.NULL : value, filter.operator(), filter.value() );
        }

    }

}
