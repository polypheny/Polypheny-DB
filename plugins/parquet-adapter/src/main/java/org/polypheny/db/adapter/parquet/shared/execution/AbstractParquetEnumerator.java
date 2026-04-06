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

package org.polypheny.db.adapter.parquet.shared.execution;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.model.AdapterFilter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

public abstract class AbstractParquetEnumerator implements Enumerator<PolyValue[]> {

    protected final ParquetGroupReader reader;
    protected final ParquetValueExtractor valueExtractor;
    protected final List<AdapterFilter> filters;
    private PolyValue[] current;


    public AbstractParquetEnumerator( Source source, AtomicBoolean cancelFlag, int[] fields, List<AdapterFilter> filters, ParquetValueExtractor valueExtractor ) {
        this.reader = new ParquetGroupReader( source, cancelFlag, fields, filters );
        this.valueExtractor = valueExtractor;
        this.filters = filters == null ? List.of() : List.copyOf( filters );
    }


    @Override
    public PolyValue[] current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        try {
            for ( ; ; ) {
                // group (single row) can be still filtered, while parquet filter works on the row group level
                Group group = reader.next();
                if ( group == null ) {
                    current = null;
                    return false;
                }
                if ( !accept( group ) ) {
                    continue;
                }
                current = extractRow( group );
                return true;
            }
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error while reading parquet data", e );
        }
    }


    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {
        reader.close();
    }


    /*
    extractValue logic depends on source type (relational/document)
     */
    protected abstract PolyValue[] extractRow( Group group );


    protected boolean accept( Group group ) {
        for ( AdapterFilter filter : filters ) {
            if ( !matches( group, filter ) ) {
                return false;
            }
        }
        return true;
    }


    private boolean matches( Group group, AdapterFilter filter ) {
        // if filter is invalid row should be displayed
        if ( filter.polyValue() == null || filter.columnIndex() < 0 || filter.columnIndex() >= reader.getProjectionSchema().getFieldCount() ) {
            return true;
        }

        // this row does not have a value for that Parquet field
        if ( group.getFieldRepetitionCount( filter.columnIndex() ) == 0 ) {
            return false;
        }

        // call overloaded functionality
        PolyValue actual = extractFilterValue( group, filter );
        PolyValue expected = filter.polyValue();

        if ( actual == null || actual.isNull() || expected.isNull() ) {
            return false;
        }

        // apply filter on the row level
        return switch ( filter.operator() ) {
            case EQUALS -> actual.equals( expected );
            case NOT_EQUALS -> !actual.equals( expected );
            case GREATER_THAN -> compare( actual, expected ) > 0;
            case GREATER_THAN_OR_EQUAL -> compare( actual, expected ) >= 0;
            case LESS_THAN -> compare( actual, expected ) < 0;
            case LESS_THAN_OR_EQUAL -> compare( actual, expected ) <= 0;
            default -> true;
        };
    }


    protected PolyValue extractFilterValue( Group group, AdapterFilter filter ) {
        Type field = reader.getProjectionSchema().getType( filter.columnIndex() );
        return valueExtractor.extractValue( group, filter.columnIndex(), field );
    }


    protected int compare( PolyValue actual, PolyValue expected ) {
        try {
            return actual.compareTo( expected );
        } catch ( Exception e ) {
            return actual.toString().compareTo( expected.toString() );
        }
    }

}
