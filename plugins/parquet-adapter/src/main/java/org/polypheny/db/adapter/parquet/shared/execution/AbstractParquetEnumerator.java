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

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

/**
 * Enumerator base class.
 * Manages row iteration, projection handling, cancellation support,
 * and reader lifecycle for both models.
 */
public abstract class AbstractParquetEnumerator implements Enumerator<PolyValue[]> {

    protected final ParquetSourceReader reader;
    protected final ParquetValueExtractor valueExtractor;
    protected final List<ParquetAdapterFilter> filters;
    private final Queue<PolyValue[]> rows = new ArrayDeque<>();
    private PolyValue[] current;


    public AbstractParquetEnumerator( Source source, AtomicBoolean cancelFlag, int[] fields, List<ParquetAdapterFilter> filters, ParquetValueExtractor valueExtractor ) {
        this.filters = filters == null ? List.of() : List.copyOf( filters );
        this.reader = new ParquetSourceReader( source, cancelFlag, fields, this.filters );
        this.valueExtractor = valueExtractor;
    }


    @Override
    public PolyValue[] current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        try {
            // fill rows queue from parquet queue/row
            while ( rows.isEmpty() ) {
                for ( ; ; ) {
                    // group (single row) can be still filtered, while parquet filter
                    // works on the row group level
                    Group group = reader.next();
                    if ( group == null ) {
                        current = null;
                        return false;
                    }
                    // turn single parquet row (group) into multiple rows for nested repeated fields and store them in queue rows
                    enqueueRows( group, rows );
                    if ( rows.isEmpty() ) {
                        continue;
                    }
                    break;
                }
            }
            current = rows.remove();
            return true;
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
        try {
            reader.close();
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error closing parquet reader", e );
        }
    }


    /**
     * extractValue logic depends on source type (relational/document)
     */
    protected abstract PolyValue[] extractRow( Group group );


    /**
     * The function that turns one input Parquet row into zero (filtered), one, or many output relational rows
     * It lets the shared enumerator support both simple one-row scans and repeated nested scans that emit multiple rows.
     * @param group - parquet group/row
     * @param rows - Queue<PolyValue[]> - output to fill
     */
    protected void enqueueRows( Group group, Queue<PolyValue[]> rows ) {
        for ( var row : expandRow( group ) ) {
            if ( !accept( row ) ) { //apply filter on adapter level
                continue;
            }
            // converts the accepted Parquet row/group into PolyValue[]
            PolyValue[] extracted = extractRow( row );
            if ( extracted != null ) {
                rows.add( extracted ); // stores produced rows into queue
            }
        }
    }


    /**
     * decides how many logical rows come from this input row
     * default implementation - return
     * repeated nested enumerator overrides this and returns all nested groups
     * @param group - parquet group/row
     * @return List<Group>
     */
    protected List<Group> expandRow( Group group ) {
        return List.of( group );
    }


    /**
     * apply filter on adapter level for each row
     * by calling matches()
     *
     * @param group - parquet row
     * @return boolean
     */
    protected boolean accept( Group group ) {
        for ( var filter : filters ) {
            if ( !matches( group, filter ) ) {
                return false;
            }
        }
        return true;
    }


    private boolean matches( Group group, ParquetAdapterFilter filter ) {
        // if filter is invalid row should be displayed
        if ( filter.polyValue() == null || filter.columnIndex() < 0 || filter.columnIndex() >= reader.getProjectionSchema().getFieldCount() ) {
            return true;
        }

        // this row does not have a value for that Parquet field
        if ( group.getFieldRepetitionCount( filter.columnIndex() ) == 0 ) {
            return false;
        }

        // call overloaded functionality
        PolyValue actual = extractValue( group, filter );
        PolyValue expected = filter.polyValue();

        if ( actual == null || actual.isNull() || expected.isNull() ) {
            return false;
        }

        // apply filter on the row level
        return matches( actual, filter.operator(), expected );
    }


    protected boolean matches( PolyValue actual, Kind operator, PolyValue expected ) {
        if ( actual == null || actual.isNull() || expected == null || expected.isNull() ) {
            return false;
        }

        // apply filter on the row level
        return switch ( operator ) {
            case EQUALS -> actual.equals( expected );
            case NOT_EQUALS -> !actual.equals( expected );
            case GREATER_THAN -> compare( actual, expected ) > 0;
            case GREATER_THAN_OR_EQUAL -> compare( actual, expected ) >= 0;
            case LESS_THAN -> compare( actual, expected ) < 0;
            case LESS_THAN_OR_EQUAL -> compare( actual, expected ) <= 0;
            default -> true;
        };
    }


    /**
     * Extract value from parquet group according to filter column index
     * @param group - parquet group
     * @param filter - ParquetAdapterFilter
     * @return - PolyValue
     */
    protected PolyValue extractValue( Group group, ParquetAdapterFilter filter ) {
        if ( filter.pathElements().isEmpty() ) {
            Type field = reader.getProjectionSchema().getType( filter.columnIndex() );
            return valueExtractor.extractValue( group, filter.columnIndex(), field );
        }
        return valueExtractor.extractValue( group, filter.pathElements() );
    }


    protected int compare( PolyValue actual, PolyValue expected ) {
        try {
            return actual.compareTo( expected );
        } catch ( Exception e ) {
            return actual.toString().compareTo( expected.toString() );
        }
    }

}
