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
import org.apache.calcite.linq4j.Enumerator;
import org.apache.parquet.example.data.Group;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetGroupFilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Enumerator base class.
 * Manages row iteration, projection handling, cancellation support,
 * and reader lifecycle for both models.
 */
public abstract class AbstractParquetEnumerator implements Enumerator<PolyValue[]> {

    protected final ParquetSourceReader reader;
    protected final List<ParquetAdapterFilter<PolyValue>> filters;
    protected final ParquetGroupFilterEvaluator filterEvaluator;
    private final boolean readerOwner;
    private final Queue<PolyValue[]> rows = new ArrayDeque<>();
    private PolyValue[] current;


    protected AbstractParquetEnumerator( ParquetSourceReader reader, FiltersContainer filtersContainer, ParquetGroupFilterEvaluator filterEvaluator ) {
        this( reader, filtersContainer, filterEvaluator, true );
    }


    protected AbstractParquetEnumerator( ParquetSourceReader reader, FiltersContainer filtersContainer, ParquetGroupFilterEvaluator filterEvaluator, boolean readerOwner ) {
        this.filters = filtersContainer.adapterFilters();
        this.reader = reader;
        this.filterEvaluator = filterEvaluator;
        this.readerOwner = readerOwner;
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
        if ( readerOwner ) {
            try {
                reader.close();
            } catch ( Exception e ) {
                throw new GenericRuntimeException( "Error closing parquet reader", e );
            }
        }
    }


    /**
     * extractValue logic depends on source type (relational/document)
     */
    protected abstract PolyValue[] extractRow( Group group );


    /**
     * The function that turns one input Parquet row into zero (filtered), one, or many output relational rows
     * It lets the shared enumerator support both simple one-row scans and repeated nested scans that emit multiple rows.
     *
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
     * default implementation - return provided group
     * repeated nested enumerator overrides this and returns all nested groups
     *
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
            if ( !filterEvaluator.matches( group, filter ) ) {
                return false;
            }
        }
        return true;
    }


    protected ParquetValueExtractor valueExtractor() {
        return filterEvaluator.valueExtractor();
    }

}
