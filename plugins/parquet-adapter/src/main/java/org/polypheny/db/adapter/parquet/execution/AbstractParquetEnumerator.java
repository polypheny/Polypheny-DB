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

package org.polypheny.db.adapter.parquet.execution;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.parquet.example.data.Group;
import org.polypheny.db.adapter.parquet.model.AdapterFilter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractParquetEnumerator implements Enumerator<PolyValue[]> {

    protected final ParquetGroupReader reader;
    protected final ValueExtractor valueExtractor;
    private PolyValue[] current;


    public AbstractParquetEnumerator(Source source, AtomicBoolean cancelFlag, int[] fields, List<AdapterFilter> filters ) {
        this.reader = new ParquetGroupReader( source, cancelFlag, fields, filters );
        this.valueExtractor = new ValueExtractor();
    }


    @Override
    public PolyValue[] current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        try {
            Group group = reader.next();
            if ( group == null ) {
                current = null;
                return false;
            }
            current = extractRow( group );
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
        reader.close();
    }

    /*
    extractValue logic depends on source type (relational/document)
     */
    protected abstract PolyValue[] extractRow( Group group );

}
