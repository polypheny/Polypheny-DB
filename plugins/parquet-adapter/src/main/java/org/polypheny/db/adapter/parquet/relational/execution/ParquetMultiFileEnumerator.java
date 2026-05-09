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

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Presents multiple per-file Parquet enumerators as one continuous enumerator.
 */
public class ParquetMultiFileEnumerator implements Enumerator<PolyValue[]> {

    private final Iterator<ParquetSourceFile> files;
    private final Function<ParquetSourceFile, Enumerator<PolyValue[]>> enumeratorFactory;
    private Enumerator<PolyValue[]> currentEnumerator;
    private PolyValue[] current;


    public ParquetMultiFileEnumerator( List<ParquetSourceFile> files, Function<ParquetSourceFile, Enumerator<PolyValue[]>> enumeratorFactory ) {
        this.files = List.copyOf( files ).iterator();
        this.enumeratorFactory = enumeratorFactory;
    }


    @Override
    public PolyValue[] current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        try {
            for ( ; ; ) {
                if ( currentEnumerator == null ) {
                    if ( !files.hasNext() ) {
                        current = null;
                        return false;
                    }
                    currentEnumerator = enumeratorFactory.apply( files.next() );
                }

                if ( currentEnumerator.moveNext() ) {
                    current = currentEnumerator.current();
                    return true;
                }

                currentEnumerator.close();
                currentEnumerator = null;
            }
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error while reading multiple parquet files", e );
        }
    }


    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {
        if ( currentEnumerator != null ) {
            currentEnumerator.close();
            currentEnumerator = null;
        }
    }

}
