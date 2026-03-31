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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.parquet.example.data.Group;
import org.polypheny.db.adapter.parquet.model.AdapterFilter;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

public class ParquetTableEnumerator extends AbstractParquetEnumerator implements Enumerator<PolyValue[]> {

    public ParquetTableEnumerator(Source source, AtomicBoolean cancelFlag, int[] fields ) {
        super( source, cancelFlag, fields, List.of() );
    }

    public ParquetTableEnumerator(Source source, AtomicBoolean cancelFlag, int[] fields, List<AdapterFilter> filters ) {
        super( source, cancelFlag, fields, filters );
    }

    protected PolyValue[] extractRow( Group group ) {
        var projectionSchema = reader.getProjectionSchema();
        PolyValue[] row = new PolyValue[projectionSchema.getFieldCount()];
        for ( int readIndex = 0; readIndex < projectionSchema.getFieldCount(); readIndex++ ) {
            var type = projectionSchema.getType( readIndex );
            row[readIndex] = valueExtractor.extractValue( group, readIndex, type );
        }
        return row;
    }

}
