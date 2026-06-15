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

package org.polypheny.db.adapter.parquet.shared.execution.aggregate;

import java.util.List;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.type.PolyType;


/**
 * Model-independent view required by Parquet aggregate optimizations.
 */
public interface ParquetAggregateSource {

    List<ParquetSourceFile> sourceFiles();

    ParquetSchemaReader schemaReader();

    int fieldCount();

    PolyType fieldType( int field );

    ParquetColumnBinding binding( int field );


    default int parquetFieldIndex( String fieldName ) {
        for ( int i = 0; i < schemaReader().getSchema().getFieldCount(); i++ ) {
            if ( schemaReader().getSchema().getFieldName( i ).equals( fieldName ) ) {
                return i;
            }
        }
        return -1;
    }


}
