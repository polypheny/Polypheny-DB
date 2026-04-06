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

package org.polypheny.db.adapter.parquet.document.execution;


import java.util.List;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.parquet.shared.execution.ParquetFilterTranslationSupport;
import org.polypheny.db.adapter.parquet.shared.model.AdapterFilter;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;

public class ParquetDocFilterTranslator {

    public AdapterFilter translate( List<ExportedColumn> columns, RexNode filter ) {
        ParquetFilterTranslationSupport.ParsedFilter parsed = ParquetFilterTranslationSupport.parse( filter );
        if ( parsed == null ) {
            return null;
        }

        RexNode left = parsed.left();
        RexNode right = parsed.right();

        if ( !(left instanceof RexNameRef nameRef) ) {
            return null;
        }

        if ( nameRef.names.size() != 1 ) {
            return null;
        }

        String fieldName = nameRef.names.get( 0 );
        if ( !ParquetFilterTranslationSupport.isValueOperand( right ) ) {
            return null;
        }

        for ( ExportedColumn column : columns ) {
            if ( column.name().equalsIgnoreCase( fieldName ) ) {
                return ParquetFilterTranslationSupport.toAdapterFilter( column.physicalPosition(), filter.getKind(), right );
            }
        }
        return null;
    }

}
