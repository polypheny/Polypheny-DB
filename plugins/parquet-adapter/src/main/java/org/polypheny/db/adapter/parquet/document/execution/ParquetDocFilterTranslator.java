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
import org.polypheny.db.adapter.parquet.shared.execution.AbstractFilterTranslator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;

/**
 * Translates supported document filter expressions into Parquet filter instances.
 */
public class ParquetDocFilterTranslator extends AbstractFilterTranslator {

    /**
     * Translate Polypheny filter to ParquetFilter
     *
     * @param columns - list of valid columns
     * @param filter - RexNode
     * @return ParquetFilter
     */
    public ParquetAdapterFilter translate( List<ExportedColumn> columns, RexNode filter ) {
        ParsedFilter parsed = parse( filter );
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
        if ( !isValueOperand( right ) ) {
            return null;
        }

        for ( ExportedColumn column : columns ) {
            if ( column.name().equalsIgnoreCase( fieldName ) ) {
                if ( !isColumnPredicateSupported( column.type(), parsed.operator(), right ) ) {
                    return null;
                }
                return toParquetAdapterFilter( column.physicalPosition(), parsed.operator(), right );
            }
        }
        return null;
    }

}
