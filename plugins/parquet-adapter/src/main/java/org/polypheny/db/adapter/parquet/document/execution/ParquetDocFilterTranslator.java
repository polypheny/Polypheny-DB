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
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyValue;

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
    public ParquetAdapterFilter<PolyValue> translate( List<ExportedColumn> columns, RexNode filter ) {
        ParsedFilter parsed = parse( filter );
        if ( parsed == null ) {
            return null;
        }

        RexNode left = parsed.left();
        RexNode right = parsed.right();

        String fieldName = fieldName( left );
        if ( fieldName == null ) {
            return null;
        }

        if ( !isValueOperand( right ) ) {
            return null;
        }

        for ( ExportedColumn column : columns ) {
            if ( column.name().equalsIgnoreCase( fieldName ) ) {
                return toParquetAdapterFilter( column.physicalPosition(), parsed.operator(), right );
            }
        }
        return null;
    }


    private String fieldName( RexNode node ) {
        if ( node instanceof RexNameRef nameRef ) {
            return nameRef.names.size() == 1 ? nameRef.names.get( 0 ) : null;
        }

        if ( !(node instanceof RexCall call) || call.getKind() != Kind.MQL_QUERY_VALUE || call.getOperands().size() != 2 ) {
            return null;
        }

        RexNode path = call.getOperands().get( 1 );
        if ( !(path instanceof RexCall pathCall) || pathCall.getKind() != Kind.ARRAY_VALUE_CONSTRUCTOR || pathCall.getOperands().size() != 1 ) {
            return null;
        }

        RexNode element = pathCall.getOperands().get( 0 );
        if ( !(element instanceof RexLiteral literal) || literal.getValue() == null || !literal.getValue().isString() ) {
            return null;
        }

        return literal.getValue().asString().value;
    }

}
