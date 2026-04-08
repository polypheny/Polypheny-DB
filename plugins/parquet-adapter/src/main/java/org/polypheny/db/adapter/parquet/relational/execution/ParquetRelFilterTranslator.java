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

import java.util.List;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.execution.ParquetFilterTranslationSupport;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;

/**
 * Translates adapter filters into parquet-native predicates.
 */
public class ParquetRelFilterTranslator {

    /**
     * Translates a Rex filter into Parquet filter form when possible.
     */
    public ParquetAdapterFilter translate( List<PolyType> fieldTypes, RexNode polyFilter ) {
        ParquetFilterTranslationSupport.ParsedFilter parsed = ParquetFilterTranslationSupport.parse( polyFilter );
        if ( parsed == null ) {
            return null;
        }

        RexNode left = parsed.left();
        RexNode right = parsed.right();

        if ( !(left instanceof RexIndexRef indexRef) || !ParquetFilterTranslationSupport.isValueOperand( right ) ) {
            return null;
        }

        int index = indexRef.getIndex();
        if ( index < 0 || index >= fieldTypes.size() ) {
            return null;
        }

        if ( !isPushdownSupported( fieldTypes, index, polyFilter.getKind(), right ) ) {
            return null;
        }

        return ParquetFilterTranslationSupport.toParquetAdapterFilter( index, polyFilter.getKind(), right );
    }

    /**
     * Checks whether the operator can be handled by the reader.
     */
    private boolean isPushdownSupported( List<PolyType> fieldTypes, int index, Kind kind, RexNode valueNode ) {
        PolyType type = fieldTypes.get( index );
        return switch ( type ) {
            case BOOLEAN, VARCHAR, CHAR, TEXT -> kind == Kind.EQUALS || kind == Kind.NOT_EQUALS;
            case INTEGER, BIGINT, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP -> true;
            default -> false;
        } && (valueNode instanceof org.polypheny.db.rex.RexDynamicParam
                || (valueNode instanceof org.polypheny.db.rex.RexLiteral literal && literal.getValue() != null));
    }
}
