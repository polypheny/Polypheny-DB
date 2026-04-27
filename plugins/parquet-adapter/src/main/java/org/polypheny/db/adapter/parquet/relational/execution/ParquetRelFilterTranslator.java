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
import java.util.Objects;
import org.polypheny.db.adapter.parquet.shared.execution.AbstractFilterTranslator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;

/**
 * Translates adapter filters into parquet-native predicates.
 */
public class ParquetRelFilterTranslator extends AbstractFilterTranslator {

    /**
     * Translates a Rex filter into Parquet filter form when possible.
     */
    public ParquetAdapterFilter translate( List<PolyType> fieldTypes, RexNode polyFilter ) {
        if ( polyFilter instanceof RexCall call ) {
            if ( polyFilter.isA( Kind.AND ) || polyFilter.isA( Kind.OR ) ) {
                return translateLogical( fieldTypes, polyFilter.getKind(), call.getOperands() );
            }
            if ( polyFilter.isA( Kind.NOT ) && call.getOperands().size() == 1 ) {
                ParquetAdapterFilter operand = translate( fieldTypes, call.getOperands().get( 0 ) );
                return operand == null ? null : ParquetAdapterFilter.logical( Kind.NOT, List.of( operand ) );
            }
            if ( polyFilter.isA( Kind.IN ) ) {
                return translateIn( fieldTypes, call.getOperands() );
            }
        }

        ParsedFilter parsed = parse( polyFilter );
        if ( parsed == null ) {
            return null;
        }

        RexNode left = parsed.left();
        RexNode right = parsed.right();

        if ( !(left instanceof RexIndexRef indexRef) || !isValueOperand( right ) ) {
            return null;
        }

        int index = indexRef.getIndex();
        if ( !isPushdownSupported( fieldTypes, index, parsed.operator(), right ) ) {
            return null;
        }

        return toParquetAdapterFilter( index, parsed.operator(), right );
    }


    private ParquetAdapterFilter translateLogical( List<PolyType> fieldTypes, Kind operator, List<RexNode> operands ) {
        if ( operands.isEmpty() ) {
            return null;
        }

        List<ParquetAdapterFilter> translated = operands.stream()
                .map( operand -> translate( fieldTypes, operand ) )
                .toList();

        if ( translated.stream().anyMatch( Objects::isNull ) ) {
            return null;
        }

        return ParquetAdapterFilter.logical( operator, translated );
    }


    private ParquetAdapterFilter translateIn( List<PolyType> fieldTypes, List<RexNode> operands ) {
        if ( operands.size() < 2 ) {
            return null;
        }

        RexNode left = unwrapCast( operands.get( 0 ) );
        if ( !(left instanceof RexIndexRef indexRef) ) {
            return null;
        }

        int index = indexRef.getIndex();
        List<ParquetAdapterFilter> equalsFilters = operands.subList( 1, operands.size() ).stream()
                .map( this::unwrapCast )
                .map( value -> isPushdownSupported( fieldTypes, index, Kind.EQUALS, value )
                        ? toParquetAdapterFilter( index, Kind.EQUALS, value )
                        : null )
                .toList();

        if ( equalsFilters.stream().anyMatch( Objects::isNull ) ) {
            return null;
        }

        return ParquetAdapterFilter.logical( Kind.OR, equalsFilters );
    }


    /**
     * Checks whether the operator can be handled by the reader.
     */
    private boolean isPushdownSupported( List<PolyType> fieldTypes, int index, Kind kind, RexNode valueNode ) {
        if ( index < 0 || index >= fieldTypes.size() ) {
            return false;
        }

        PolyType type = fieldTypes.get( index );
        return switch ( type ) {
            case BOOLEAN, VARCHAR, CHAR, TEXT -> kind == Kind.EQUALS || kind == Kind.NOT_EQUALS;
            case INTEGER, BIGINT, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP -> true;
            default -> false;
        } && (valueNode instanceof RexDynamicParam || (valueNode instanceof RexLiteral literal && literal.getValue() != null));
    }

}
