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
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;

/**
 * Translates adapter filters into parquet-native predicates.
 */
public class ParquetRelFilterTranslator extends AbstractFilterTranslator {

    /**
     * Translates a Rex filter into Parquet Adapter filter form when possible.
     */
    public ParquetAdapterFilter translate( List<PolyType> fieldTypes, RexNode polyFilter ) {
        // support logical filter
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
        if ( !isFieldPredicateSupported( fieldTypes, index, parsed.operator(), right ) ) {
            return null;
        }

        // call parent functionality
        return toParquetAdapterFilter( index, parsed.operator(), right );
    }


    /**
     * Translates a logical Rex filter node (AND, OR) into a logical ParquetAdapterFilter
     * @param fieldTypes - poly field types
     * @param operator - logical operation: AND, OR
     * @param operands - child expressions inside that operation (a > 10 AND b = 20)
     * @return adapter level filter
     */
    private ParquetAdapterFilter translateLogical( List<PolyType> fieldTypes, Kind operator, List<RexNode> operands ) {
        // reject if no operands
        if ( operands.isEmpty() ) {
            return null;
        }

        // recursively translate each child Rex operand
        List<ParquetAdapterFilter> translated = operands.stream()
                .map( operand -> translate( fieldTypes, operand ) )
                .toList();

        // If any child cannot be translated, the whole logical filter is rejected
        if ( translated.stream().anyMatch( Objects::isNull ) ) {
            return null;
        }

        // If all children are valid, it creates a logical ParquetAdapterFilter
        return ParquetAdapterFilter.logical( operator, translated );
    }


    /**
     * Translates a Rex IN expression into an adapter-level OR filter
     * IN (10, 20, 30) -> OR(a = 10, a = 20, a = 30)
     * @param fieldTypes - types of fields
     * @param operands - operands
     * @return adapter level filter
     */
    private ParquetAdapterFilter translateIn( List<PolyType> fieldTypes, List<RexNode> operands ) {
        // Rejects invalid IN with no values. It needs at least column and value
        if ( operands.size() < 2 ) {
            return null;
        }

        // The first operand must be a relational column reference. Supports: column in (...)
        RexNode left = unwrapCast( operands.get( 0 ) );
        if ( !(left instanceof RexIndexRef indexRef) ) {
            return null;
        }

        // take all operands after the first one
        // Each value is converted into an equality filter

        int index = indexRef.getIndex();
        List<ParquetAdapterFilter> equalsFilters = operands.subList( 1, operands.size() ).stream()
                .map( this::unwrapCast )
                .map( value -> isFieldPredicateSupported( fieldTypes, index, Kind.EQUALS, value )
                        ? toParquetAdapterFilter( index, Kind.EQUALS, value )
                        : null )
                .toList();

        if ( equalsFilters.stream().anyMatch( Objects::isNull ) ) {
            return null;
        }

        // returns one logical ParquetAdapterFilter with Kind.OR
        return ParquetAdapterFilter.logical( Kind.OR, equalsFilters );
    }


    /**
     * Checks whether the operator can be handled by the reader.
     */
    private boolean isFieldPredicateSupported( List<PolyType> fieldTypes, int index, Kind kind, RexNode valueNode ) {
        if ( index < 0 || index >= fieldTypes.size() ) {
            return false;
        }

        return isColumnPredicateSupported( fieldTypes.get( index ), kind, valueNode );
    }

}
