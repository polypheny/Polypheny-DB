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

package org.polypheny.db.adapter.parquet.shared.execution;

import java.util.List;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;

public abstract class AbstractFilterTranslator {

    protected ParsedFilter parse( RexNode filter ) {
        if ( !(filter instanceof RexCall call) || !isSupportedOperator( filter.getKind() ) || call.getOperands().size() != 2 ) {
            return null;
        }

        RexNode left = unwrapCast( call.getOperands().get( 0 ) );
        RexNode right = unwrapCast( call.getOperands().get( 1 ) );
        Kind operator = filter.getKind();

        if ( isValueOperand( left ) && !isValueOperand( right ) ) {
            RexNode tmp = left;
            left = right;
            right = tmp;
            operator = reverse( operator );
        }

        return new ParsedFilter( left, right, operator );
    }


    protected ParquetAdapterFilter toParquetAdapterFilter( int columnIndex, Kind operator, RexNode valueNode ) {
        if ( valueNode instanceof RexLiteral literal ) {
            if ( literal.getValue() == null ) {
                return null;
            }
            return new ParquetAdapterFilter( columnIndex, operator, literal.getValue() );
        }

        if ( valueNode instanceof RexDynamicParam dynamicParam ) {
            return new ParquetAdapterFilter( columnIndex, List.of(), operator, null, dynamicParam.getIndex() );
        }

        return null;
    }


    protected boolean isValueOperand( RexNode node ) {
        return node instanceof RexLiteral || node instanceof RexDynamicParam;
    }


    protected boolean isSupportedOperator( Kind kind ) {
        return kind == Kind.EQUALS
                || kind == Kind.NOT_EQUALS
                || kind == Kind.GREATER_THAN
                || kind == Kind.GREATER_THAN_OR_EQUAL
                || kind == Kind.LESS_THAN
                || kind == Kind.LESS_THAN_OR_EQUAL;
    }


    protected Kind reverse( Kind kind ) {
        return switch ( kind ) {
            case GREATER_THAN -> Kind.LESS_THAN;
            case GREATER_THAN_OR_EQUAL -> Kind.LESS_THAN_OR_EQUAL;
            case LESS_THAN -> Kind.GREATER_THAN;
            case LESS_THAN_OR_EQUAL -> Kind.GREATER_THAN_OR_EQUAL;
            default -> kind;
        };
    }


    protected RexNode unwrapCast( RexNode node ) {
        while ( node.isA( Kind.CAST ) ) {
            node = ((RexCall) node).getOperands().get( 0 );
        }
        return node;
    }


    protected record ParsedFilter( RexNode left, RexNode right, Kind operator ) {

    }

}
