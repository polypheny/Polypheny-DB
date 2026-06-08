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

package org.polypheny.db.adapter.parquet.shared.filter;

import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Evaluates Parquet adapter filters against a concrete context.
 * A null result means the evaluator cannot decide, so callers that prune data should keep it.
 */
public abstract class ParquetFilterEvaluator<C, V extends Comparable<V>> {

    public boolean matches( C context, ParquetAdapterFilter<V> filter ) {
        return !Boolean.FALSE.equals( evaluate( context, filter ) );
    }


    public Boolean evaluate( C context, ParquetAdapterFilter<V> filter ) {
        if ( filter.isLogical() ) {
            return switch ( filter.operator() ) {
                case AND -> evaluateAnd( context, filter );
                case OR -> evaluateOr( context, filter );
                case NOT -> filter.operands().size() == 1 ? negate( evaluate( context, filter.operands().get( 0 ) ) ) : null;
                default -> null;
            };
        }
        return evaluateLeaf( context, filter );
    }


    protected abstract Boolean evaluateLeaf( C context, ParquetAdapterFilter<V> filter );


    protected Boolean matchesValue( V actual, Kind operator, V expected ) {
        if ( operator == Kind.IS_NULL ) {
            return isNull( actual );
        }
        if ( operator == Kind.IS_NOT_NULL ) {
            return !isNull( actual );
        }

        if ( isNull( actual ) || isNull( expected ) ) {
            return false;
        }

        return switch ( operator ) {
            case EQUALS -> actual.equals( expected );
            case NOT_EQUALS -> !actual.equals( expected );
            case GREATER_THAN -> compare( actual, expected ) > 0;
            case GREATER_THAN_OR_EQUAL -> compare( actual, expected ) >= 0;
            case LESS_THAN -> compare( actual, expected ) < 0;
            case LESS_THAN_OR_EQUAL -> compare( actual, expected ) <= 0;
            default -> true;
        };
    }


    private Boolean evaluateAnd( C context, ParquetAdapterFilter<V> filter ) {
        boolean hasUnknown = false;
        for ( ParquetAdapterFilter<V> operand : filter.operands() ) {
            Boolean result = evaluate( context, operand );
            if ( Boolean.FALSE.equals( result ) ) {
                return false;
            }
            hasUnknown |= result == null;
        }
        return hasUnknown ? null : true;
    }


    private Boolean evaluateOr( C context, ParquetAdapterFilter<V> filter ) {
        boolean hasUnknown = false;
        for ( ParquetAdapterFilter<V> operand : filter.operands() ) {
            Boolean result = evaluate( context, operand );
            if ( Boolean.TRUE.equals( result ) ) {
                return true;
            }
            hasUnknown |= result == null;
        }
        return hasUnknown ? null : false;
    }


    private Boolean negate( Boolean value ) {
        return value == null ? null : !value;
    }


    private int compare( V actual, V expected ) {
        try {
            return actual.compareTo( expected );
        } catch ( Exception e ) {
            return actual.toString().compareTo( expected.toString() );
        }
    }


    private boolean isNull( V value ) {
        return value == null || (value instanceof PolyValue pv && pv.isNull());
    }

}
