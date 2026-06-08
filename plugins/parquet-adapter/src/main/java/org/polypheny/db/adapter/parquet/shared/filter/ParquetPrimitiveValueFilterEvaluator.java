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

import org.apache.parquet.io.api.Binary;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Evaluates adapter filters against primitive values read from Parquet pages.
 */
public class ParquetPrimitiveValueFilterEvaluator extends ParquetFilterEvaluator<Object[], PolyValue> {

    @Override
    protected Boolean evaluateLeaf( Object[] values, ParquetAdapterFilter<PolyValue> filter ) {
        if ( filter.columnIndex() < 0 || filter.columnIndex() >= values.length ) {
            return null;
        }

        Object actual = values[filter.columnIndex()];
        PolyValue expected = filter.value();
        return matchesValue( actual, filter.operator(), expected );
    }


    private Boolean matchesValue( Object actual, Kind operator, PolyValue expected ) {
        if ( operator == Kind.IS_NULL ) {
            return actual == null;
        }
        if ( operator == Kind.IS_NOT_NULL ) {
            return actual != null;
        }
        if ( actual == null || expected == null || expected.isNull() ) {
            return false;
        }

        int comparison = compare( actual, expected );
        return switch ( operator ) {
            case EQUALS -> comparison == 0;
            case NOT_EQUALS -> comparison != 0;
            case GREATER_THAN -> comparison > 0;
            case GREATER_THAN_OR_EQUAL -> comparison >= 0;
            case LESS_THAN -> comparison < 0;
            case LESS_THAN_OR_EQUAL -> comparison <= 0;
            default -> true;
        };
    }


    private int compare( Object actual, PolyValue expected ) {
        if ( actual instanceof Number number && expected.isNumber() ) {
            return Double.compare( number.doubleValue(), expected.asNumber().doubleValue() );
        }
        if ( actual instanceof Boolean bool ) {
            boolean expectedValue = expected.isBoolean() ? expected.asBoolean().value : Boolean.parseBoolean( expected.toString() );
            return Boolean.compare( bool, expectedValue );
        }
        if ( actual instanceof Binary binary ) {
            String expectedValue = expected.isString() ? expected.asString().value : expected.toString();
            return binary.toStringUsingUTF8().compareTo( expectedValue );
        }
        if ( actual instanceof Comparable<?> comparable && actual.getClass().isInstance( expected ) ) {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            int result = ((Comparable) comparable).compareTo( expected );
            return result;
        }
        return actual.toString().compareTo( expected.toString() );
    }

}
