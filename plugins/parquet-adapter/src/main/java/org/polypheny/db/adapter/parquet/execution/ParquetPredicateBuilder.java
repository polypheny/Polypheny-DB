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

package org.polypheny.db.adapter.parquet.execution;

import java.util.List;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.model.FilterInfo;
import org.polypheny.db.adapter.parquet.schema.ParquetTypeConverter;
import org.polypheny.db.algebra.constant.Kind;

/**
 * Translates adapter filters into parquet-native predicates.
 */
public class ParquetPredicateBuilder {

    private final ParquetTypeConverter typeConverter = new ParquetTypeConverter();


    /**
     * Build parquet filter for pushdown from adapter filter
     * @param schema - parquet file native schema
     * @param filters - adapter level filters
     * @return FilterCompat.Filter - filter in parquet format
     */
    public FilterCompat.Filter translate( MessageType schema, List<FilterInfo> filters ) {
        FilterPredicate predicate = null;

        for ( FilterInfo filter : filters ) {
            FilterPredicate next = buildPredicate( schema, filter );
            if ( next == null ) {
                throw new IllegalArgumentException( "Unsupported parquet predicate: " + filter );
            }
            predicate = predicate == null ? next : FilterApi.and( predicate, next );
        }

        // return filter for given predicate
        return predicate == null ? FilterCompat.NOOP : FilterCompat.get( predicate );
    }


    /**
     * Build parquet filter predicate from provided filter info
     * @param schema - parquet schema
     * @param filter - adapter level filter info
     * @return FilterPredicate object
     */
    private FilterPredicate buildPredicate( MessageType schema, FilterInfo filter ) {
        int index = filter.columnIndex();
        if ( index < 0 || index >= schema.getFieldCount() ) {
            return null;
        }

        Type type = schema.getType( index );
        if ( !type.isPrimitive() ) {
            return null;
        }

        PrimitiveType primitive = type.asPrimitiveType();
        Object expected = typeConverter.fromPolyValueToParquetObj( primitive, filter.polyValue() );
        if ( expected == null ) {
            return null;
        }

        String columnName = schema.getFieldName( index );
        return switch ( primitive.getPrimitiveTypeName() ) {
            case BOOLEAN -> buildBooleanPredicate( filter.operator(), columnName, expected );
            case INT32 -> buildIntPredicate( filter.operator(), columnName, expected );
            case INT64 -> buildLongPredicate( filter.operator(), columnName, expected );
            case FLOAT -> buildFloatPredicate( filter.operator(), columnName, expected );
            case DOUBLE -> buildDoublePredicate( filter.operator(), columnName, expected );
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> buildBinaryPredicate( filter.operator(), columnName, expected, primitive.getLogicalTypeAnnotation() );
        };
    }


    private FilterPredicate buildBooleanPredicate( Kind operator, String columnName, Object expected ) {
        if ( !(expected instanceof Boolean value) ) {
            return null;
        }
        return switch ( operator ) {
            case EQUALS -> FilterApi.eq( FilterApi.booleanColumn( columnName ), value );
            case NOT_EQUALS -> FilterApi.notEq( FilterApi.booleanColumn( columnName ), value );
            default -> null;
        };
    }


    private FilterPredicate buildIntPredicate( Kind operator, String columnName, Object expected ) {
        if ( !(expected instanceof Integer value) ) {
            return null;
        }
        return switch ( operator ) {
            case EQUALS -> FilterApi.eq( FilterApi.intColumn( columnName ), value );
            case NOT_EQUALS -> FilterApi.notEq( FilterApi.intColumn( columnName ), value );
            case GREATER_THAN -> FilterApi.gt( FilterApi.intColumn( columnName ), value );
            case GREATER_THAN_OR_EQUAL -> FilterApi.gtEq( FilterApi.intColumn( columnName ), value );
            case LESS_THAN -> FilterApi.lt( FilterApi.intColumn( columnName ), value );
            case LESS_THAN_OR_EQUAL -> FilterApi.ltEq( FilterApi.intColumn( columnName ), value );
            default -> null;
        };
    }


    private FilterPredicate buildLongPredicate( Kind operator, String columnName, Object expected ) {
        if ( !(expected instanceof Long value) ) {
            return null;
        }
        return switch ( operator ) {
            case EQUALS -> FilterApi.eq( FilterApi.longColumn( columnName ), value );
            case NOT_EQUALS -> FilterApi.notEq( FilterApi.longColumn( columnName ), value );
            case GREATER_THAN -> FilterApi.gt( FilterApi.longColumn( columnName ), value );
            case GREATER_THAN_OR_EQUAL -> FilterApi.gtEq( FilterApi.longColumn( columnName ), value );
            case LESS_THAN -> FilterApi.lt( FilterApi.longColumn( columnName ), value );
            case LESS_THAN_OR_EQUAL -> FilterApi.ltEq( FilterApi.longColumn( columnName ), value );
            default -> null;
        };
    }


    private FilterPredicate buildFloatPredicate( Kind operator, String columnName, Object expected ) {
        if ( !(expected instanceof Float value) ) {
            return null;
        }
        return switch ( operator ) {
            case EQUALS -> FilterApi.eq( FilterApi.floatColumn( columnName ), value );
            case NOT_EQUALS -> FilterApi.notEq( FilterApi.floatColumn( columnName ), value );
            case GREATER_THAN -> FilterApi.gt( FilterApi.floatColumn( columnName ), value );
            case GREATER_THAN_OR_EQUAL -> FilterApi.gtEq( FilterApi.floatColumn( columnName ), value );
            case LESS_THAN -> FilterApi.lt( FilterApi.floatColumn( columnName ), value );
            case LESS_THAN_OR_EQUAL -> FilterApi.ltEq( FilterApi.floatColumn( columnName ), value );
            default -> null;
        };
    }


    private FilterPredicate buildDoublePredicate( Kind operator, String columnName, Object expected ) {
        if ( !(expected instanceof Double value) ) {
            return null;
        }
        return switch ( operator ) {
            case EQUALS -> FilterApi.eq( FilterApi.doubleColumn( columnName ), value );
            case NOT_EQUALS -> FilterApi.notEq( FilterApi.doubleColumn( columnName ), value );
            case GREATER_THAN -> FilterApi.gt( FilterApi.doubleColumn( columnName ), value );
            case GREATER_THAN_OR_EQUAL -> FilterApi.gtEq( FilterApi.doubleColumn( columnName ), value );
            case LESS_THAN -> FilterApi.lt( FilterApi.doubleColumn( columnName ), value );
            case LESS_THAN_OR_EQUAL -> FilterApi.ltEq( FilterApi.doubleColumn( columnName ), value );
            default -> null;
        };
    }


    private FilterPredicate buildBinaryPredicate( Kind operator, String columnName, Object expected, LogicalTypeAnnotation logicalType ) {
        Binary value = expected instanceof Binary binary
                ? binary
                : Binary.fromString( expected.toString() );

        return switch ( operator ) {
            case EQUALS -> FilterApi.eq( FilterApi.binaryColumn( columnName ), value );
            case NOT_EQUALS -> FilterApi.notEq( FilterApi.binaryColumn( columnName ), value );
            case GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> logicalType == null ? null : buildOrderedBinaryPredicate( operator, columnName, value );
            default -> null;
        };
    }


    private FilterPredicate buildOrderedBinaryPredicate( Kind operator, String columnName, Binary value ) {
        return switch ( operator ) {
            case GREATER_THAN -> FilterApi.gt( FilterApi.binaryColumn( columnName ), value );
            case GREATER_THAN_OR_EQUAL -> FilterApi.gtEq( FilterApi.binaryColumn( columnName ), value );
            case LESS_THAN -> FilterApi.lt( FilterApi.binaryColumn( columnName ), value );
            case LESS_THAN_OR_EQUAL -> FilterApi.ltEq( FilterApi.binaryColumn( columnName ), value );
            default -> null;
        };
    }

}
