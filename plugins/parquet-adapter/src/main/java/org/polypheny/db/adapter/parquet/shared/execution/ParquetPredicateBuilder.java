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

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.polypheny.db.algebra.constant.Kind;

/**
 * Creates native Parquet filter predicates
 * for the supported comparison operators and value types
 */
public class ParquetPredicateBuilder {
    public static FilterPredicate buildBoolean( Kind operator, String columnName, Object expected ) {
        if ( !(expected instanceof Boolean value) ) {
            return null;
        }
        return switch ( operator ) {
            case EQUALS -> FilterApi.eq( FilterApi.booleanColumn( columnName ), value );
            case NOT_EQUALS -> FilterApi.notEq( FilterApi.booleanColumn( columnName ), value );
            default -> null;
        };
    }


    public static FilterPredicate buildInt( Kind operator, String columnName, Object expected ) {
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


    public static FilterPredicate buildLong( Kind operator, String columnName, Object expected ) {
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


    public static FilterPredicate buildFloat( Kind operator, String columnName, Object expected ) {
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


    public static FilterPredicate buildDouble( Kind operator, String columnName, Object expected ) {
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


    public static FilterPredicate buildBinary( Kind operator, String columnName, Object expected, LogicalTypeAnnotation logicalType ) {
        Binary value = expected instanceof Binary binary ? binary : Binary.fromString( expected.toString() );

        return switch ( operator ) {
            case EQUALS -> FilterApi.eq( FilterApi.binaryColumn( columnName ), value );
            case NOT_EQUALS -> FilterApi.notEq( FilterApi.binaryColumn( columnName ), value );
            case GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> logicalType == null ? null : buildOrderedBinary( operator, columnName, value );
            default -> null;
        };
    }


    public static FilterPredicate buildOrderedBinary( Kind operator, String columnName, Binary value ) {
        return switch ( operator ) {
            case GREATER_THAN -> FilterApi.gt( FilterApi.binaryColumn( columnName ), value );
            case GREATER_THAN_OR_EQUAL -> FilterApi.gtEq( FilterApi.binaryColumn( columnName ), value );
            case LESS_THAN -> FilterApi.lt( FilterApi.binaryColumn( columnName ), value );
            case LESS_THAN_OR_EQUAL -> FilterApi.ltEq( FilterApi.binaryColumn( columnName ), value );
            default -> null;
        };
    }
}
