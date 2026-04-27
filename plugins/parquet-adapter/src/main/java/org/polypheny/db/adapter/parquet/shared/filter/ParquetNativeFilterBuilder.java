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

import java.util.List;
import java.util.Objects;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Builds native Parquet predicates from shared filter descriptions.
 */
public final class ParquetNativeFilterBuilder {

    private static final ParquetTypeConverter TYPE_CONVERTER = new ParquetTypeConverter();


    private ParquetNativeFilterBuilder() {
    }


    /**
     * Build parquet native filter predicates from adapter level filter
     * @param schema contains fields
     * @param filters adapter level filters
     * @return FilterCompat.Filter - parquet native
     */
    public static FilterCompat.Filter build( MessageType schema, List<ParquetAdapterFilter> filters ) {
        if ( filters == null || filters.isEmpty() ) {
            return FilterCompat.NOOP;
        }

        FilterPredicate predicate = null;

        for ( var filter : filters ) {
            FilterPredicate next = buildPredicate( schema, filter );
            if ( next == null ) {
                continue;
            }
            predicate = predicate == null ? next : FilterApi.and( predicate, next );
        }

        return predicate == null ? FilterCompat.NOOP : FilterCompat.get( predicate );
    }


    private static FilterPredicate buildPredicate( MessageType schema, ParquetAdapterFilter filter ) {
        if ( filter.isLogical() ) {
            return buildLogicalPredicate( schema, filter );
        }

        if ( filter.pathElements().isEmpty() ) {
            int index = filter.columnIndex();
            if ( index < 0 || index >= schema.getFieldCount() ) {
                return null;
            }

            Type type = schema.getType( index );
            if ( !type.isPrimitive() ) {
                return null;
            }

            String columnName = schema.getFieldName( index );
            return buildPredicatePrimitive( filter.operator(), filter.polyValue(), type, columnName );
        } else {
            // build native filter to push down for nested fields
            Type type = resolveType( schema, filter.pathElements() );
            if ( type == null || !type.isPrimitive() ) {
                return null;
            }

            if ( isRepeatedPath( schema, filter.pathElements() ) ) {
                return null;
            }

            String columnName = String.join( ".", filter.pathElements() );
            return buildPredicatePrimitive( filter.operator(), filter.polyValue(), type, columnName );
        }
    }


    private static FilterPredicate buildLogicalPredicate( MessageType schema, ParquetAdapterFilter filter ) {
        List<FilterPredicate> operands = filter.operands().stream()
                .map( operand -> buildPredicate( schema, operand ) )
                .toList();

        if ( operands.stream().anyMatch( Objects::isNull ) ) {
            return null;
        }

        return switch ( filter.operator() ) {
            case AND -> combineAnd( operands );
            case OR -> combineOr( operands );
            case NOT -> operands.size() == 1 ? FilterApi.not( operands.get( 0 ) ) : null;
            default -> null;
        };
    }


    private static FilterPredicate combineAnd( List<FilterPredicate> operands ) {
        FilterPredicate predicate = null;
        for ( FilterPredicate operand : operands ) {
            predicate = predicate == null ? operand : FilterApi.and( predicate, operand );
        }
        return predicate;
    }


    private static FilterPredicate combineOr( List<FilterPredicate> operands ) {
        FilterPredicate predicate = null;
        for ( FilterPredicate operand : operands ) {
            predicate = predicate == null ? operand : FilterApi.or( predicate, operand );
        }
        return predicate;
    }


    private static FilterPredicate buildPredicatePrimitive( Kind operator, PolyValue value, Type type, String columnName ) {
        PrimitiveType primitive = type.asPrimitiveType();
        Object expected = TYPE_CONVERTER.fromPolyValueToParquetObj( primitive, value );
        if ( expected == null ) {
            return null;
        }

        return switch ( primitive.getPrimitiveTypeName() ) {
            case BOOLEAN -> buildBoolean( operator, columnName, expected );
            case INT32 -> buildInt( operator, columnName, expected );
            case INT64 -> buildLong( operator, columnName, expected );
            case FLOAT -> buildFloat( operator, columnName, expected );
            case DOUBLE -> buildDouble( operator, columnName, expected );
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> buildBinary( operator, columnName, expected, primitive.getLogicalTypeAnnotation() );
        };
    }


    private static Type resolveType( GroupType groupType, List<String> path ) {
        GroupType current = groupType;
        for ( int i = 0; i < path.size(); i++ ) {
            Type type = null;
            for ( int fieldIndex = 0; fieldIndex < current.getFieldCount(); fieldIndex++ ) {
                Type candidate = current.getType( fieldIndex );
                if ( candidate.getName().equals( path.get( i ) ) ) {
                    type = candidate;
                    break;
                }
            }
            if ( type == null ) {
                return null;
            }
            if ( i == path.size() - 1 ) {
                return type;
            }
            if ( type.isPrimitive() ) {
                return null;
            }
            current = type.asGroupType();
        }
        return null;
    }


    private static boolean isRepeatedPath( MessageType schema, List<String> path ) {
        ColumnPath columnPath = ColumnPath.get( path.toArray( String[]::new ) );
        for ( ColumnDescriptor descriptor : schema.getColumns() ) {
            if ( ColumnPath.get( descriptor.getPath() ).equals( columnPath ) ) {
                return descriptor.getMaxRepetitionLevel() > 0;
            }
        }
        return true;
    }


    private static FilterPredicate buildBoolean( Kind operator, String columnName, Object expected ) {
        if ( !(expected instanceof Boolean value) ) {
            return null;
        }
        return switch ( operator ) {
            case EQUALS -> FilterApi.eq( FilterApi.booleanColumn( columnName ), value );
            case NOT_EQUALS -> FilterApi.notEq( FilterApi.booleanColumn( columnName ), value );
            default -> null;
        };
    }


    private static FilterPredicate buildInt( Kind operator, String columnName, Object expected ) {
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


    private static FilterPredicate buildLong( Kind operator, String columnName, Object expected ) {
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


    private static FilterPredicate buildFloat( Kind operator, String columnName, Object expected ) {
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


    private static FilterPredicate buildDouble( Kind operator, String columnName, Object expected ) {
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


    private static FilterPredicate buildBinary( Kind operator, String columnName, Object expected, LogicalTypeAnnotation logicalType ) {
        Binary value = expected instanceof Binary binary ? binary : Binary.fromString( expected.toString() );

        return switch ( operator ) {
            case EQUALS -> FilterApi.eq( FilterApi.binaryColumn( columnName ), value );
            case NOT_EQUALS -> FilterApi.notEq( FilterApi.binaryColumn( columnName ), value );
            case GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> logicalType == null ? null : buildOrderedBinary( operator, columnName, value );
            default -> null;
        };
    }


    private static FilterPredicate buildOrderedBinary( Kind operator, String columnName, Binary value ) {
        return switch ( operator ) {
            case GREATER_THAN -> FilterApi.gt( FilterApi.binaryColumn( columnName ), value );
            case GREATER_THAN_OR_EQUAL -> FilterApi.gtEq( FilterApi.binaryColumn( columnName ), value );
            case LESS_THAN -> FilterApi.lt( FilterApi.binaryColumn( columnName ), value );
            case LESS_THAN_OR_EQUAL -> FilterApi.ltEq( FilterApi.binaryColumn( columnName ), value );
            default -> null;
        };
    }

}
