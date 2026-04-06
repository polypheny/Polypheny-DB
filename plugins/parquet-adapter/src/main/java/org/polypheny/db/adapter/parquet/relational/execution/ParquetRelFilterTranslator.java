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
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.execution.ParquetFilterTranslationSupport;
import org.polypheny.db.adapter.parquet.shared.execution.ParquetPredicateBuilder;
import org.polypheny.db.adapter.parquet.shared.model.AdapterFilter;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;

/**
 * Translates adapter filters into parquet-native predicates.
 */
public class ParquetRelFilterTranslator {

    private final ParquetTypeConverter typeConverter = new ParquetTypeConverter();


    /**
     * Translates a Rex filter into adapter filter form when possible.
     */
    public AdapterFilter translate( List<PolyType> fieldTypes, RexNode polyFilter ) {
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

        return ParquetFilterTranslationSupport.toAdapterFilter( index, polyFilter.getKind(), right );
    }


    /**
     * Translate adapter filters to Parquet filter for pushdown
     *
     * @param schema - parquet file native schema
     * @param adapterFilters - adapter level filters
     * @return FilterCompat.Filter - filter in parquet format
     */
    public FilterCompat.Filter translate( MessageType schema, List<AdapterFilter> adapterFilters ) {
        FilterPredicate predicate = null;

        for ( AdapterFilter filter : adapterFilters ) {
            FilterPredicate next = buildParquetFilterPredicate( schema, filter );
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
     *
     * @param schema - parquet schema
     * @param adapterFilter - adapter level filter info
     * @return FilterPredicate object
     */
    private FilterPredicate buildParquetFilterPredicate( MessageType schema, AdapterFilter adapterFilter ) {
        int index = adapterFilter.columnIndex();
        if ( index < 0 || index >= schema.getFieldCount() ) {
            return null;
        }

        Type type = schema.getType( index );
        if ( !type.isPrimitive() ) {
            return null;
        }

        PrimitiveType primitive = type.asPrimitiveType();
        Object expected = typeConverter.fromPolyValueToParquetObj( primitive, adapterFilter.polyValue() );
        if ( expected == null ) {
            return null;
        }

        String columnName = schema.getFieldName( index );
        return switch ( primitive.getPrimitiveTypeName() ) {
            case BOOLEAN -> ParquetPredicateBuilder.buildBoolean( adapterFilter.operator(), columnName, expected );
            case INT32 -> ParquetPredicateBuilder.buildInt( adapterFilter.operator(), columnName, expected );
            case INT64 -> ParquetPredicateBuilder.buildLong( adapterFilter.operator(), columnName, expected );
            case FLOAT -> ParquetPredicateBuilder.buildFloat( adapterFilter.operator(), columnName, expected );
            case DOUBLE -> ParquetPredicateBuilder.buildDouble( adapterFilter.operator(), columnName, expected );
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> ParquetPredicateBuilder.buildBinary( adapterFilter.operator(), columnName, expected, primitive.getLogicalTypeAnnotation() );
        };
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
