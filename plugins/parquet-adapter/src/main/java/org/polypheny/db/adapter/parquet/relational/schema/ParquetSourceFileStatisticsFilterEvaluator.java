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

package org.polypheny.db.adapter.parquet.relational.schema;

import java.util.function.Function;
import org.polypheny.db.adapter.parquet.shared.filter.FilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.algebra.constant.Kind;


/**
 * Uses Parquet footer min/max/null-count statistics to reject files that cannot
 * contain rows matching a filter.
 */
class ParquetSourceFileStatisticsFilterEvaluator extends FilterEvaluator<ParquetSourceFile> {

    private final Function<ParquetAdapterFilter, ParquetColumnBinding> selector;
    private final ParquetTypeConverter typeConverter;


    ParquetSourceFileStatisticsFilterEvaluator( Function<ParquetAdapterFilter, ParquetColumnBinding> selector ) {
        this.selector = selector;
        this.typeConverter = new ParquetTypeConverter();
    }


    @Override
    protected Boolean evaluateLeaf( ParquetSourceFile sourceFile, ParquetAdapterFilter filter ) {
        ParquetColumnBinding columnBinding = selector.apply( filter );
        if ( columnBinding == null || columnBinding.role() != ParquetColumnRole.DATA || columnBinding.sourcePathElements().isEmpty() ) {
            return null;
        }

        ParquetColumnStatistics statistics = sourceFile.columnStatistics().get( columnBinding.sourcePathElements() );
        if ( statistics == null ) {
            return null;
        }

        if ( filter.operator() == Kind.IS_NULL ) {
            return statistics.hasNoNulls() ? Boolean.FALSE : null;
        }
        if ( filter.operator() == Kind.IS_NOT_NULL ) {
            return statistics.hasOnlyNulls() ? Boolean.FALSE : null;
        }
        if ( filter.polyValue() == null || filter.polyValue().isNull() ) {
            return null;
        }
        if ( statistics.hasOnlyNulls() ) {
            return Boolean.FALSE;
        }
        if ( !statistics.hasRange() ) {
            return null;
        }
        Comparable<?> value = typeConverter.fromPolyValueToComparable( filter.polyValue(), statistics.type() );
        Comparable<?> min = typeConverter.fromStringToComparable( statistics.type(), statistics.min() );
        Comparable<?> max = typeConverter.fromStringToComparable( statistics.type(), statistics.max() );
        if ( value == null || min == null || max == null ) {
            return null;
        }

        return switch ( filter.operator() ) {
            case EQUALS -> outsideRange( value, min, max ) ? Boolean.FALSE : null;
            case NOT_EQUALS -> singleValueRangeEquals( value, min, max ) ? Boolean.FALSE : null;
            case GREATER_THAN -> typeConverter.compare( max, value ) <= 0 ? Boolean.FALSE : null;
            case GREATER_THAN_OR_EQUAL -> typeConverter.compare( max, value ) < 0 ? Boolean.FALSE : null;
            case LESS_THAN -> typeConverter.compare( min, value ) >= 0 ? Boolean.FALSE : null;
            case LESS_THAN_OR_EQUAL -> typeConverter.compare( min, value ) > 0 ? Boolean.FALSE : null;
            default -> null;
        };
    }


    private boolean outsideRange( Comparable<?> value, Comparable<?> min, Comparable<?> max ) {
        return typeConverter.compare( value, min ) < 0 || typeConverter.compare( value, max ) > 0;
    }


    private boolean singleValueRangeEquals( Comparable<?> value, Comparable<?> min, Comparable<?> max ) {
        return typeConverter.compare( min, max ) == 0 && typeConverter.compare( min, value ) == 0;
    }

}
