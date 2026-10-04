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

package org.polypheny.db.adapter.parquet.relational.filter;

import java.util.function.Function;

import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnStatistics;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetConstantColumnResolver;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetFilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Uses Parquet footer min/max/null-count statistics to evaluate filters at file
 * level. A {@code false} result rejects a file that cannot contain matching
 * rows. A {@code true} result proves that every row in the file matches.
 */
public class ParquetSourceFileStatisticsFilterEvaluator extends ParquetFilterEvaluator<ParquetSourceFile, PolyValue> {

    private final Function<ParquetAdapterFilter<PolyValue>, ParquetColumnBinding> selector;
    private final ParquetTypeConverter typeConverter;
    private final ParquetConstantColumnResolver constantColumnResolver;


    public ParquetSourceFileStatisticsFilterEvaluator( Function<ParquetAdapterFilter<PolyValue>, ParquetColumnBinding> selector ) {
        this.selector = selector;
        this.typeConverter = new ParquetTypeConverter();
        this.constantColumnResolver = new ParquetConstantColumnResolver();
    }


    /**
     * Checks whether this evaluator can decide the filter from file metadata for
     * any runtime filter value. This is intentionally stricter than pruning:
     * metadata aggregates may include a file only when every row matches.
     *
     * @param sourceFile a source parquet file.
     * @param filter a non-logical filter.
     * @return {@code true} when footer statistics are sufficient for an exact decision.
     */
    public boolean supportsExactEvaluation( ParquetSourceFile sourceFile, ParquetAdapterFilter<PolyValue> filter ) {
        if ( filter.isLogical() ) {
            return false;
        }
        ParquetColumnBinding columnBinding = selector.apply( filter );
        if ( columnBinding == null || columnBinding.role() != ParquetColumnRole.DATA || columnBinding.sourcePathElements().size() != 1 ) {
            return false;
        }
        ParquetColumnStatistics statistics = statistics( sourceFile, filter );
        if ( statistics == null || statistics.valueCount() != statistics.rowCount() ) {
            return false;
        }
        if ( filter.operator() == Kind.IS_NULL || filter.operator() == Kind.IS_NOT_NULL ) {
            return statistics.hasNoNulls() || statistics.hasOnlyNulls();
        }
        if ( !isComparison( filter.operator() ) ) {
            return false;
        }
        if ( statistics.hasOnlyNulls() ) {
            return true;
        }
        return constantColumnResolver.resolve( statistics ).isPresent();
    }


    @Override
    protected Boolean evaluateLeaf( ParquetSourceFile sourceFile, ParquetAdapterFilter<PolyValue> filter ) {
        ParquetColumnStatistics statistics = statistics( sourceFile, filter );
        if ( statistics == null ) {
            return null;
        }

        if ( filter.operator() == Kind.IS_NULL ) {
            return statistics.hasOnlyNulls() ? Boolean.TRUE : statistics.hasNoNulls() ? Boolean.FALSE : null;
        }
        if ( filter.operator() == Kind.IS_NOT_NULL ) {
            return statistics.hasNoNulls() ? Boolean.TRUE : statistics.hasOnlyNulls() ? Boolean.FALSE : null;
        }
        if ( !isComparison( filter.operator() ) ) {
            return null;
        }
        if ( filter.value() == null || filter.value().isNull() ) {
            return Boolean.FALSE;
        }
        if ( statistics.hasOnlyNulls() ) {
            return Boolean.FALSE;
        }
        if ( !statistics.hasRange() ) {
            return null;
        }
        Comparable<?> value = typeConverter.fromPolyValueToComparable( filter.value(), statistics.type() );
        Comparable<?> min = typeConverter.fromStringToComparable( statistics.type(), statistics.min() );
        Comparable<?> max = typeConverter.fromStringToComparable( statistics.type(), statistics.max() );
        if ( value == null || min == null || max == null ) {
            return null;
        }

        return switch ( filter.operator() ) {
            case EQUALS -> outsideRange( value, min, max )
                    ? Boolean.FALSE
                    : statistics.hasNoNulls() && singleValueRangeEquals( value, min, max ) ? Boolean.TRUE : null;
            case NOT_EQUALS -> singleValueRangeEquals( value, min, max )
                    ? Boolean.FALSE
                    : statistics.hasNoNulls() && outsideRange( value, min, max ) ? Boolean.TRUE : null;
            case GREATER_THAN -> typeConverter.compare( max, value ) <= 0
                    ? Boolean.FALSE
                    : statistics.hasNoNulls() && typeConverter.compare( min, value ) > 0 ? Boolean.TRUE : null;
            case GREATER_THAN_OR_EQUAL -> typeConverter.compare( max, value ) < 0
                    ? Boolean.FALSE
                    : statistics.hasNoNulls() && typeConverter.compare( min, value ) >= 0 ? Boolean.TRUE : null;
            case LESS_THAN -> typeConverter.compare( min, value ) >= 0
                    ? Boolean.FALSE
                    : statistics.hasNoNulls() && typeConverter.compare( max, value ) < 0 ? Boolean.TRUE : null;
            case LESS_THAN_OR_EQUAL -> typeConverter.compare( min, value ) > 0
                    ? Boolean.FALSE
                    : statistics.hasNoNulls() && typeConverter.compare( max, value ) <= 0 ? Boolean.TRUE : null;
            default -> null;
        };
    }


    private ParquetColumnStatistics statistics( ParquetSourceFile sourceFile, ParquetAdapterFilter<PolyValue> filter ) {
        ParquetColumnBinding columnBinding = selector.apply( filter );
        if ( columnBinding == null || columnBinding.role() != ParquetColumnRole.DATA || columnBinding.sourcePathElements().isEmpty() ) {
            return null;
        }
        return sourceFile.columnStatistics().get( columnBinding.sourcePathElements() );
    }


    private boolean isComparison( Kind operator ) {
        return operator == Kind.EQUALS
                || operator == Kind.NOT_EQUALS
                || operator == Kind.GREATER_THAN
                || operator == Kind.GREATER_THAN_OR_EQUAL
                || operator == Kind.LESS_THAN
                || operator == Kind.LESS_THAN_OR_EQUAL;
    }


    private boolean outsideRange( Comparable<?> value, Comparable<?> min, Comparable<?> max ) {
        return typeConverter.compare( value, min ) < 0 || typeConverter.compare( value, max ) > 0;
    }


    private boolean singleValueRangeEquals( Comparable<?> value, Comparable<?> min, Comparable<?> max ) {
        return typeConverter.compare( min, max ) == 0 && typeConverter.compare( min, value ) == 0;
    }

}
