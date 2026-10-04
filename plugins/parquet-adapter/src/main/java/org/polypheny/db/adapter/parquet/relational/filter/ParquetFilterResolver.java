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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;


public final class ParquetFilterResolver {

    private ParquetFilterResolver() {
    }


    /**
     * Support parametrized queries
     *
     * @param dataContext context
     * @param filters filters
     * @param selector a helper function for column binding selection
     * @return list of parquet filters
     */
    public static <T> List<ParquetAdapterFilter<T>> resolveFilters( DataContext dataContext, List<ParquetAdapterFilter<T>> filters, Function<ParquetAdapterFilter<T>, ParquetColumnBinding> selector ) {
        List<ParquetAdapterFilter<T>> resolved = new ArrayList<>( filters.size() );
        for ( ParquetAdapterFilter<T> filter : filters ) {
            resolved.add( resolveFilter( dataContext, filter, selector ) );
        }
        return resolved;
    }


    /**
     * Converts a filter with column index pointing to a projected fields to a filter pointing to a physical field index in parquet file.
     *
     * @param filter a filter to convert.
     * @param fields a projection.
     * @return converted filter with physical index.
     */
    public static <T> ParquetAdapterFilter<T> toPhysicalFilter( ParquetAdapterFilter<T> filter, int[] fields ) {
        if ( filter.isLogical() ) {
            List<ParquetAdapterFilter<T>> operands = new ArrayList<>( filter.operands().size() );
            for ( ParquetAdapterFilter<T> operand : filter.operands() ) {
                ParquetAdapterFilter<T> physicalOperand = toPhysicalFilter( operand, fields );
                if ( physicalOperand == null ) {
                    return null;
                }
                operands.add( physicalOperand );
            }
            return ParquetAdapterFilter.logical( filter.operator(), operands );
        }

        int physicalIndex = toPhysicalIndex( filter.columnIndex(), fields );
        if ( physicalIndex < 0 ) {
            return null;
        }

        return new ParquetAdapterFilter<>(
                physicalIndex,
                filter.pathElements(),
                filter.operator(),
                filter.value(),
                filter.dynamicParamIndex() );
    }


    /**
     * Converts a filter with column index pointing to a physical field index to a filter to a projected field index.
     *
     * @param filter a filter to convert.
     * @param projectionIndex an index conversion function.
     * @return a new converted filter.
     */
    public static <T> ParquetAdapterFilter<T> toProjectionFilter( ParquetAdapterFilter<T> filter, IntUnaryOperator projectionIndex ) {
        if ( filter.isLogical() ) {
            List<ParquetAdapterFilter<T>> operands = new ArrayList<>( filter.operands().size() );
            for ( ParquetAdapterFilter<T> operand : filter.operands() ) {
                ParquetAdapterFilter<T> projectionOperand = toProjectionFilter( operand, projectionIndex );
                if ( projectionOperand == null ) {
                    return null;
                }
                operands.add( projectionOperand );
            }
            return ParquetAdapterFilter.logical( filter.operator(), operands );
        }

        int index = projectionIndex.applyAsInt( filter.columnIndex() );
        if ( index < 0 ) {
            return null;
        }

        return new ParquetAdapterFilter<>(
                index,
                List.of(),
                filter.operator(),
                filter.value(),
                filter.dynamicParamIndex() );
    }


    @SuppressWarnings("unchecked")
    private static <T> ParquetAdapterFilter<T> resolveFilter( DataContext dataContext, ParquetAdapterFilter<T> filter, Function<ParquetAdapterFilter<T>, ParquetColumnBinding> selector ) {
        if ( filter.isLogical() ) {
            return ParquetAdapterFilter.logical( filter.operator(), filter.operands().stream()
                    .map( operand -> resolveFilter( dataContext, operand, selector ) )
                    .toList() );
        }

        T value = filter.dynamicParamIndex() == null
                ? filter.value()
                : (T) dataContext.getParameterValue( filter.dynamicParamIndex() );

        ParquetColumnBinding columnBinding = Objects.requireNonNull( selector.apply( filter ), "Missing parquet column binding" );
        return new ParquetAdapterFilter<>( filter.columnIndex(), columnBinding.sourcePathElements(), filter.operator(), value );
    }


    private static int toPhysicalIndex( int index, int[] fields ) {
        if ( index < 0 || index >= fields.length ) {
            return -1;
        }
        return fields[index];
    }


}
