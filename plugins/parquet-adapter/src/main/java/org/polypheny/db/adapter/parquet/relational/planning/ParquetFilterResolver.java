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

package org.polypheny.db.adapter.parquet.relational.planning;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.type.entity.PolyValue;


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
    public static List<ParquetAdapterFilter> resolveFilters( DataContext dataContext, List<ParquetAdapterFilter> filters, Function<ParquetAdapterFilter, ParquetColumnBinding> selector ) {
        List<ParquetAdapterFilter> resolved = new ArrayList<>( filters.size() );
        for ( ParquetAdapterFilter filter : filters ) {
            resolved.add( resolveFilter( dataContext, filter, selector ) );
        }
        return resolved;
    }


    public static ParquetAdapterFilter toPhysicalFilter( ParquetAdapterFilter filter, int[] fields ) {
        if ( filter.isLogical() ) {
            List<ParquetAdapterFilter> operands = new ArrayList<>( filter.operands().size() );
            for ( ParquetAdapterFilter operand : filter.operands() ) {
                ParquetAdapterFilter physicalOperand = toPhysicalFilter( operand, fields );
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

        return new ParquetAdapterFilter(
                physicalIndex,
                filter.pathElements(),
                filter.operator(),
                filter.polyValue(),
                filter.dynamicParamIndex() );
    }


    private static ParquetAdapterFilter resolveFilter( DataContext dataContext, ParquetAdapterFilter filter, Function<ParquetAdapterFilter, ParquetColumnBinding> selector ) {
        if ( filter.isLogical() ) {
            return ParquetAdapterFilter.logical( filter.operator(), filter.operands().stream()
                    .map( operand -> resolveFilter( dataContext, operand, selector ) )
                    .toList() );
        }

        PolyValue value = filter.dynamicParamIndex() == null
                ? filter.polyValue()
                : dataContext.getParameterValue( filter.dynamicParamIndex() );

        ParquetColumnBinding columnBinding = Objects.requireNonNull( selector.apply( filter ), "Missing parquet column binding" );
        return new ParquetAdapterFilter( filter.columnIndex(), columnBinding.sourcePathElements(), filter.operator(), value );
    }


    private static int toPhysicalIndex( int index, int[] fields ) {
        if ( index < 0 || index >= fields.length ) {
            return -1;
        }
        return fields[index];
    }

}
