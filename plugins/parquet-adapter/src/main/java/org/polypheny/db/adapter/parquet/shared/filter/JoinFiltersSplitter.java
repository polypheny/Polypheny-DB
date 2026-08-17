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

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyValue;

public class JoinFiltersSplitter {

    private static FilterKind classifyFilter( ParquetAdapterFilter<PolyValue> filter, boolean leftIsParent, int parentFieldCount, int childFieldCount ) {
        if ( filter.isLogical() ) {
            List<FilterKind> operandSides = filter.operands().stream()
                    .map( operand -> classifyFilter( operand, leftIsParent, parentFieldCount, childFieldCount ) )
                    .distinct()
                    .toList();
            return operandSides.size() == 1 ? operandSides.get( 0 ) : FilterKind.ADAPTER;
        }

        if ( leftIsParent ) {
            if ( filter.columnIndex() >= 0 && filter.columnIndex() < parentFieldCount ) {
                return FilterKind.PARENT;
            }
            if ( filter.columnIndex() >= parentFieldCount && filter.columnIndex() < parentFieldCount + childFieldCount ) {
                return FilterKind.CHILD;
            }
            return FilterKind.ADAPTER;
        }

        if ( filter.columnIndex() >= 0 && filter.columnIndex() < childFieldCount ) {
            return FilterKind.CHILD;
        }
        if ( filter.columnIndex() >= childFieldCount && filter.columnIndex() < childFieldCount + parentFieldCount ) {
            return FilterKind.PARENT;
        }
        return FilterKind.ADAPTER;
    }


    private static boolean canUseAsReaderFilter( ParquetAdapterFilter<PolyValue> filter ) {
        if ( filter.isLogical() ) {
            return !filter.operands().isEmpty() && filter.operands().stream().allMatch( JoinFiltersSplitter::canUseAsReaderFilter );
        }
        return !filter.pathElements().isEmpty();
    }


    public JoinFiltersContainer split( List<ParquetAdapterFilter<PolyValue>> filters, boolean leftIsParent, int parentFieldCount, int childFieldCount ) {
        List<ParquetAdapterFilter<PolyValue>> parentFilters = new ArrayList<>();
        List<ParquetAdapterFilter<PolyValue>> childFilters = new ArrayList<>();
        List<ParquetAdapterFilter<PolyValue>> adapterFilters = new ArrayList<>();
        List<ParquetAdapterFilter<PolyValue>> readerFilters = new ArrayList<>();

        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            splitFilter( filter, leftIsParent, parentFieldCount, childFieldCount, parentFilters, childFilters, adapterFilters, readerFilters );
        }
        return new JoinFiltersContainer( parentFilters, childFilters, adapterFilters, readerFilters );
    }


    private void splitFilter( ParquetAdapterFilter<PolyValue> filter, boolean leftIsParent, int parentFieldCount, int childFieldCount, List<ParquetAdapterFilter<PolyValue>> parentFilters, List<ParquetAdapterFilter<PolyValue>> childFilters, List<ParquetAdapterFilter<PolyValue>> adapterFilters, List<ParquetAdapterFilter<PolyValue>> readerFilters ) {
        if ( filter.isLogical() && filter.operator() == Kind.AND ) {
            filter.operands().forEach( operand -> splitFilter( operand, leftIsParent, parentFieldCount, childFieldCount, parentFilters, childFilters, adapterFilters, readerFilters ) );
            return;
        }

        switch ( classifyFilter( filter, leftIsParent, parentFieldCount, childFieldCount ) ) {
            case PARENT -> {
                if ( canUseAsReaderFilter( filter ) ) {
                    readerFilters.add( filter );
                }
                parentFilters.add( filter );
            }
            case CHILD -> childFilters.add( filter );
            case ADAPTER -> adapterFilters.add( filter );
        }
    }


    private enum FilterKind {
        PARENT,
        CHILD,
        ADAPTER
    }

}
