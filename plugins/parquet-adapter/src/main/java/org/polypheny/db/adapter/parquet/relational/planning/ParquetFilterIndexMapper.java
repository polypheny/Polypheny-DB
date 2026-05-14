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
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;


final class ParquetFilterIndexMapper {

    private ParquetFilterIndexMapper() {
    }


    static ParquetAdapterFilter remapFilter( ParquetAdapterFilter filter, int[] oldFields, int[] newFields ) {
        if ( filter.isLogical() ) {
            List<ParquetAdapterFilter> remappedOperands = new ArrayList<>( filter.operands().size() );
            for ( ParquetAdapterFilter operand : filter.operands() ) {
                ParquetAdapterFilter remappedOperand = remapFilter( operand, oldFields, newFields );
                if ( remappedOperand == null ) {
                    return null;
                }
                remappedOperands.add( remappedOperand );
            }
            return ParquetAdapterFilter.logical( filter.operator(), remappedOperands );
        }

        int remappedIndex = remapIndex( filter.columnIndex(), oldFields, newFields );
        if ( remappedIndex < 0 ) {
            return null;
        }

        return new ParquetAdapterFilter(
                remappedIndex,
                filter.pathElements(),
                filter.operator(),
                filter.polyValue(),
                filter.dynamicParamIndex() );
    }


    static List<ParquetAdapterFilter> remapFilters( List<ParquetAdapterFilter> filters, int[] oldFields, int[] newFields ) {
        List<ParquetAdapterFilter> remappedFilters = new ArrayList<>( filters.size() );
        for ( ParquetAdapterFilter filter : filters ) {
            ParquetAdapterFilter remappedFilter = remapFilter( filter, oldFields, newFields );
            if ( remappedFilter == null ) {
                return null;
            }
            remappedFilters.add( remappedFilter );
        }
        return remappedFilters;
    }


    private static int remapIndex( int oldIndex, int[] oldFields, int[] newFields ) {
        if ( oldIndex < 0 || oldIndex >= oldFields.length ) {
            return -1;
        }

        int physicalColumn = oldFields[oldIndex];
        for ( int i = 0; i < newFields.length; i++ ) {
            if ( newFields[i] == physicalColumn ) {
                return i;
            }
        }
        return -1;
    }

}
