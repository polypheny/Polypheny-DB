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

import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import java.util.List;

public record PhysicalScan( ParquetRelTable table, int[] fields, List<ParquetAdapterFilter> filters ) {

    public PhysicalScan {
        filters = List.copyOf( filters );
    }


    /**
     * Support parametrized queries
     *
     * @param dataContext context
     * @return list of parquet filters
     */
    public List<ParquetAdapterFilter> resolveFilters( DataContext dataContext ) {
        return ParquetFilterResolver.resolveFilters( dataContext, filters, f -> selectPhysicalBinding( f.columnIndex() ) );
    }


    public ParquetColumnBinding selectPhysicalBinding( int columnIndex ) {
        if ( columnIndex < 0 || columnIndex >= table.columns.size() ) {
            throw new GenericRuntimeException( "Invalid physical filter column index: " + columnIndex );
        }
        return table.getBinding().getColumnBinding( table.columns.get( columnIndex ).id );
    }

}
