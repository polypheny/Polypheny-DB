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

public class JoinFiltersContainer extends FiltersContainer {

    public static JoinFiltersContainer empty = new JoinFiltersContainer( List.of(), List.of(), List.of(), List.of() );

    private final List<ParquetAdapterFilter> parentFilters;
    private final List<ParquetAdapterFilter> childFilters;


    /**
     * Creates a new instance of {@link FiltersContainer}
     *
     * @param parentFilters a list of filters to be used only on a parent table.
     * @param childFilters a list of filters to be used only on a child table.
     * @param adapterFilters a list of filters to be used on adapter level.
     * @param readerFilters a list of filters to be used on parquet reader level.
     */
    public JoinFiltersContainer( List<ParquetAdapterFilter> parentFilters, List<ParquetAdapterFilter> childFilters, List<ParquetAdapterFilter> adapterFilters, List<ParquetAdapterFilter> readerFilters ) {
        super( adapterFilters, readerFilters );
        this.parentFilters = parentFilters;
        this.childFilters = childFilters;
    }


    /**
     * Gets a list of parent table filters.
     *
     * @return a list of parent table filters.
     */
    public List<ParquetAdapterFilter> parentFilters() {
        return parentFilters;
    }


    /**
     * Gets a list of child table filters.
     *
     * @return a list of child table filters.
     */
    public List<ParquetAdapterFilter> childFilters() {
        return childFilters;
    }

}
