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

/**
 * A container for filters split by usage.
 */
public class FiltersContainer {

    public static FiltersContainer empty = new FiltersContainer( List.of(), List.of() );

    private final List<ParquetAdapterFilter> adapterFilters;
    private final List<ParquetAdapterFilter> nativeFilters;


    /**
     * Creates a new instance of {@link FiltersContainer}
     *
     * @param adapterFilters a list of filters to be used on adapter level.
     * @param nativeFilters a list of filters to be used on parquet reader level.
     */
    public FiltersContainer( List<ParquetAdapterFilter> adapterFilters, List<ParquetAdapterFilter> nativeFilters ) {
        this.adapterFilters = adapterFilters == null ? List.of() : List.copyOf( adapterFilters );
        this.nativeFilters = nativeFilters == null ? List.of() : List.copyOf( nativeFilters );
    }


    /**
     * Creates a {@link FiltersContainer} with the provided filters as both adapter and reader filters.
     *
     * @param filters filters to share between adapter and reader.
     * @return a new instance of {@link FiltersContainer}
     */
    public static FiltersContainer shared( List<ParquetAdapterFilter> filters ) {
        return new FiltersContainer( filters, filters );
    }


    /**
     * Keeps the same filters, but removes path elements from adapter-level filters.
     * This is useful for simple projected scans where row validation must use projected column indexes,
     * while reader filters still keep paths for native Parquet pushdown.
     *
     * @return a new instance of {@link FiltersContainer}
     */
    public FiltersContainer withoutPathElementsInAdapterFilters() {
        return new FiltersContainer(
                adapterFilters.stream().map( FiltersContainer::withoutPathElements ).toList(),
                nativeFilters );
    }


    private static ParquetAdapterFilter withoutPathElements( ParquetAdapterFilter filter ) {
        if ( filter.isLogical() ) {
            return ParquetAdapterFilter.logical(
                    filter.operator(),
                    filter.operands().stream().map( FiltersContainer::withoutPathElements ).toList() );
        }
        return new ParquetAdapterFilter( filter.columnIndex(), List.of(), filter.operator(), filter.polyValue(), filter.dynamicParamIndex() );
    }


    /**
     * Gets a list of adapter filters.
     *
     * @return a list of adapter filters.
     */
    public List<ParquetAdapterFilter> adapterFilters() {
        return adapterFilters;
    }


    /**
     * Gets a list of parquet reader filters.
     *
     * @return a list of reader filters.
     */
    public List<ParquetAdapterFilter> nativeFilters() {
        return nativeFilters;
    }

}
