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

import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyValue;
import java.util.List;

/**
 * Immutable filter description
 *
 * @param columnIndex - index of filter column
 * @param operator - filter operation
 * @param polyValue - filter value
 */
public record ParquetAdapterFilter( int columnIndex, List<String> pathElements, Kind operator, PolyValue polyValue, Long dynamicParamIndex ) {

    public ParquetAdapterFilter( int columnIndex, Kind operator, PolyValue polyValue ) {
        this( columnIndex, List.of(), operator, polyValue, null );
    }


    public ParquetAdapterFilter( int columnIndex, List<String> pathElements, Kind operator, PolyValue polyValue ) {
        this( columnIndex, pathElements, operator, polyValue, null );
    }

    // validation performed in main constructor
    public ParquetAdapterFilter {
        pathElements = pathElements == null ? List.of() : List.copyOf( pathElements );
    }


    /**
     * Creates new {@link ParquetAdapterFilter} that contain sub path starting from the provided index.
     *
     * @param startIndex the index to copy path elements from.
     * @return a new instance of {@link ParquetAdapterFilter}.
     */
    public ParquetAdapterFilter makeNested( int startIndex ) {
        if ( startIndex >= pathElements.size() ) {
            return this;
        }
        return new ParquetAdapterFilter( columnIndex, pathElements.subList( startIndex, pathElements.size() ), operator, polyValue, dynamicParamIndex );
    }

}
