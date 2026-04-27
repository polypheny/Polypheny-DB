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
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Immutable filter description
 *
 * @param columnIndex - index of filter column
 * @param operator - filter operation
 * @param polyValue - filter value
 */
public record ParquetAdapterFilter( int columnIndex, List<String> pathElements, Kind operator, PolyValue polyValue, Long dynamicParamIndex, List<ParquetAdapterFilter> operands ) {

    public ParquetAdapterFilter( int columnIndex, Kind operator, PolyValue polyValue ) {
        this( columnIndex, List.of(), operator, polyValue, null, List.of() );
    }


    public ParquetAdapterFilter( int columnIndex, List<String> pathElements, Kind operator, PolyValue polyValue ) {
        this( columnIndex, pathElements, operator, polyValue, null, List.of() );
    }


    public ParquetAdapterFilter( int columnIndex, List<String> pathElements, Kind operator, PolyValue polyValue, Long dynamicParamIndex ) {
        this( columnIndex, pathElements, operator, polyValue, dynamicParamIndex, List.of() );
    }


    // validation performed in main constructor
    public ParquetAdapterFilter {
        pathElements = pathElements == null ? List.of() : List.copyOf( pathElements );
        operands = operands == null ? List.of() : List.copyOf( operands );
    }


    public static ParquetAdapterFilter logical( Kind operator, List<ParquetAdapterFilter> operands ) {
        return new ParquetAdapterFilter( -1, List.of(), operator, null, null, operands );
    }


    public boolean isLogical() {
        return operator == Kind.AND || operator == Kind.OR || operator == Kind.NOT;
    }

}
