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

package org.polypheny.db.adapter.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.constant.Kind;


class FiltersContainerTest {

    @Test
    void removesOnlyAdapterFilterPaths() {
        ParquetAdapterFilter leaf = new ParquetAdapterFilter( 2, List.of( "fare_amount" ), Kind.GREATER_THAN, null, 7L );
        ParquetAdapterFilter logical = ParquetAdapterFilter.logical( Kind.AND, List.of( leaf ) );
        FiltersContainer container = new FiltersContainer( List.of( logical ), List.of( leaf ) );

        FiltersContainer converted = container.withoutPathElementsInAdapterFilters();

        ParquetAdapterFilter convertedLeaf = converted.adapterFilters().get( 0 ).operands().get( 0 );
        assertEquals( 2, convertedLeaf.columnIndex() );
        assertEquals( List.of(), convertedLeaf.pathElements() );
        assertEquals( 7L, convertedLeaf.dynamicParamIndex() );
        assertEquals( List.of( "fare_amount" ), converted.nativeFilters().get( 0 ).pathElements() );
    }

}
