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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.parquet.shared.filter.JoinFiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.JoinFiltersSplitter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.constant.Kind;


class JoinFiltersSplitterTest {

    @Test
    void keepsReaderParentFilterForAdapterValidation() {
        ParquetAdapterFilter parentFilter = new ParquetAdapterFilter( 0, List.of( "amount" ), Kind.GREATER_THAN, null );

        JoinFiltersContainer split = new JoinFiltersSplitter().split( List.of( parentFilter ), true, 2, 1 );

        assertEquals( 1, split.nativeFilters().size() );
        assertEquals( 1, split.parentFilters().size() );
        assertSame( parentFilter, split.nativeFilters().get( 0 ) );
        assertSame( parentFilter, split.parentFilters().get( 0 ) );
        assertTrue( split.childFilters().isEmpty() );
        assertTrue( split.adapterFilters().isEmpty() );
    }


    @Test
    void keepsCrossSideOrAsJoinedRowFilter() {
        ParquetAdapterFilter parentFilter = new ParquetAdapterFilter( 0, List.of( "amount" ), Kind.GREATER_THAN, null );
        ParquetAdapterFilter childFilter = new ParquetAdapterFilter( 2, List.of( "items", "price" ), Kind.GREATER_THAN, null );
        ParquetAdapterFilter orFilter = ParquetAdapterFilter.logical( Kind.OR, List.of( parentFilter, childFilter ) );

        JoinFiltersContainer split = new JoinFiltersSplitter().split( List.of( orFilter ), true, 2, 1 );

        assertEquals( 1, split.adapterFilters().size() );
        assertSame( orFilter, split.adapterFilters().get( 0 ) );
        assertTrue( split.nativeFilters().isEmpty() );
        assertTrue( split.parentFilters().isEmpty() );
        assertTrue( split.childFilters().isEmpty() );
    }

}
