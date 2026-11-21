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

package org.polypheny.db.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.snapshot.AllocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.routing.ColumnDistribution.FullPartition;
import org.polypheny.db.routing.ColumnDistribution.ShrinkingMergeStrategy;

class JoinedScanCacheKeyTest {

    @Test
    void preservesRoutingOrderWhenTheLegacyHashCollides() {
        Snapshot snapshot = snapshot();
        ShrinkingMergeStrategy strategy = new ShrinkingMergeStrategy();
        List<Long> source = new ArrayList<>( List.of( 2L, 3L ) );
        ColumnDistribution forward = new ColumnDistribution( 1, List.of( 4L ), List.of( 2L, 3L ), source, List.of(), snapshot, strategy );
        Collections.reverse( source );
        ColumnDistribution reverse = new ColumnDistribution( 1, List.of( 4L ), List.of( 2L, 3L ), source, List.of(), snapshot, strategy );

        assertEquals( forward.hashCode(), reverse.hashCode() );
        assertEquals( List.of( 2L, 3L ), forward.route().partitions().stream().map( FullPartition::id ).toList() );
        assertEquals( List.of( 3L, 2L ), reverse.route().partitions().stream().map( FullPartition::id ).toList() );
        assertNotEquals( forward.cacheKey( snapshot ).orElseThrow(), reverse.cacheKey( snapshot ).orElseThrow() );

        ColumnDistribution equivalent = new ColumnDistribution( 1, List.of( 4L ), List.of( 2L, 3L ), List.of( 2L, 3L ), List.of(), snapshot );
        assertEquals( forward.cacheKey( snapshot ).orElseThrow(), equivalent.cacheKey( snapshot ).orElseThrow() );
    }


    @Test
    void bypassesCacheForStaleCatalogOrCustomMergeStrategy() {
        Snapshot snapshot = snapshot();
        ColumnDistribution distribution = new ColumnDistribution( 1, List.of( 4L ), List.of( 2L, 3L ), List.of( 2L, 3L ), List.of(), snapshot );

        assertTrue( distribution.cacheKey( snapshot() ).isEmpty() );
        assertTrue( distribution.withMergeStrategy( new ShrinkingMergeStrategy() {
        } ).cacheKey( snapshot ).isEmpty() );
    }


    private static Snapshot snapshot() {
        Snapshot snapshot = mock( Snapshot.class );
        LogicalRelSnapshot relational = mock( LogicalRelSnapshot.class );
        AllocSnapshot allocations = mock( AllocSnapshot.class );
        when( snapshot.rel() ).thenReturn( relational );
        when( snapshot.alloc() ).thenReturn( allocations );
        when( relational.getTable( 1 ) ).thenReturn( Optional.of( new LogicalTable( 1, "partitioned", 0, EntityType.ENTITY, null, true ) ) );
        when( allocations.getPlacements() ).thenReturn( List.of( new AllocationPlacement( 5, 1, 0, 6 ) ) );
        when( allocations.getPartitions() ).thenReturn( List.of( partition( 2 ), partition( 3 ) ) );
        when( allocations.getAllocations() ).thenReturn( List.of( new AllocationTable( 7, 5, 2, 1, 0, 6 ), new AllocationTable( 8, 5, 3, 1, 0, 6 ) ) );
        when( allocations.getColumns() ).thenReturn( List.of( new AllocationColumn( 0, 5, 1, 4, PlacementType.AUTOMATIC, 0, 6 ) ) );
        return snapshot;
    }


    private static AllocationPartition partition( long id ) {
        return new AllocationPartition( id, 0, 1, 0, PlacementType.AUTOMATIC, null, DataPlacementRole.UP_TO_DATE, false, List.of(), PartitionType.HASH );
    }

}
