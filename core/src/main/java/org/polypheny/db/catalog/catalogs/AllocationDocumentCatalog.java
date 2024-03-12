/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.catalog.catalogs;

import java.util.Map;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.logistic.PartitionType;

public interface AllocationDocumentCatalog extends AllocationCatalog {

    AllocationCollection addAllocation( LogicalCollection collection, long placementId, long partitionId, long adapterId );

    void removeAllocation( long id );

    AllocationPlacement addPlacement( LogicalCollection collection, long adapterId );

    void removePlacement( long placementId );


    AllocationPartition addPartition( LogicalCollection collection, PartitionType partitionType, String name );

    void removePartition( long partitionId );

    Map<Long, ? extends AllocationCollection> getCollections();

}
