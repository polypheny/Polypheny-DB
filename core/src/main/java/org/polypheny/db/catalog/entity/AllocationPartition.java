/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.catalog.entity;


import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import lombok.NonNull;
import lombok.Value;
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.PlacementType;


/**
 * This class is considered the logical representation of a physical table on a specific store.
 */
@Value
public class AllocationPartition implements CatalogObject {

    private static final long serialVersionUID = 8835793248417591036L;

    @Serialize
    public long namespaceId;
    @Serialize
    public long adapterId;
    @Serialize
    public long entityId;
    @Serialize
    public long partitionId;
    @Serialize
    public PlacementType placementType;

    // Related to multi-tier replication. A physical partition placement is considered to be primary (uptodate) if it needs to receive every update eagerly.
    // If false, physical partition placements are considered to be refreshable and can therefore become outdated and need to be lazily updated.
    // This attribute is derived from an effective data placement (table entity on a store)

    // Related to multi-tier replication. If Placement is considered a primary node it needs to receive every update eagerly.
    // If false, nodes are considered refreshable and can be lazily replicated
    // This attribute is derived from an effective data placement (table entity on a store)
    // This means that the store is not entirely considered a primary and can therefore be different on another table
    // Is present at the DataPlacement && the PartitionPlacement
    // Although, partitionPlacements are those that get effectively updated
    // A DataPlacement can directly forbid that any Placements within this DataPlacement container can get outdated.
    // Therefore, the role at the DataPlacement specifies if underlying placements can even be outdated.s
    @Serialize
    public DataPlacementRole role;


    public AllocationPartition(
            @Deserialize("namespaceId") long namespaceId,
            @Deserialize("entityId") final long entityId,
            @Deserialize("adapterId") final long adapterId,
            @Deserialize("placementType") @NonNull final PlacementType placementType,
            @Deserialize("partitionId") final long partitionId,
            @Deserialize("role") DataPlacementRole role ) {
        this.namespaceId = namespaceId;
        this.entityId = entityId;
        this.adapterId = adapterId;
        this.placementType = placementType;
        this.partitionId = partitionId;
        this.role = role;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }

}
