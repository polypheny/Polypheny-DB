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

package org.polypheny.db.catalog.entity.allocation;


import com.google.common.collect.ImmutableList;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import java.io.Serial;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Value;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.entity.PolyObject;
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.type.entity.PolyValue;


/**
 * This class is considered the logical representation of a physical table on a specific store.
 */
@Value
public class AllocationPartition implements PolyObject {

    @Serial
    private static final long serialVersionUID = 8835793248417591036L;

    @Serialize
    public long namespaceId;

    @Serialize
    public long logicalEntityId;

    @Serialize
    public long groupId;

    @Getter
    @Serialize
    @SerializeNullable
    public String name;

    @Serialize
    public long id;

    @Serialize
    public PlacementType placementType;

    @Serialize
    public PartitionType partitionType;

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
    @Serialize
    public boolean isUnbound;
    @Serialize
    @NotNull
    public List<String> qualifiers;


    public AllocationPartition(
            @Deserialize("id") final long id,
            @Deserialize("namespaceId") long namespaceId,
            @Deserialize("logicalEntityId") final long logicalEntityId,
            @Deserialize("groupId") final long groupId,
            @Deserialize("placementType") @NotNull final PlacementType placementType,
            @Deserialize("name") @Nullable final String name,
            @Deserialize("role") DataPlacementRole role,
            @Deserialize("isUnbound") final boolean isUnbound,
            @Deserialize("qualifiers") @Nullable final List<String> qualifiers,
            @Deserialize("partitionType") final PartitionType partitionType ) {
        this.namespaceId = namespaceId;
        this.logicalEntityId = logicalEntityId;
        this.placementType = placementType;
        this.partitionType = partitionType;
        this.groupId = groupId;
        this.id = id;
        this.role = role;
        this.isUnbound = isUnbound;
        this.name = name;
        this.qualifiers = qualifiers == null ? List.of() : ImmutableList.copyOf( qualifiers );
    }


    @Override
    public PolyValue[] getParameterArray() {
        return new PolyValue[0];
    }

}
