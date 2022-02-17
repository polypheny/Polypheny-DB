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


import java.io.Serializable;
import lombok.NonNull;
import org.polypheny.db.catalog.Catalog.DataPlacementRole;
import org.polypheny.db.catalog.Catalog.PlacementType;


/**
 * This class is considered the logical representation of a physical table on a specific store.
 */
public class CatalogPartitionPlacement implements CatalogEntity {

    private static final long serialVersionUID = 3035193464866141590L;

    public final long tableId;
    public final long partitionId;
    public final int adapterId;
    public final String adapterUniqueName;
    public final PlacementType placementType;

    public final String physicalSchemaName;
    public final String physicalTableName;


    // Related to multi-tier replication. A physical partition placement is considered to be primary (uptodate) if it needs to receive every update eagerly.
    // If false, physical partition placements are considered to be refreshable and can therefore become outdated and need to be lazily updated.
    // This attribute is derived from an effective data placement (table entity on a store)
    // TODO @HENNLO remove default role
    public final DataPlacementRole role = DataPlacementRole.UPTODATE;


    public CatalogPartitionPlacement(
            final long tableId,
            final int adapterId,
            @NonNull final String adapterUniqueName,
            @NonNull final PlacementType placementType,
            final String physicalSchemaName,
            final String physicalTableName,
            final long partitionId ) {
        this.tableId = tableId;
        this.adapterId = adapterId;
        this.adapterUniqueName = adapterUniqueName;
        this.placementType = placementType;
        this.physicalSchemaName = physicalSchemaName;
        this.physicalTableName = physicalTableName;
        this.partitionId = partitionId;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }

}
