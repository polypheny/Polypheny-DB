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


import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.DataPlacementRole;
import org.polypheny.db.catalog.Catalog.PlacementType;


/**
 * Serves as a container, which holds all information related to a table entity placed on physical store.
 */
public class CatalogDataPlacement implements CatalogEntity {

    private static final long serialVersionUID = 3758054726464326557L;
    public final long tableId;
    public final int adapterId;

    public final PlacementType placementType;

    // Is present at the DataPlacement && the PartitionPlacement
    // Although, partitionPlacements are those that get effectively updated
    // A DataPlacement can directly forbid that any Placements within this DataPlacement container can get outdated.
    // Therefore, the role at the DataPlacement specifies if underlying placements can even be outdated.
    public final DataPlacementRole dataPlacementRole;

    public final ImmutableList<Long> columnPlacementsOnAdapter;
    public final ImmutableList<Long> partitionPlacementsOnAdapter;


    public CatalogDataPlacement(
            long tableId,
            int adapterId,
            PlacementType placementType,
            DataPlacementRole dataPlacementRole,
            @NonNull final ImmutableList<Long> columnPlacementsOnAdapter,
            @NonNull final ImmutableList<Long> partitionPlacementsOnAdapter ) {
        this.tableId = tableId;
        this.adapterId = adapterId;
        this.placementType = placementType;
        this.dataPlacementRole = dataPlacementRole;
        this.columnPlacementsOnAdapter = columnPlacementsOnAdapter;
        this.partitionPlacementsOnAdapter = partitionPlacementsOnAdapter;
    }


    @SneakyThrows
    public String getTableName() {
        return Catalog.getInstance().getTable( tableId ).name;
    }


    @SneakyThrows
    public String getAdapterName() {
        return Catalog.getInstance().getAdapter( adapterId ).uniqueName;
    }


    public boolean hasFullPlacement() {
        if ( hasColumnFullPlacement() && hasPartitionFullPlacement() ) {
            return true;
        }
        return false;
    }


    public boolean hasColumnFullPlacement() {
        return Catalog.getInstance().getTable( this.tableId ).columnIds.size() == columnPlacementsOnAdapter.size();
    }


    public boolean hasPartitionFullPlacement() {
        return Catalog.getInstance().getTable( this.tableId ).partitionProperty.partitionIds.size() == partitionPlacementsOnAdapter.size();
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }

}
