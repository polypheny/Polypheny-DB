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
import java.sql.Timestamp;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;


/**
 * Serves as a container, which holds all information related to a table entity placed on physical store.
 */
public class CatalogDataPlacement implements CatalogEntity {

    private static final long serialVersionUID = 3758054726464326557L;
    public final long tableId;
    public final int adapterId;

    public final PlacementType placementType;


    public final ImmutableList<Long> columnPlacementsOnAdapter;
    public final ImmutableList<Long> partitionPlacementsOnAdapter;


    // The newest commit timestamp when any partitions inside this placement has been updated or refreshed
    // Equals the newest timestamp ony any of the CatalogPartitionPlacements.
    // Technically other  linked attachments could still have older update timestamps.
    // This should help to quickly identify placements that can fulfil certain conditions.
    // Without having to traverse all partition placements one-by-one
    @Setter
    public Timestamp updateTimestamp;


    public CatalogDataPlacement(
            long tableId,
            int adapterId,
            PlacementType placementType,
            @NonNull final ImmutableList columnPlacementsOnAdapter,
            @NonNull final ImmutableList partitionPlacementsOnAdapter ) {

        this.tableId = tableId;
        this.adapterId = adapterId;
        this.placementType = placementType;
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


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }
}
