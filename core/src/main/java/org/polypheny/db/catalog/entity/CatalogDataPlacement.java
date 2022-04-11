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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementState;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.ReplicationStrategy;


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
    public final PlacementState placementState;

    public final ReplicationStrategy replicationStrategy;

    public final ImmutableList<Long> columnPlacementsOnAdapter;

    // Serves as a pre-aggregation to apply filters more easily. In that case reads are more important
    // and frequent than writes
    public final ImmutableList<Long> partitionPlacementsOnAdapter;


    public CatalogDataPlacement(
            long tableId,
            int adapterId,
            PlacementType placementType,
            PlacementState placementState,
            ReplicationStrategy replicationStrategy,
            @NonNull final ImmutableList<Long> columnPlacementsOnAdapter,
            @NonNull final ImmutableList<Long> partitionPlacementsOnAdapter ) {
        this.tableId = tableId;
        this.adapterId = adapterId;
        this.placementType = placementType;
        this.placementState = placementState;
        this.replicationStrategy = replicationStrategy;
        this.columnPlacementsOnAdapter = ImmutableList.copyOf( columnPlacementsOnAdapter.stream().sorted().collect( Collectors.toList() ) );
        this.partitionPlacementsOnAdapter = ImmutableList.copyOf( partitionPlacementsOnAdapter.stream().sorted().collect( Collectors.toList() ) );

    }


    @SneakyThrows
    public String getLogicalTableName() {
        return Catalog.getInstance().getTable( tableId ).name;
    }


    @SneakyThrows
    public String getLogicalSchemaName() {
        return Catalog.getInstance().getTable( tableId ).getSchemaName();
    }


    @SneakyThrows
    public String getAdapterName() {
        return Catalog.getInstance().getAdapter( adapterId ).uniqueName;
    }


    @SneakyThrows
    public List<String> getLogicalColumnNames() {
        List<String> columnNames = new ArrayList<>();
        columnPlacementsOnAdapter.forEach( columnId -> columnNames.add( Catalog.getInstance().getColumn( columnId ).name ) );
        return columnNames;
    }


    public boolean hasFullPlacement() {
        return hasColumnFullPlacement() && hasPartitionFullPlacement();
    }


    public boolean hasColumnFullPlacement() {
        return Catalog.getInstance().getTable( this.tableId ).columnIds.size() == columnPlacementsOnAdapter.size();
    }


    public boolean hasPartitionFullPlacement() {
        return Catalog.getInstance().getTable( this.tableId ).partitionProperty.partitionIds.size() == getAllPartitionIds().size();
    }


    public List<Long> getAllPartitionIds() {
        return partitionPlacementsOnAdapter.stream().collect( Collectors.toList() );
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }

}
