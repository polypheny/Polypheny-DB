/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.catalog.snapshot.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.catalogs.AllocationCatalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogCollectionMapping;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPartitionGroup;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.AllocSnapshot;
import org.polypheny.db.partition.properties.PartitionProperty;

public class AllocSnapshotImpl implements AllocSnapshot {

    public AllocSnapshotImpl( Map<Long, AllocationCatalog> allocationCatalogs ) {
    }


    @Override
    public List<AllocationEntity<?>> getAllocationsOnAdapter( long id ) {
        return null;
    }


    @Override
    public AllocationEntity<?> getAllocEntity( long id ) {
        return null;
    }


    @Override
    public CatalogColumnPlacement getColumnPlacement( long adapterId, long columnId ) {
        return null;
    }


    @Override
    public boolean checkIfExistsColumnPlacement( long adapterId, long columnId ) {
        return false;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacements( long columnId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterPerTable( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapter( long adapterId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsByColumn( long columnId ) {
        return null;
    }


    @Override
    public ImmutableMap<Long, ImmutableList<Long>> getColumnPlacementsByAdapter( long tableId ) {
        return null;
    }


    @Override
    public long getPartitionGroupByPartition( long partitionId ) {
        return 0;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterAndSchema( long adapterId, long schemaId ) {
        return null;
    }


    @Override
    public CatalogPartitionGroup getPartitionGroup( long partitionGroupId ) {
        return null;
    }


    @Override
    public CatalogPartition getPartition( long partitionId ) {
        return null;
    }


    @Override
    public List<CatalogPartition> getPartitionsByTable( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionGroup> getPartitionGroups( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionGroup> getPartitionGroups( Pattern schemaNamePattern, Pattern tableNamePattern ) {
        return null;
    }


    @Override
    public List<CatalogPartition> getPartitions( long partitionGroupId ) {
        return null;
    }


    @Override
    public List<CatalogPartition> getPartitions( Pattern schemaNamePattern, Pattern tableNamePattern ) {
        return null;
    }


    @Override
    public List<String> getPartitionGroupNames( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsByPartitionGroup( long tableId, long partitionGroupId, long columnId ) {
        return null;
    }


    @Override
    public List<CatalogAdapter> getAdaptersByPartitionGroup( long tableId, long partitionGroupId ) {
        return null;
    }


    @Override
    public List<Long> getPartitionGroupsOnDataPlacement( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<Long> getPartitionsOnDataPlacement( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<Long> getPartitionGroupsIndexOnDataPlacement( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public CatalogDataPlacement getDataPlacement( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getDataPlacements( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getAllFullDataPlacements( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getAllColumnFullDataPlacements( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getAllPartitionFullDataPlacements( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getDataPlacementsByRole( long tableId, DataPlacementRole role ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByRole( long tableId, DataPlacementRole role ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByIdAndRole( long tableId, long partitionId, DataPlacementRole role ) {
        return null;
    }


    @Override
    public CatalogPartitionPlacement getPartitionPlacement( long adapterId, long partitionId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByAdapter( long adapterId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByTableOnAdapter( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getAllPartitionPlacementsByTable( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacements( long partitionId ) {
        return null;
    }


    @Override
    public boolean isHorizontalPartitioned( long id ) {
        return false;
    }


    @Override
    public boolean isVerticalPartitioned( long id ) {
        return false;
    }


    @Override
    public boolean checkIfExistsPartitionPlacement( long adapterId, long partitionId ) {
        return false;
    }


    @Override
    public List<AllocationTable> getAllocationsFromLogical( long logicalId ) {
        return null;
    }


    @Override
    public boolean isPartitioned( long id ) {
        return false;
    }


    @Override
    public CatalogGraphPlacement getGraphPlacement( long graphId, long adapterId ) {
        return null;
    }


    @Override
    public List<CatalogGraphPlacement> getGraphPlacements( long adapterId ) {
        return null;
    }


    @Override
    public CatalogCollectionPlacement getCollectionPlacement( long id, long placementId ) {
        return null;
    }


    @Override
    public CatalogCollectionMapping getCollectionMapping( long id ) {
        return null;
    }


    @Override
    public List<CatalogCollectionPlacement> getCollectionPlacementsByAdapter( long id ) {
        return null;
    }


    @Override
    public List<CatalogCollectionPlacement> getCollectionPlacements( long collectionId ) {
        return null;
    }


    @Override
    public PartitionProperty getPartitionProperty( long id ) {
        return null;
    }

}
