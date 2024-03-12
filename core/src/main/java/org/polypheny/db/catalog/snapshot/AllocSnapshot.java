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

package org.polypheny.db.catalog.snapshot;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPartitionGroup;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.partition.properties.PartitionProperty;

public interface AllocSnapshot {

    @NotNull List<AllocationColumn> getColumns();

    @NotNull
    Optional<List<AllocationEntity>> getEntitiesOnAdapter( long id );

    /**
     * Gets a collective list of field placements per field on an adapter.
     * Effectively used to retrieve all relevant placements including partitions.
     *
     * @param placementId The id of the adapter
     * @param columnId The id of the field
     * @return The specific field placement
     */
    @NotNull
    Optional<AllocationColumn> getColumn( long placementId, long columnId );

    /**
     * Get all column placements of a column
     *
     * @param columnId The id of the specific field
     * @return List of column placements of specific field
     */
    @NotNull
    Optional<List<AllocationColumn>> getColumnFromLogical( long columnId );

    /**
     * Get column placements of a specific entity on a specific adapter on field detail level.
     * Only returns one ColumnPlacement per field on adapter. Ignores multiplicity due to different partitionsIds
     *
     * @param adapterId The id of the adapter
     * @return List of column placements of the entity on the specified adapter
     */
    @NotNull
    List<AllocationColumn> getColumnPlacementsOnAdapterPerEntity( long adapterId, long entityId );


    /**
     * Gets all column placements of an entity structured by the id of the adapters.
     *
     * @param entityId The id of the entity for the requested field placements
     * @return The requested collection
     */
    @NotNull
    Map<Long, List<Long>> getColumnPlacementsByAdapters( long entityId );


    /**
     * Get a List of all partitions belonging to a specific entity
     *
     * @param partitionGroupId Entity to be queried
     * @return list of all partitions on this entity
     */
    List<AllocationPartition> getPartitions( long partitionGroupId );

    /**
     * Get a list of all partition name belonging to a specific entity
     *
     * @param entityId Entity to be queried
     * @return list of all partition names on this entity
     */
    List<String> getPartitionGroupNames( long entityId );

    /**
     * Get placements by partition. Identify the location of partitions.
     * Essentially returns all ColumnPlacements which hold the specified partitionID.
     *
     * @param entityId The id of the entity
     * @param partitionGroupId The id of the partition
     * @param columnId The id of tje column
     * @return List of allocation columns
     */
    List<AllocationColumn> getColumnAllocsByPartitionGroup( long entityId, long partitionGroupId, long columnId );

    /**
     * Get adapters by partition. Identify the location of partitions/replicas
     * Essentially returns all adapters which hold the specified partitionID
     *
     * @param entityId The unique id of the entity
     * @param partitionGroupId The unique id of the partition
     * @return List of adapters
     */
    List<LogicalAdapter> getAdaptersByPartitionGroup( long entityId, long partitionGroupId );

    /**
     * Get all partitions of a DataPlacement (identified by adapterId and entityId)
     *
     * @param adapterId The unique id of the adapter
     * @param entityId The unique id of the entity
     * @return List of partitionIds
     */
    List<Long> getPartitionsOnDataPlacement( long adapterId, long entityId );

    /**
     * Returns list with the index of the partitions on this store from  0..numPartitions
     *
     * @param adapterId The unique id of the adapter
     * @param entityId The unique id of the entity
     * @return List of partitionId indices
     */
    List<Long> getPartitionGroupsIndexOnDataPlacement( long adapterId, long entityId );

    /**
     * Returns a specific DataPlacement of a given entity.
     *
     * @param adapterId adapter where placement is located
     * @param logicalEntityId entity to retrieve the placement from
     * @return DataPlacement of an entity placed on a specific store
     */
    @NotNull
    Optional<AllocationPlacement> getPlacement( long adapterId, long logicalEntityId );

    @NotNull
    List<AllocationEntity> getFromLogical( long logicalId );

    @NotNull
    Optional<PartitionProperty> getPartitionProperty( long id );

    @NotNull
    List<AllocationColumn> getColumns( long placementId );

    @NotNull
    List<AllocationPartitionGroup> getPartitionGroupsFromLogical( long logicalId );

    @NotNull
    List<AllocationPartition> getPartitionsFromLogical( long logicalId );

    @NotNull
    List<AllocationPlacement> getPlacementsFromLogical( long logicalId );

    @NotNull
    Optional<AllocationEntity> getAlloc( long placementId, long partitionId );

    @NotNull
    List<AllocationEntity> getAllocsOfPlacement( long placementId );

    @NotNull
    List<AllocationPlacement> getPlacementsOfColumn( long logicalId );

    @NotNull
    Optional<AllocationPartition> getPartition( long partitionId );


    Optional<AllocationPartition> getPartitionFromName( long logicalId, String name );

    @NotNull
    List<AllocationEntity> getAllocations();

    @NotNull
    List<AllocationPlacement> getPlacements();

    @NotNull
    List<AllocationPartition> getPartitions();

    @NotNull
    List<AllocationPartition> getPartitionsFromGroup( long groupId );


    @NotNull
    List<AllocationEntity> getAllocsOfPartitions( long partitionId );

}
