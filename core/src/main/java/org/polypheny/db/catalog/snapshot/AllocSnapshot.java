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

package org.polypheny.db.catalog.snapshot;

import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.entity.AllocationColumn;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.partition.properties.PartitionProperty;

public interface AllocSnapshot {


    List<AllocationEntity> getAllocationsOnAdapter( long id );

    AllocationEntity getAllocation( long id );

    /**
     * Gets a collective list of column placements per column on an adapter.
     * Effectively used to retrieve all relevant placements including partitions.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @return The specific column placement
     */
    AllocationColumn getColumn( long adapterId, long columnId );

    /**
     * Get all column placements of a column
     *
     * @param columnId The id of the specific column
     * @return List of column placements of specific column
     */
    List<AllocationColumn> getColumnFromLogical( long columnId );

    /**
     * Get column placements of a specific table on a specific adapter on column detail level.
     * Only returns one ColumnPlacement per column on adapter. Ignores multiplicity due to different partitionsIds
     *
     * @param adapterId The id of the adapter
     * @return List of column placements of the table on the specified adapter
     */
    List<AllocationColumn> getColumnPlacementsOnAdapterPerTable( long adapterId, long tableId );

    /**
     * Get column placements on a adapter. On column detail level
     * Only returns one ColumnPlacement per column on adapter. Ignores multiplicity due to different partitionsIds
     *
     * @param adapterId The id of the adapter
     * @return List of column placements on the specified adapter
     */
    List<AllocationColumn> getColumnPlacementsOnAdapter( long adapterId );

    /**
     * Gets all column placements of a table structured by the id of the adapters.
     *
     * @param tableId The id of the table for the requested column placements
     * @return The requested collection
     */
    Map<Long, List<Long>> getColumnPlacementsByAdapter( long tableId );


    /**
     * Get a List of all partitions belonging to a specific table
     *
     * @param partitionGroupId Table to be queried
     * @return list of all partitions on this table
     */
    List<CatalogPartition> getPartitions( long partitionGroupId );

    /**
     * Get a list of all partition name belonging to a specific table
     *
     * @param tableId Table to be queried
     * @return list of all partition names on this table
     */
    List<String> getPartitionGroupNames( long tableId );

    /**
     * Get placements by partition. Identify the location of partitions.
     * Essentially returns all ColumnPlacements which hold the specified partitionID.
     *
     * @param tableId The id of the table
     * @param partitionGroupId The id of the partition
     * @param columnId The id of tje column
     * @return List of CatalogColumnPlacements
     */
    List<AllocationColumn> getColumnPlacementsByPartitionGroup( long tableId, long partitionGroupId, long columnId );

    /**
     * Get adapters by partition. Identify the location of partitions/replicas
     * Essentially returns all adapters which hold the specified partitionID
     *
     * @param tableId The unique id of the table
     * @param partitionGroupId The unique id of the partition
     * @return List of CatalogAdapters
     */
    List<CatalogAdapter> getAdaptersByPartitionGroup( long tableId, long partitionGroupId );

    /**
     * Get all partitions of a DataPlacement (identified by adapterId and tableId)
     *
     * @param adapterId The unique id of the adapter
     * @param tableId The unique id of the table
     * @return List of partitionIds
     */
    List<Long> getPartitionGroupsOnDataPlacement( long adapterId, long tableId );

    /**
     * Get all partitions of a DataPlacement (identified by adapterId and tableId)
     *
     * @param adapterId The unique id of the adapter
     * @param tableId The unique id of the table
     * @return List of partitionIds
     */
    List<Long> getPartitionsOnDataPlacement( long adapterId, long tableId );

    /**
     * Returns list with the index of the partitions on this store from  0..numPartitions
     *
     * @param adapterId The unique id of the adapter
     * @param tableId The unique id of the table
     * @return List of partitionId Indices
     */
    List<Long> getPartitionGroupsIndexOnDataPlacement( long adapterId, long tableId );

    /**
     * Returns a specific DataPlacement of a given table.
     *
     * @param adapterId adapter where placement is located
     * @param tableId table to retrieve the placement from
     * @return DataPlacement of a table placed on a specific store
     */
    CatalogDataPlacement getDataPlacement( long adapterId, long tableId );

    /**
     * Returns all DataPlacements of a given table.
     *
     * @param tableId table to retrieve the placements from
     * @return List of all DataPlacements for the table
     */
    List<CatalogDataPlacement> getDataPlacements( long tableId );


    /**
     * Returns a specific partition entity which is placed on a store.
     *
     * @param adapterId The adapter on which the requested partition placements reside
     * @param partitionId The id of the requested partition
     * @return The requested PartitionPlacement on that store for a given is
     */
    CatalogPartitionPlacement getPartitionPlacement( long adapterId, long partitionId );

    /**
     * Returns a list of all Partition Placements which currently reside on an adapter, for a specific table.
     *
     * @param adapterId The adapter on which the requested partition placements reside
     * @param tableId The table for which all partition placements on an adapter should be considered
     * @return A list of all Partition Placements, that are currently located  on that specific store for an individual table
     */
    List<CatalogPartitionPlacement> getPartitionPlacementsByTableOnAdapter( long adapterId, long tableId );

    /**
     * Returns a list of all Partition Placements which are currently associated with a table.
     *
     * @param tableId The table on which the requested partition placements are currently associated with.
     * @return A list of all Partition Placements, that belong to the desired table
     */
    List<CatalogPartitionPlacement> getAllPartitionPlacementsByTable( long tableId );



    List<AllocationEntity> getFromLogical( long logicalId );

    boolean isPartitioned( long id );



    /**
     * Gets a collection of graph placements for a given adapter.
     *
     * @param adapterId The id of the adapter on which the placements are placed
     * @return The collection of graph placements
     */
    List<CatalogGraphPlacement> getGraphPlacements( long adapterId );

    List<CatalogCollectionPlacement> getCollectionPlacementsByAdapter( long id );

    List<CatalogCollectionPlacement> getCollectionPlacements( long collectionId );

    PartitionProperty getPartitionProperty( long id );

    boolean adapterHasPlacement( long adapterId, long logicalId );

    AllocationEntity getAllocation( long adapterId, long entityId );

    List<AllocationColumn> getColumns( long allocId );


}
