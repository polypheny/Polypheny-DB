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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
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
import org.polypheny.db.partition.properties.PartitionProperty;

public interface AllocSnapshot {

    //// ALLOCATION ENTITIES

    // AllocationTable getAllocTable( long id );

    // AllocationCollection getAllocCollection( long id );

    // AllocationGraph getAllocGraph( long id );

    List<AllocationEntity> getAllocationsOnAdapter( long id );

    AllocationEntity getAllocEntity( long id );

    /**
     * Gets a collective list of column placements per column on an adapter.
     * Effectively used to retrieve all relevant placements including partitions.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @return The specific column placement
     */
    CatalogColumnPlacement getColumnPlacement( long adapterId, long columnId );

    /**
     * Checks if there is a column with the specified name in the specified table.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @return true if there is a column placement, false if not.
     */
    boolean checkIfExistsColumnPlacement( long adapterId, long columnId );

    /**
     * Get all column placements of a column
     *
     * @param columnId The id of the specific column
     * @return List of column placements of specific column
     */
    List<CatalogColumnPlacement> getColumnPlacements( long columnId );

    /**
     * Get column placements of a specific table on a specific adapter on column detail level.
     * Only returns one ColumnPlacement per column on adapter. Ignores multiplicity due to different partitionsIds
     *
     * @param adapterId The id of the adapter
     * @return List of column placements of the table on the specified adapter
     */
    List<CatalogColumnPlacement> getColumnPlacementsOnAdapterPerTable( long adapterId, long tableId );

    /**
     * Get column placements on a adapter. On column detail level
     * Only returns one ColumnPlacement per column on adapter. Ignores multiplicity due to different partitionsIds
     *
     * @param adapterId The id of the adapter
     * @return List of column placements on the specified adapter
     */
    List<CatalogColumnPlacement> getColumnPlacementsOnAdapter( long adapterId );

    /**
     * Gets a collection of column placements for a given column.
     *
     * @param columnId The id of the column of requested column placements
     * @return The collection of placements sorted
     */
    List<CatalogColumnPlacement> getColumnPlacementsByColumn( long columnId );

    /**
     * Gets all column placements of a table structured by the id of the adapters.
     *
     * @param tableId The id of the table for the requested column placements
     * @return The requested collection
     */
    ImmutableMap<Long, ImmutableList<Long>> getColumnPlacementsByAdapter( long tableId );

    /**
     * Gets the partition group sorted by partition.
     *
     * @param partitionId The id of the partitions group
     */
    long getPartitionGroupByPartition( long partitionId );


    /**
     * Get column placements in a specific schema on a specific adapter
     *
     * @param adapterId The id of the adapter
     * @param schemaId The id of the schema
     * @return List of column placements on this adapter and schema
     */
    List<CatalogColumnPlacement> getColumnPlacementsOnAdapterAndSchema( long adapterId, long schemaId );

    /**
     * Get a partition object by its unique id
     *
     * @param partitionGroupId The unique id of the partition
     * @return A catalog partitionGroup
     */
    CatalogPartitionGroup getPartitionGroup( long partitionGroupId );

    /**
     * Get a partition object by its unique id
     *
     * @param partitionId The unique id of the partition
     * @return A catalog partition
     */
    CatalogPartition getPartition( long partitionId );

    /**
     * Retrieves a list of partitions which are associated with a specific table
     *
     * @param tableId Table for which partitions shall be gathered
     * @return List of all partitions associated with that table
     */
    List<CatalogPartition> getPartitionsByTable( long tableId );

    /**
     * Get a List of all partitions belonging to a specific table
     *
     * @param tableId Table to be queried
     * @return list of all partitions on this table
     */
    List<CatalogPartitionGroup> getPartitionGroups( long tableId );

    /**
     * Get all partitions of the specified database which fit to the specified filter patterns.
     * <code>getColumns(xid, databaseName, null, null, null)</code> returns all partitions of the database.
     *
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns catalog/src/test/java/org/polypheny/db/test/CatalogTest.javaall.
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    List<CatalogPartitionGroup> getPartitionGroups( Pattern schemaNamePattern, Pattern tableNamePattern );


    /**
     * Get a List of all partitions belonging to a specific table
     *
     * @param partitionGroupId Table to be queried
     * @return list of all partitions on this table
     */
    List<CatalogPartition> getPartitions( long partitionGroupId );

    /**
     * Get all partitions of the specified database which fit to the specified filter patterns.
     * <code>getColumns(xid, databaseName, null, null, null)</code> returns all partitions of the database.
     *
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns catalog/src/test/java/org/polypheny/db/test/CatalogTest.javaall.
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    List<CatalogPartition> getPartitions( Pattern schemaNamePattern, Pattern tableNamePattern );

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
    List<CatalogColumnPlacement> getColumnPlacementsByPartitionGroup( long tableId, long partitionGroupId, long columnId );

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
     * Returns a list of all DataPlacements that contain all columns as well as all partitions
     *
     * @param tableId table to retrieve the list from
     * @return list of all full DataPlacements
     */
    List<CatalogDataPlacement> getAllFullDataPlacements( long tableId );

    /**
     * Returns a list of all DataPlacements that contain all columns
     *
     * @param tableId table to retrieve the list from
     * @return list of all full DataPlacements
     */
    List<CatalogDataPlacement> getAllColumnFullDataPlacements( long tableId );

    /**
     * Returns a list of all DataPlacements that contain all partitions
     *
     * @param tableId table to retrieve the list from
     * @return list of all full DataPlacements
     */
    List<CatalogDataPlacement> getAllPartitionFullDataPlacements( long tableId );

    /**
     * Returns all DataPlacements of a given table that are associated with a given role.
     *
     * @param tableId table to retrieve the placements from
     * @param role role to specifically filter
     * @return List of all DataPlacements for the table that are associated with a specific role
     */
    List<CatalogDataPlacement> getDataPlacementsByRole( long tableId, DataPlacementRole role );

    /**
     * Returns all PartitionPlacements of a given table that are associated with a given role.
     *
     * @param tableId table to retrieve the placements from
     * @param role role to specifically filter
     * @return List of all PartitionPlacements for the table that are associated with a specific role
     */
    List<CatalogPartitionPlacement> getPartitionPlacementsByRole( long tableId, DataPlacementRole role );

    /**
     * Returns all PartitionPlacements of a given table with a given ID that are associated with a given role.
     *
     * @param tableId table to retrieve the placements from
     * @param role role to specifically filter
     * @param partitionId filter by ID
     * @return List of all PartitionPlacements for the table that are associated with a specific role for a specific partitionId
     */
    List<CatalogPartitionPlacement> getPartitionPlacementsByIdAndRole( long tableId, long partitionId, DataPlacementRole role );


    /**
     * Returns a specific partition entity which is placed on a store.
     *
     * @param adapterId The adapter on which the requested partition placements reside
     * @param partitionId The id of the requested partition
     * @return The requested PartitionPlacement on that store for a given is
     */
    CatalogPartitionPlacement getPartitionPlacement( long adapterId, long partitionId );

    /**
     * Returns a list of all Partition Placements which currently reside on an adapter, disregarded of the table.
     *
     * @param adapterId The adapter on which the requested partition placements reside
     * @return A list of all Partition Placements, that are currently located  on that specific store
     */
    List<CatalogPartitionPlacement> getPartitionPlacementsByAdapter( long adapterId );

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

    /**
     * Get all Partition Placements which are associated with an individual partition ID.
     * Identifies on which locations and how often the individual partition is placed.
     *
     * @param partitionId The requested partition ID
     * @return A list of Partition Placements which are physically responsible for that partition
     */
    List<CatalogPartitionPlacement> getPartitionPlacements( long partitionId );

    //// LOGISTICS

    boolean isHorizontalPartitioned( long id );


    boolean isVerticalPartitioned( long id );

    /**
     * Probes if a Partition Placement on an adapter for a specific partition already exists.
     *
     * @param adapterId Adapter on which to check
     * @param partitionId Partition which to check
     * @return teh response of the probe
     */
    boolean checkIfExistsPartitionPlacement( long adapterId, long partitionId );


    List<AllocationTable> getAllocationsFromLogical( long logicalId );

    boolean isPartitioned( long id );


    /**
     * Gets a specific placement for a graph on a given adapter.
     *
     * @param graphId The id of the graph
     * @param adapterId The id of the adapter on which the placement is placed
     * @return The placement matching the conditions
     */
    public abstract CatalogGraphPlacement getGraphPlacement( long graphId, long adapterId );


    /**
     * Gets a collection of graph placements for a given adapter.
     *
     * @param adapterId The id of the adapter on which the placements are placed
     * @return The collection of graph placements
     */
    public abstract List<CatalogGraphPlacement> getGraphPlacements( long adapterId );


    CatalogCollectionPlacement getCollectionPlacement( long id, long placementId );

    CatalogCollectionMapping getCollectionMapping( long id );

    List<CatalogCollectionPlacement> getCollectionPlacementsByAdapter( long id );

    List<CatalogCollectionPlacement> getCollectionPlacements( long collectionId );

    PartitionProperty getPartitionProperty( long id );

    boolean adapterHasPlacement( long adapterId, long id );

}
