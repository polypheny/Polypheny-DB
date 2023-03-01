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

package org.polypheny.db.catalog.catalogs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPartitionGroup;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.partition.properties.PartitionProperty;

public interface AllocationRelationalCatalog extends AllocationCatalog {


    /**
     * Adds a placement for a column.
     *
     * @param adapterId The adapter on which the table should be placed on
     * @param columnId The id of the column to be placed
     * @param placementType The type of placement
     * @param physicalSchemaName The schema name on the adapter
     * @param physicalTableName The table name on the adapter
     * @param physicalColumnName The column name on the adapter
     */
    public abstract void addColumnPlacement( int adapterId, long columnId, PlacementType placementType, String physicalSchemaName, String physicalTableName, String physicalColumnName );

    /**
     * Deletes all dependent column placements
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @param columnOnly columnOnly If delete originates from a dropColumn
     */
    public abstract void deleteColumnPlacement( int adapterId, long columnId, boolean columnOnly );

    /**
     * Gets a collective list of column placements per column on an adapter.
     * Effectively used to retrieve all relevant placements including partitions.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @return The specific column placement
     */
    public abstract CatalogColumnPlacement getColumnPlacement( int adapterId, long columnId );

    /**
     * Checks if there is a column with the specified name in the specified table.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @return true if there is a column placement, false if not.
     */
    public abstract boolean checkIfExistsColumnPlacement( int adapterId, long columnId );

    /**
     * Get all column placements of a column
     *
     * @param columnId The id of the specific column
     * @return List of column placements of specific column
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacement( long columnId );

    /**
     * Get column placements of a specific table on a specific adapter on column detail level.
     * Only returns one ColumnPlacement per column on adapter. Ignores multiplicity due to different partitionsIds
     *
     * @param adapterId The id of the adapter
     * @return List of column placements of the table on the specified adapter
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacementsOnAdapterPerTable( int adapterId, long tableId );

    /**
     * Get column placements on a adapter. On column detail level
     * Only returns one ColumnPlacement per column on adapter. Ignores multiplicity due to different partitionsIds
     *
     * @param adapterId The id of the adapter
     * @return List of column placements on the specified adapter
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacementsOnAdapter( int adapterId );

    /**
     * Gets a collection of column placements for a given column.
     *
     * @param columnId The id of the column of requested column placements
     * @return The collection of placements sorted
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacementsByColumn( long columnId );

    /**
     * Gets all column placements of a table structured by the id of the adapters.
     *
     * @param tableId The id of the table for the requested column placements
     * @return The requested collection
     */
    public abstract ImmutableMap<Integer, ImmutableList<Long>> getColumnPlacementsByAdapter( long tableId );

    /**
     * Gets a map partition placements sorted by adapter.
     *
     * @param tableId The id of the table for which the partitions are returned
     * @return The sorted partitions placements
     */
    public abstract ImmutableMap<Integer, ImmutableList<Long>> getPartitionPlacementsByAdapter( long tableId );

    /**
     * Gets the partition group sorted by partition.
     *
     * @param partitionId The id of the partitions group
     */
    public abstract long getPartitionGroupByPartition( long partitionId );


    /**
     * Get column placements in a specific schema on a specific adapter
     *
     * @param adapterId The id of the adapter
     * @param schemaId The id of the schema
     * @return List of column placements on this adapter and schema
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacementsOnAdapterAndSchema( int adapterId, long schemaId );

    /**
     * Update the type of a placement.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @param placementType The new type of placement
     */
    public abstract void updateColumnPlacementType( int adapterId, long columnId, PlacementType placementType );

    /**
     * Update physical position of a column placement on a specified adapter.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @param position The physical position to set
     */
    public abstract void updateColumnPlacementPhysicalPosition( int adapterId, long columnId, long position );

    /**
     * Update physical position of a column placement on a specified adapter. Uses auto-increment to get the globally increasing number.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     */
    public abstract void updateColumnPlacementPhysicalPosition( int adapterId, long columnId );

    /**
     * Change physical names of all column placements.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @param physicalSchemaName The physical schema name
     * @param physicalColumnName The physical column name
     * @param updatePhysicalColumnPosition Whether to reset the column position (the highest number in the table; represents that the column is now at the last position)
     */
    public abstract void updateColumnPlacementPhysicalNames( int adapterId, long columnId, String physicalSchemaName, String physicalColumnName, boolean updatePhysicalColumnPosition );


    /**
     * Adds a partition to the catalog
     *
     * @param tableId The unique id of the table
     * @param schemaId The unique id of the table
     * @param partitionType partition Type of the added partition
     * @return The id of the created partitionGroup
     */
    public abstract long addPartitionGroup( long tableId, String partitionGroupName, long schemaId, PartitionType partitionType, long numberOfInternalPartitions, List<String> effectivePartitionGroupQualifier, boolean isUnbound ) throws GenericCatalogException;

    /**
     * Should only be called from mergePartitions(). Deletes a single partition and all references.
     *
     * @param tableId The unique id of the table
     * @param schemaId The unique id of the table
     * @param partitionGroupId The partitionId to be deleted
     */
    public abstract void deletePartitionGroup( long tableId, long schemaId, long partitionGroupId );

    /**
     * Get a partition object by its unique id
     *
     * @param partitionGroupId The unique id of the partition
     * @return A catalog partitionGroup
     */
    public abstract CatalogPartitionGroup getPartitionGroup( long partitionGroupId );

    /**
     * Adds a partition to the catalog
     *
     * @param tableId The unique id of the table
     * @param schemaId The unique id of the table
     * @param partitionGroupId partitionGroupId where the partition should be initially added to
     * @return The id of the created partition
     */
    public abstract long addPartition( long tableId, long schemaId, long partitionGroupId, List<String> effectivePartitionGroupQualifier, boolean isUnbound ) throws GenericCatalogException;

    /**
     * Deletes a single partition and all references.
     *
     * @param tableId The unique id of the table
     * @param schemaId The unique id of the table
     * @param partitionId The partitionId to be deleted
     */
    public abstract void deletePartition( long tableId, long schemaId, long partitionId );

    /**
     * Get a partition object by its unique id
     *
     * @param partitionId The unique id of the partition
     * @return A catalog partition
     */
    public abstract CatalogPartition getPartition( long partitionId );

    /**
     * Retrieves a list of partitions which are associated with a specific table
     *
     * @param tableId Table for which partitions shall be gathered
     * @return List of all partitions associated with that table
     */
    public abstract List<CatalogPartition> getPartitionsByTable( long tableId );

    /**
     * Effectively partitions a table with the specified partitionType
     *
     * @param tableId Table to be partitioned
     * @param partitionType Partition function to apply on the table
     * @param partitionColumnId Column used to apply the partition function on
     * @param numPartitionGroups Explicit number of partitions
     * @param partitionGroupIds List of ids of the catalog partitions
     */
    public abstract void partitionTable( long tableId, PartitionType partitionType, long partitionColumnId, int numPartitionGroups, List<Long> partitionGroupIds, PartitionProperty partitionProperty );

    /**
     * Merges a  partitioned table.
     * Resets all objects and structures which were introduced by partitionTable.
     *
     * @param tableId Table to be merged
     */
    public abstract void mergeTable( long tableId );

    /**
     * Updates partitionProperties on table
     *
     * @param tableId Table to be partitioned
     * @param partitionProperty Partition properties
     */
    public abstract void updateTablePartitionProperties( long tableId, PartitionProperty partitionProperty );

    /**
     * Get a List of all partitions belonging to a specific table
     *
     * @param tableId Table to be queried
     * @return list of all partitions on this table
     */
    public abstract List<CatalogPartitionGroup> getPartitionGroups( long tableId );

    /**
     * Get all partitions of the specified database which fit to the specified filter patterns.
     * <code>getColumns(xid, databaseName, null, null, null)</code> returns all partitions of the database.
     *
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns catalog/src/test/java/org/polypheny/db/test/CatalogTest.javaall.
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogPartitionGroup> getPartitionGroups( Pattern schemaNamePattern, Pattern tableNamePattern );

    /**
     * Updates the specified partition group with the attached partitionIds
     *
     * @param partitionGroupId Partition Group to be updated
     * @param partitionIds List of new partitionIds
     */
    public abstract void updatePartitionGroup( long partitionGroupId, List<Long> partitionIds );

    /**
     * Adds a partition to an already existing partition Group
     *
     * @param partitionGroupId Group to add to
     * @param partitionId Partition to add
     */
    public abstract void addPartitionToGroup( long partitionGroupId, Long partitionId );

    /**
     * Removes a partition from an already existing partition Group
     *
     * @param partitionGroupId Group to remove the partition from
     * @param partitionId Partition to remove
     */
    public abstract void removePartitionFromGroup( long partitionGroupId, Long partitionId );

    /**
     * Assign the partition to a new partitionGroup
     *
     * @param partitionId Partition to move
     * @param partitionGroupId New target group to move the partition to
     */
    public abstract void updatePartition( long partitionId, Long partitionGroupId );

    /**
     * Get a List of all partitions belonging to a specific table
     *
     * @param partitionGroupId Table to be queried
     * @return list of all partitions on this table
     */
    public abstract List<CatalogPartition> getPartitions( long partitionGroupId );

    /**
     * Get all partitions of the specified database which fit to the specified filter patterns.
     * <code>getColumns(xid, databaseName, null, null, null)</code> returns all partitions of the database.
     *
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns catalog/src/test/java/org/polypheny/db/test/CatalogTest.javaall.
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogPartition> getPartitions( Pattern schemaNamePattern, Pattern tableNamePattern );

    /**
     * Get a list of all partition name belonging to a specific table
     *
     * @param tableId Table to be queried
     * @return list of all partition names on this table
     */
    public abstract List<String> getPartitionGroupNames( long tableId );

    /**
     * Get placements by partition. Identify the location of partitions.
     * Essentially returns all ColumnPlacements which hold the specified partitionID.
     *
     * @param tableId The id of the table
     * @param partitionGroupId The id of the partition
     * @param columnId The id of tje column
     * @return List of CatalogColumnPlacements
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacementsByPartitionGroup( long tableId, long partitionGroupId, long columnId );

    /**
     * Get adapters by partition. Identify the location of partitions/replicas
     * Essentially returns all adapters which hold the specified partitionID
     *
     * @param tableId The unique id of the table
     * @param partitionGroupId The unique id of the partition
     * @return List of CatalogAdapters
     */
    public abstract List<CatalogAdapter> getAdaptersByPartitionGroup( long tableId, long partitionGroupId );

    /**
     * Get all partitions of a DataPlacement (identified by adapterId and tableId)
     *
     * @param adapterId The unique id of the adapter
     * @param tableId The unique id of the table
     * @return List of partitionIds
     */
    public abstract List<Long> getPartitionGroupsOnDataPlacement( int adapterId, long tableId );

    /**
     * Get all partitions of a DataPlacement (identified by adapterId and tableId)
     *
     * @param adapterId The unique id of the adapter
     * @param tableId The unique id of the table
     * @return List of partitionIds
     */
    public abstract List<Long> getPartitionsOnDataPlacement( int adapterId, long tableId );

    /**
     * Returns list with the index of the partitions on this store from  0..numPartitions
     *
     * @param adapterId The unique id of the adapter
     * @param tableId The unique id of the table
     * @return List of partitionId Indices
     */
    public abstract List<Long> getPartitionGroupsIndexOnDataPlacement( int adapterId, long tableId );

    /**
     * Returns a specific DataPlacement of a given table.
     *
     * @param adapterId adapter where placement is located
     * @param tableId table to retrieve the placement from
     * @return DataPlacement of a table placed on a specific store
     */
    public abstract CatalogDataPlacement getDataPlacement( int adapterId, long tableId );

    /**
     * Returns all DataPlacements of a given table.
     *
     * @param tableId table to retrieve the placements from
     * @return List of all DataPlacements for the table
     */
    public abstract List<CatalogDataPlacement> getDataPlacements( long tableId );

    /**
     * Returns a list of all DataPlacements that contain all columns as well as all partitions
     *
     * @param tableId table to retrieve the list from
     * @return list of all full DataPlacements
     */
    public abstract List<CatalogDataPlacement> getAllFullDataPlacements( long tableId );

    /**
     * Returns a list of all DataPlacements that contain all columns
     *
     * @param tableId table to retrieve the list from
     * @return list of all full DataPlacements
     */
    public abstract List<CatalogDataPlacement> getAllColumnFullDataPlacements( long tableId );

    /**
     * Returns a list of all DataPlacements that contain all partitions
     *
     * @param tableId table to retrieve the list from
     * @return list of all full DataPlacements
     */
    public abstract List<CatalogDataPlacement> getAllPartitionFullDataPlacements( long tableId );

    /**
     * Returns all DataPlacements of a given table that are associated with a given role.
     *
     * @param tableId table to retrieve the placements from
     * @param role role to specifically filter
     * @return List of all DataPlacements for the table that are associated with a specific role
     */
    public abstract List<CatalogDataPlacement> getDataPlacementsByRole( long tableId, DataPlacementRole role );

    /**
     * Returns all PartitionPlacements of a given table that are associated with a given role.
     *
     * @param tableId table to retrieve the placements from
     * @param role role to specifically filter
     * @return List of all PartitionPlacements for the table that are associated with a specific role
     */
    public abstract List<CatalogPartitionPlacement> getPartitionPlacementsByRole( long tableId, DataPlacementRole role );

    /**
     * Returns all PartitionPlacements of a given table with a given ID that are associated with a given role.
     *
     * @param tableId table to retrieve the placements from
     * @param role role to specifically filter
     * @param partitionId filter by ID
     * @return List of all PartitionPlacements for the table that are associated with a specific role for a specific partitionId
     */
    public abstract List<CatalogPartitionPlacement> getPartitionPlacementsByIdAndRole( long tableId, long partitionId, DataPlacementRole role );

    /**
     * Checks if the planned changes are allowed in terms of placements that need to be present.
     * Each column must be present for all partitions somewhere.
     *
     * @param tableId Table to be checked
     * @param adapterId Adapter where Ids will be removed from
     * @param columnIdsToBeRemoved columns that shall be removed
     * @param partitionsIdsToBeRemoved partitions that shall be removed
     * @return true if these changes can be made to the data placement, false if not
     */
    public abstract boolean validateDataPlacementsConstraints( long tableId, long adapterId, List<Long> columnIdsToBeRemoved, List<Long> partitionsIdsToBeRemoved );


    /**
     * Adds a placement for a partition.
     *
     * @param namespaceId
     * @param adapterId The adapter on which the table should be placed on
     * @param tableId The table for which a partition placement shall be created
     * @param partitionId The id of a specific partition that shall create a new placement
     * @param placementType The type of placement
     * @param physicalSchemaName The schema name on the adapter
     * @param physicalTableName The table name on the adapter
     */
    public abstract void addPartitionPlacement( long namespaceId, int adapterId, long tableId, long partitionId, PlacementType placementType, String physicalSchemaName, String physicalTableName, DataPlacementRole role );

    /**
     * Adds a new DataPlacement for a given table on a specific store
     *
     * @param adapterId adapter where placement should be located
     * @param tableId table to retrieve the placement from
     */
    public abstract void addDataPlacement( int adapterId, long tableId );

    /**
     * Adds a new DataPlacement for a given table on a specific store.
     * If it already exists it simply returns the existing placement.
     *
     * @param adapterId adapter where placement is located
     * @param tableId table to retrieve the placement from
     * @return DataPlacement of a table placed on a specific store
     */
    public abstract CatalogDataPlacement addDataPlacementIfNotExists( int adapterId, long tableId );

    /**
     * Modifies a specific DataPlacement of a given table.
     *
     * @param adapterId adapter where placement is located
     * @param tableId table to retrieve the placement from
     * @param catalogDataPlacement new dataPlacement to be written
     */
    abstract void modifyDataPlacement( int adapterId, long tableId, CatalogDataPlacement catalogDataPlacement );


    /**
     * Removes a DataPlacement for a given table on a specific store
     *
     * @param adapterId adapter where placement should be removed from
     * @param tableId table to retrieve the placement from
     */
    public abstract void removeDataPlacement( int adapterId, long tableId );

    /**
     * Adds a single dataPlacement on a store for a specific table
     *
     * @param adapterId adapter id corresponding to a new DataPlacements
     * @param tableId table to be updated
     */
    abstract void addSingleDataPlacementToTable( Integer adapterId, long tableId );

    /**
     * Removes a single dataPlacement from a store for a specific table
     *
     * @param adapterId adapter id corresponding to a new DataPlacements
     * @param tableId table to be updated
     */
    abstract void removeSingleDataPlacementFromTable( Integer adapterId, long tableId );

    /**
     * Updates the list of data placements on a table
     *
     * @param tableId table to be updated
     * @param newDataPlacements list of new DataPlacements that shall replace the old ones
     */
    public abstract void updateDataPlacementsOnTable( long tableId, List<Integer> newDataPlacements );

    /**
     * Adds columns to dataPlacement on a store for a specific table
     *
     * @param adapterId adapter id corresponding to a new DataPlacements
     * @param tableId table to be updated
     * @param columnIds List of columnIds to add to a specific store for the table
     */
    abstract void addColumnsToDataPlacement( int adapterId, long tableId, List<Long> columnIds );

    /**
     * Remove columns to dataPlacement on a store for a specific table
     *
     * @param adapterId adapter id corresponding to a new DataPlacements
     * @param tableId table to be updated
     * @param columnIds List of columnIds to remove from a specific store for the table
     */
    abstract void removeColumnsFromDataPlacement( int adapterId, long tableId, List<Long> columnIds );

    /**
     * Adds partitions to dataPlacement on a store for a specific table
     *
     * @param adapterId adapter id corresponding to a new DataPlacements
     * @param tableId table to be updated
     * @param partitionIds List of partitionIds to add to a specific store for the table
     */
    abstract void addPartitionsToDataPlacement( int adapterId, long tableId, List<Long> partitionIds );

    /**
     * Remove partitions to dataPlacement on a store for a specific table
     *
     * @param adapterId adapter id corresponding to a new DataPlacements
     * @param tableId table to be updated
     * @param partitionIds List of partitionIds to remove from a specific store for the table
     */
    abstract void removePartitionsFromDataPlacement( int adapterId, long tableId, List<Long> partitionIds );

    /**
     * Updates and overrides list of associated columnPlacements {@code &} partitionPlacements for a given data placement
     *
     * @param adapterId adapter where placement is located
     * @param tableId table to retrieve the placement from
     * @param columnIds List of columnIds to be located on a specific store for the table
     * @param partitionIds List of partitionIds to be located on a specific store for the table
     */
    public abstract void updateDataPlacement( int adapterId, long tableId, List<Long> columnIds, List<Long> partitionIds );


    /**
     * Deletes a placement for a partition.
     *
     * @param adapterId The adapter on which the table should be placed on
     * @param partitionId The id of a partition which shall be removed from that store.
     */
    public abstract void deletePartitionPlacement( int adapterId, long partitionId );

    /**
     * Returns a specific partition entity which is placed on a store.
     *
     * @param adapterId The adapter on which the requested partition placements reside
     * @param partitionId The id of the requested partition
     * @return The requested PartitionPlacement on that store for a given is
     */
    public abstract CatalogPartitionPlacement getPartitionPlacement( int adapterId, long partitionId );

    /**
     * Returns a list of all Partition Placements which currently reside on an adapter, disregarded of the table.
     *
     * @param adapterId The adapter on which the requested partition placements reside
     * @return A list of all Partition Placements, that are currently located  on that specific store
     */
    public abstract List<CatalogPartitionPlacement> getPartitionPlacementsByAdapter( int adapterId );

    /**
     * Returns a list of all Partition Placements which currently reside on an adapter, for a specific table.
     *
     * @param adapterId The adapter on which the requested partition placements reside
     * @param tableId The table for which all partition placements on an adapter should be considered
     * @return A list of all Partition Placements, that are currently located  on that specific store for an individual table
     */
    public abstract List<CatalogPartitionPlacement> getPartitionPlacementsByTableOnAdapter( int adapterId, long tableId );

    /**
     * Returns a list of all Partition Placements which are currently associated with a table.
     *
     * @param tableId The table on which the requested partition placements are currently associated with.
     * @return A list of all Partition Placements, that belong to the desired table
     */
    public abstract List<CatalogPartitionPlacement> getAllPartitionPlacementsByTable( long tableId );

    /**
     * Get all Partition Placements which are associated with an individual partition ID.
     * Identifies on which locations and how often the individual partition is placed.
     *
     * @param partitionId The requested partition ID
     * @return A list of Partition Placements which are physically responsible for that partition
     */
    public abstract List<CatalogPartitionPlacement> getPartitionPlacements( long partitionId );

    /**
     * Registers a table to be considered for periodic processing
     *
     * @param tableId ID of table to be considered for periodic processing
     */
    public abstract void addTableToPeriodicProcessing( long tableId );

    /**
     * Remove a table from periodic background processing
     *
     * @param tableId ID of table to be removed for periodic processing
     */
    public abstract void removeTableFromPeriodicProcessing( long tableId );

    /**
     * Probes if a Partition Placement on an adapter for a specific partition already exists.
     *
     * @param adapterId Adapter on which to check
     * @param partitionId Partition which to check
     * @return teh response of the probe
     */
    public abstract boolean checkIfExistsPartitionPlacement( int adapterId, long partitionId );


}
