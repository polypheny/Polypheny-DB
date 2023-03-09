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

import java.util.List;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.partition.properties.PartitionProperty;

public interface AllocationRelationalCatalog extends AllocationCatalog {


    /**
     * Adds a placement for a column.
     *
     * @param table
     * @param adapterId The adapter on which the table should be placed on
     * @param columnId The id of the column to be placed
     * @param placementType The type of placement
     * @param physicalSchemaName The schema name on the adapter
     * @param physicalTableName The table name on the adapter
     * @param physicalColumnName The column name on the adapter
     * @param position
     */
    void addColumnPlacement( LogicalTable table, long adapterId, long columnId, PlacementType placementType, String physicalSchemaName, String physicalTableName, String physicalColumnName, int position );

    /**
     * Deletes all dependent column placements
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @param columnOnly columnOnly If delete originates from a dropColumn
     */
    void deleteColumnPlacement( long adapterId, long columnId, boolean columnOnly );



    /**
     * Update the type of a placement.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @param placementType The new type of placement
     */
    void updateColumnPlacementType( long adapterId, long columnId, PlacementType placementType );

    /**
     * Update physical position of a column placement on a specified adapter.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @param position The physical position to set
     */
    void updateColumnPlacementPhysicalPosition( long adapterId, long columnId, long position );

    /**
     * Update physical position of a column placement on a specified adapter. Uses auto-increment to get the globally increasing number.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     */
    void updateColumnPlacementPhysicalPosition( long adapterId, long columnId );

    /**
     * Change physical names of all column placements.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @param physicalSchemaName The physical schema name
     * @param physicalColumnName The physical column name
     * @param updatePhysicalColumnPosition Whether to reset the column position (the highest number in the table; represents that the column is now at the last position)
     */
    void updateColumnPlacementPhysicalNames( long adapterId, long columnId, String physicalSchemaName, String physicalColumnName, boolean updatePhysicalColumnPosition );


    /**
     * Adds a partition to the catalog
     *
     * @param tableId The unique id of the table
     * @param schemaId The unique id of the table
     * @param partitionType partition Type of the added partition
     * @return The id of the created partitionGroup
     */
    long addPartitionGroup( long tableId, String partitionGroupName, long schemaId, PartitionType partitionType, long numberOfInternalPartitions, List<String> effectivePartitionGroupQualifier, boolean isUnbound ) throws GenericCatalogException;

    /**
     * Should only be called from mergePartitions(). Deletes a single partition and all references.
     *
     * @param tableId The unique id of the table
     * @param schemaId The unique id of the table
     * @param partitionGroupId The partitionId to be deleted
     */
    void deletePartitionGroup( long tableId, long schemaId, long partitionGroupId );


    /**
     * Adds a partition to the catalog
     *
     * @param tableId The unique id of the table
     * @param schemaId The unique id of the table
     * @param partitionGroupId partitionGroupId where the partition should be initially added to
     * @return The id of the created partition
     */
    long addPartition( long tableId, long schemaId, long partitionGroupId, List<String> effectivePartitionGroupQualifier, boolean isUnbound ) throws GenericCatalogException;

    /**
     * Deletes a single partition and all references.
     *
     * @param tableId The unique id of the table
     * @param schemaId The unique id of the table
     * @param partitionId The partitionId to be deleted
     */
    void deletePartition( long tableId, long schemaId, long partitionId );


    /**
     * Effectively partitions a table with the specified partitionType
     *
     * @param tableId Table to be partitioned
     * @param partitionType Partition function to apply on the table
     * @param partitionColumnId Column used to apply the partition function on
     * @param numPartitionGroups Explicit number of partitions
     * @param partitionGroupIds List of ids of the catalog partitions
     */
    void partitionTable( long tableId, PartitionType partitionType, long partitionColumnId, int numPartitionGroups, List<Long> partitionGroupIds, PartitionProperty partitionProperty );

    /**
     * Merges a  partitioned table.
     * Resets all objects and structures which were introduced by partitionTable.
     *
     * @param tableId Table to be merged
     */
    void mergeTable( long tableId );

    /**
     * Updates partitionProperties on table
     *
     * @param tableId Table to be partitioned
     * @param partitionProperty Partition properties
     */
    void updateTablePartitionProperties( long tableId, PartitionProperty partitionProperty );


    /**
     * Updates the specified partition group with the attached partitionIds
     *
     * @param partitionGroupId Partition Group to be updated
     * @param partitionIds List of new partitionIds
     */
    void updatePartitionGroup( long partitionGroupId, List<Long> partitionIds );

    /**
     * Adds a partition to an already existing partition Group
     *
     * @param partitionGroupId Group to add to
     * @param partitionId Partition to add
     */
    void addPartitionToGroup( long partitionGroupId, Long partitionId );

    /**
     * Removes a partition from an already existing partition Group
     *
     * @param partitionGroupId Group to remove the partition from
     * @param partitionId Partition to remove
     */
    void removePartitionFromGroup( long partitionGroupId, Long partitionId );

    /**
     * Assign the partition to a new partitionGroup
     *
     * @param partitionId Partition to move
     * @param partitionGroupId New target group to move the partition to
     */
    void updatePartition( long partitionId, Long partitionGroupId );


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
    boolean validateDataPlacementsConstraints( long tableId, long adapterId, List<Long> columnIdsToBeRemoved, List<Long> partitionsIdsToBeRemoved );


    /**
     * Adds a placement for a partition.
     *
     * @param namespaceId
     * @param adapterId The adapter on which the table should be placed on
     * @param tableId The table for which a partition placement shall be created
     * @param partitionId The id of a specific partition that shall create a new placement
     * @param placementType The type of placement
     */
    void addPartitionPlacement( long namespaceId, long adapterId, long tableId, long partitionId, PlacementType placementType, DataPlacementRole role );

    /**
     * Adds a new DataPlacement for a given table on a specific store
     *
     * @param adapterId adapter where placement should be located
     * @param tableId table to retrieve the placement from
     */
    void addDataPlacement( long adapterId, long tableId );

    /**
     * Adds a new DataPlacement for a given table on a specific store.
     * If it already exists it simply returns the existing placement.
     *
     * @param adapterId adapter where placement is located
     * @param tableId table to retrieve the placement from
     * @return DataPlacement of a table placed on a specific store
     */
    CatalogDataPlacement addDataPlacementIfNotExists( long adapterId, long tableId );

    /**
     * Modifies a specific DataPlacement of a given table.
     *
     * @param adapterId adapter where placement is located
     * @param tableId table to retrieve the placement from
     * @param catalogDataPlacement new dataPlacement to be written
     */
    void modifyDataPlacement( long adapterId, long tableId, CatalogDataPlacement catalogDataPlacement );


    /**
     * Removes a DataPlacement for a given table on a specific store
     *
     * @param adapterId adapter where placement should be removed from
     * @param tableId table to retrieve the placement from
     */
    void removeDataPlacement( long adapterId, long tableId );

    /**
     * Adds a single dataPlacement on a store for a specific table
     *
     * @param adapterId adapter id corresponding to a new DataPlacements
     * @param tableId table to be updated
     */
    void addSingleDataPlacementToTable( long adapterId, long tableId );

    /**
     * Removes a single dataPlacement from a store for a specific table
     *
     * @param adapterId adapter id corresponding to a new DataPlacements
     * @param tableId table to be updated
     */
    void removeSingleDataPlacementFromTable( long adapterId, long tableId );

    /**
     * Updates the list of data placements on a table
     *
     * @param tableId table to be updated
     * @param newDataPlacements list of new DataPlacements that shall replace the old ones
     */
    void updateDataPlacementsOnTable( long tableId, List<Integer> newDataPlacements );

    /**
     * Adds columns to dataPlacement on a store for a specific table
     *
     * @param adapterId adapter id corresponding to a new DataPlacements
     * @param tableId table to be updated
     * @param columnIds List of columnIds to add to a specific store for the table
     */
    void addColumnsToDataPlacement( long adapterId, long tableId, List<Long> columnIds );

    /**
     * Remove columns to dataPlacement on a store for a specific table
     *
     * @param adapterId adapter id corresponding to a new DataPlacements
     * @param tableId table to be updated
     * @param columnIds List of columnIds to remove from a specific store for the table
     */
    void removeColumnsFromDataPlacement( long adapterId, long tableId, List<Long> columnIds );

    /**
     * Adds partitions to dataPlacement on a store for a specific table
     *
     * @param adapterId adapter id corresponding to a new DataPlacements
     * @param tableId table to be updated
     * @param partitionIds List of partitionIds to add to a specific store for the table
     */
    void addPartitionsToDataPlacement( long adapterId, long tableId, List<Long> partitionIds );

    /**
     * Remove partitions to dataPlacement on a store for a specific table
     *
     * @param adapterId adapter id corresponding to a new DataPlacements
     * @param tableId table to be updated
     * @param partitionIds List of partitionIds to remove from a specific store for the table
     */
    void removePartitionsFromDataPlacement( long adapterId, long tableId, List<Long> partitionIds );

    /**
     * Updates and overrides list of associated columnPlacements {@code &} partitionPlacements for a given data placement
     *
     * @param adapterId adapter where placement is located
     * @param tableId table to retrieve the placement from
     * @param columnIds List of columnIds to be located on a specific store for the table
     * @param partitionIds List of partitionIds to be located on a specific store for the table
     */
    void updateDataPlacement( long adapterId, long tableId, List<Long> columnIds, List<Long> partitionIds );


    /**
     * Deletes a placement for a partition.
     *
     * @param adapterId The adapter on which the table should be placed on
     * @param partitionId The id of a partition which shall be removed from that store.
     */
    void deletePartitionPlacement( long adapterId, long partitionId );



    /**
     * Registers a table to be considered for periodic processing
     *
     * @param tableId ID of table to be considered for periodic processing
     */
    void addTableToPeriodicProcessing( long tableId );

    /**
     * Remove a table from periodic background processing
     *
     * @param tableId ID of table to be removed for periodic processing
     */
    void removeTableFromPeriodicProcessing( long tableId );


}
