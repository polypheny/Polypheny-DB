/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.partition;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;

@Slf4j
public class HashPartitionManager extends AbstractPartitionManager {

    public boolean allowsUnboundPartition = false;


    @Override
    public long getTargetPartitionId( CatalogTable catalogTable, String columnValue ) {
        log.debug( "HashPartitionManager" );

        long partitionID = 0;
        String partitionKey = "";

        partitionID = columnValue.hashCode() * -1;

        // Don't want any neg. value for now
        if ( partitionID <= 0 ) {
            partitionID *= -1;
        }

        // Finally decide on which partition to put it
        return catalogTable.partitionIds.get( (int) (partitionID % catalogTable.numPartitions) );
    }


    /**
     * Validates the table if the partitions are sufficiently distributed.
     * There has to be at least on columnPlacement which contains all partitions
     *
     * @param table Table to be checked
     * @return If its correctly distributed or not
     */
    @Override
    public boolean validatePartitionDistribution( CatalogTable table ) {

        // Check for every column if there exists at least one placement which contains all partitions
        for ( long columnId : table.columnIds ) {
            boolean skip = false;

            int numberOfFullPlacements = getPlacementsWithAllPartitions( columnId, table.numPartitions ).size();
            if ( numberOfFullPlacements >= 1 ) {
                log.debug( "Found ColumnPlacement which contains all partitions for column: " + columnId );
                skip = true;
                break;
            }

            if ( skip ) {
                continue;
            } else {
                log.debug( "ERROR Column: '" + Catalog.getInstance().getColumn( columnId ).name + "' has no placement containing all partitions" );
                return false;
            }
        }

        return true;
    }


    // Needed when columnPlacements are being dropped
    // HASH Partitioning needs at least one columnplacement which contains all partitions as a fallback
    @Override
    public boolean probePartitionDistributionChange( CatalogTable catalogTable, int storeId, long columnId ) {

        // Change is only critical if there is only one column left with the characteristics
        int numberOfFullPlacements = getPlacementsWithAllPartitions( columnId, catalogTable.numPartitions ).size();
        if ( numberOfFullPlacements <= 1 ) {
            Catalog catalog = Catalog.getInstance();
            //Check if this one column is the column we are about to delete
            if ( catalog.getPartitionsOnDataPlacement( storeId, catalogTable.id ).size() == catalogTable.numPartitions ) {
                return false;
            }
        }

        return true;
    }


    @Override
    public List<CatalogColumnPlacement> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionIds ) {

        Catalog catalog = Catalog.getInstance();
        List<CatalogColumnPlacement> relevantCcps = new ArrayList<>();
        // Find stores with fullplacements (partitions)
        // Pick for each column the columnplacemnt which has full partitioning //SELECT WORSTCASE ergo Fallback
        for ( long columnId : catalogTable.columnIds ) {
            // Take the first column placement
            relevantCcps.add( getPlacementsWithAllPartitions( columnId, catalogTable.numPartitions ).get( 0 ) );
        }

        return relevantCcps;
    }


    @Override
    public boolean validatePartitionSetup( List<String> partitionQualifiers, long numPartitions, List<String> partitionNames ) {
        super.validatePartitionSetup( partitionQualifiers, numPartitions, partitionNames );

        if ( !partitionQualifiers.isEmpty() ) {
            throw new RuntimeException( "PartitionType HASH does not support the assignment of values to partitions" );
        }
        if ( numPartitions < 2 ) {
            throw new RuntimeException( "You can't partition a table with less than 2 partitions. You only specified: '" + numPartitions + "'" );
        }

        return true;
    }


    @Override
    public boolean allowsUnboundPartition() {
        return false;
    }


    /**
     * Returns number of placements for this column which contain all partitions
     *
     * @param columnId column to be checked
     * @param numPartitions numPartitions
     * @return If its correctly distributed or not
     */
    private List<CatalogColumnPlacement> getPlacementsWithAllPartitions( long columnId, long numPartitions ) {

        Catalog catalog = Catalog.getInstance();

        // Return every placement of this column
        List<CatalogColumnPlacement> tempCcps = catalog.getColumnPlacements( columnId );
        List<CatalogColumnPlacement> returnCcps = new ArrayList<>();
        int placementCounter = 0;
        for ( CatalogColumnPlacement ccp : tempCcps ) {
            // If the DataPlacement has stored all partitions and therefore all partitions for this placement
            if ( catalog.getPartitionsOnDataPlacement( ccp.storeId, ccp.tableId ).size() == numPartitions ) {
                returnCcps.add( ccp );
                placementCounter++;
            }
        }
        return returnCcps;

    }

}
