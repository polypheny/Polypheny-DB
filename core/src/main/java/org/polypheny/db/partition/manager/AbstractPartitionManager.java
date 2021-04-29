/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.partition.manager;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.partition.PartitionFunctionInfo;


@Slf4j
public abstract class AbstractPartitionManager implements PartitionManager {


    // returns the Index of the partition where to place the object
    @Override
    public abstract long getTargetPartitionId( CatalogTable catalogTable, String columnValue );


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
            int numberOfFullPlacements = getPlacementsWithAllPartitions( columnId, table.numPartitions ).size();
            if ( numberOfFullPlacements >= 1 ) {
                log.debug( "Found ColumnPlacement which contains all partitions for column: {}", columnId );
                break;
            }

            if ( log.isDebugEnabled() ) {
                log.debug( "ERROR Column: '{}' has no placement containing all partitions", Catalog.getInstance().getColumn( columnId ).name );
            }
            return false;
        }

        return true;
    }


    @Override
    public abstract boolean probePartitionDistributionChange( CatalogTable catalogTable, int storeId, long columnId );

    @Override
    public abstract List<CatalogColumnPlacement> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionIds );


    @Override
    public boolean validatePartitionSetup(
            List<List<String>> partitionQualifiers,
            long numPartitions,
            List<String> partitionNames,
            CatalogColumn partitionColumn ) {
        if ( numPartitions == 0 && partitionNames.size() < 2 ) {
            throw new RuntimeException( "Partitioning of table failed! Can't partition table with less than 2 partitions/names" );
        }
        return true;
    }


    /**
     * Returns number of placements for this column which contain all partitions
     *
     * @param columnId column to be checked
     * @param numPartitions numPartitions
     * @return If its correctly distributed or not
     */
    protected List<CatalogColumnPlacement> getPlacementsWithAllPartitions( long columnId, long numPartitions ) {
        Catalog catalog = Catalog.getInstance();

        // Return every placement of this column
        List<CatalogColumnPlacement> tempCcps = catalog.getColumnPlacements( columnId );
        List<CatalogColumnPlacement> returnCcps = new ArrayList<>();
        int placementCounter = 0;
        for ( CatalogColumnPlacement ccp : tempCcps ) {
            // If the DataPlacement has stored all partitions and therefore all partitions for this placement
            if ( catalog.getPartitionsOnDataPlacement( ccp.adapterId, ccp.tableId ).size() == numPartitions ) {
                returnCcps.add( ccp );
                placementCounter++;
            }
        }
        return returnCcps;
    }


    @Override
    public abstract PartitionFunctionInfo getPartitionFunctionInfo();

}
