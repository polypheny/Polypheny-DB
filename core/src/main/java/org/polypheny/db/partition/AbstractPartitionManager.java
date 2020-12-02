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
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;

// Possible extensions could be range partitioning and hash partitioning
// Need to check if round robin would be sufficient as well or basically just needed to distribute workload for LoadBalancing
// Maybe separate partition in the technical-partition itself.
// And meta information about the partition characteristics of a table
// the latter could maybe be specified in the table as well.
public abstract class AbstractPartitionManager implements PartitionManager {


    @Getter
    protected boolean allowsUnboundPartition;


    // returns the Index of the partition where to place the object
    @Override
    public abstract long getTargetPartitionId( CatalogTable catalogTable, String columnValue );

    @Override
    public abstract boolean validatePartitionDistribution( CatalogTable table );

    @Override
    public abstract boolean probePartitionDistributionChange( CatalogTable catalogTable, int storeId, long columnId );

    @Override
    public abstract List<CatalogColumnPlacement> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionIds );


    @Override
    public boolean validatePartitionSetup( List<List<String>> partitionQualifiers, long numPartitions, List<String> partitionNames, CatalogColumn partitionColumn ) {
        if ( numPartitions == 0 && partitionNames.size() < 2 ) {
            throw new RuntimeException( "Partitioning of table failed! Can't specify partition names with less than 2 names" );
        }
        return true;
    }


    @Override
    public abstract boolean allowsUnboundPartition();


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
            if ( catalog.getPartitionsOnDataPlacement( ccp.storeId, ccp.tableId ).size() == numPartitions ) {
                returnCcps.add( ccp );
                placementCounter++;
            }
        }
        return returnCcps;
    }
}
