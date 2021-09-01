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

package org.polypheny.db.partition;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;


@Slf4j
public abstract class AbstractPartitionManager implements PartitionManager {


    // returns the Index of the partition where to place the object
    @Override
    public abstract long getTargetPartitionId( CatalogTable catalogTable, String columnValue );


    @Override
    public boolean probePartitionGroupDistributionChange( CatalogTable catalogTable, int storeId, long columnId, int threshold ) {
        Catalog catalog = Catalog.getInstance();

        //Check for the specified columnId if we still have a ColumnPlacement for every partitionGroup
        for ( Long partitionGroupId : catalogTable.partitionProperty.partitionGroupIds ) {
            List<CatalogColumnPlacement> ccps = catalog.getColumnPlacementsByPartitionGroup( catalogTable.id, partitionGroupId, columnId );
            if ( ccps.size() <= threshold ) {
                for ( CatalogColumnPlacement placement : ccps ) {
                    if ( placement.adapterId == storeId ) {
                        return false;
                    }
                }
            }
        }
        return true;
    }


    @Override
    public abstract Map<Long, List<CatalogColumnPlacement>> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionIds );


    @Override
    public boolean validatePartitionGroupSetup(
            List<List<String>> partitionGroupQualifiers,
            long numPartitionGroups,
            List<String> partitionGroupNames,
            CatalogColumn partitionColumn ) {
        if ( numPartitionGroups == 0 && partitionGroupNames.size() < 2 ) {
            throw new RuntimeException( "Partitioning of table failed! Can't partition table with less than 2 partitions/names" );
        }
        return true;
    }


    //Returns 1 for most PartitionFunctions since they have a 1:1 relation between Groups and Internal Partitions
    //In that case the input of numberOfPartitions is ommitted
    @Override
    public int getNumberOfPartitionsPerGroup( int numberOfPartitions ) {
        return 1;
    }


    @Override
    public abstract PartitionFunctionInfo getPartitionFunctionInfo();

}
