/*
 * Copyright 2019-2022 The Polypheny Project
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.AllocationColumn;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;


@Slf4j
public abstract class AbstractPartitionManager implements PartitionManager {

    // Returns the Index of the partition where to place the object
    protected final Catalog catalog = Catalog.getInstance();


    // Returns the Index of the partition where to place the object
    @Override
    public abstract long getTargetPartitionId( LogicalTable catalogTable, String columnValue );


    @Override
    public boolean probePartitionGroupDistributionChange( LogicalTable catalogTable, int storeId, long columnId, int threshold ) {
        // Check for the specified columnId if we still have a ColumnPlacement for every partitionGroup
        for ( Long partitionGroupId : Catalog.getInstance().getSnapshot().alloc().getPartitionProperty( catalogTable.id ).partitionGroupIds ) {
            List<AllocationColumn> ccps = catalog.getSnapshot().alloc().getColumnPlacementsByPartitionGroup( catalogTable.id, partitionGroupId, columnId );
            if ( ccps.size() <= threshold ) {
                for ( AllocationColumn placement : ccps ) {
                    if ( placement.adapterId == storeId ) {
                        return false;
                    }
                }
            }
        }
        return true;
    }


    @Override
    public Map<Long, List<AllocationColumn>> getRelevantPlacements( LogicalTable catalogTable, List<Long> partitionIds, List<Long> excludedAdapters ) {
        Catalog catalog = Catalog.getInstance();

        Map<Long, List<AllocationColumn>> placementDistribution = new HashMap<>();

        if ( partitionIds != null ) {
            for ( long partitionId : partitionIds ) {
                AllocationEntity allocation = catalog.getSnapshot().alloc().getAllocation( partitionId );
                List<AllocationColumn> relevantCcps = new ArrayList<>();

                for ( LogicalColumn column : catalog.getSnapshot().rel().getColumns( catalogTable.id ) ) {
                    List<AllocationColumn> ccps = catalog.getSnapshot().alloc().getColumnPlacementsByPartitionGroup( catalogTable.id, allocation.id, column.id );
                    ccps.removeIf( ccp -> excludedAdapters.contains( ccp.adapterId ) );
                    if ( !ccps.isEmpty() ) {
                        // Get first column placement which contains partition
                        relevantCcps.add( ccps.get( 0 ) );
                        if ( log.isDebugEnabled() ) {
                            log.debug( "{} with part. {}", ccps.get( 0 ).getLogicalColumnName(), partitionId );
                        }
                    }
                }
                placementDistribution.put( partitionId, relevantCcps );
            }
        }

        return placementDistribution;
    }


    @Override
    public boolean validatePartitionGroupSetup(
            List<List<String>> partitionGroupQualifiers,
            long numPartitionGroups,
            List<String> partitionGroupNames,
            LogicalColumn partitionColumn ) {

        if ( numPartitionGroups == 0 && partitionGroupNames.size() < 2 ) {
            throw new RuntimeException( "Partitioning of table failed! Can't partition table with less than 2 partitions/names" );
        }
        return true;
    }


    // Returns 1 for most PartitionFunctions since they have a 1:1 relation between Groups and Internal Partitions
    // In that case the input of numberOfPartitions is omitted
    @Override
    public int getNumberOfPartitionsPerGroup( int numberOfPartitions ) {
        return 1;
    }


    /**
     * Returns the unified null value for all partition managers.
     * Such that every partitionValue occurrence of null ist treated equally
     *
     * @return null String
     */
    @Override
    public String getUnifiedNullValue() {
        return "null";
    }


    @Override
    public abstract PartitionFunctionInfo getPartitionFunctionInfo();


    @Override
    public Map<Long, Map<Long, List<AllocationColumn>>> getAllPlacements( LogicalTable catalogTable, List<Long> partitionIds ) {
        Map<Long, Map<Long, List<AllocationColumn>>> adapterPlacements = new HashMap<>(); // adapterId -> partitionId ; placements
        if ( partitionIds != null ) {
            for ( long partitionId : partitionIds ) {
                List<CatalogAdapter> adapters = catalog.getSnapshot().alloc().getAdaptersByPartitionGroup( catalogTable.id, partitionId );

                for ( CatalogAdapter adapter : adapters ) {
                    if ( !adapterPlacements.containsKey( adapter.id ) ) {
                        adapterPlacements.put( adapter.id, new HashMap<>() );
                    }
                    List<AllocationColumn> placements = catalog.getSnapshot().alloc().getColumnPlacementsOnAdapterPerTable( adapter.id, catalogTable.id );
                    adapterPlacements.get( adapter.id ).put( partitionId, placements );
                }
            }
        }
        return adapterPlacements;
    }

}
