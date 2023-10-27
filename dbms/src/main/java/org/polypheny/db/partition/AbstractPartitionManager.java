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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.partition.properties.PartitionProperty;


@Slf4j
public abstract class AbstractPartitionManager implements PartitionManager {

    // Returns the Index of the partition where to place the object
    protected final Catalog catalog = Catalog.getInstance();


    // Returns the Index of the partition where to place the object
    @Override
    public abstract long getTargetPartitionId( LogicalTable table, PartitionProperty property, String columnValue );


    @Override
    public boolean probePartitionGroupDistributionChange( LogicalTable catalogTable, int storeId, long columnId, int threshold ) {
        // Check for the specified columnId if we still have a ColumnPlacement for every partitionGroup
        for ( Long partitionGroupId : Catalog.getInstance().getSnapshot().alloc().getPartitionProperty( catalogTable.id ).orElseThrow().partitionGroupIds ) {
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
    public Map<Long, List<AllocationColumn>> getRelevantPlacements( LogicalTable catalogTable, List<AllocationEntity> allocs, List<Long> excludedAdapters ) {

        Map<Long, List<AllocationColumn>> placementDistribution = new HashMap<>();

        if ( allocs != null ) {
            for ( AllocationEntity allocation : allocs ) {
                if ( excludedAdapters.contains( allocation.adapterId ) ) {
                    continue;
                }
                List<AllocationColumn> allocColumns = allocation.unwrap( AllocationTable.class ).getColumns();
                if( placementDistribution.containsKey( allocation.partitionId ) ){
                    List<AllocationColumn> existingAllocColumns = placementDistribution.get( allocation.partitionId );
                    List<Long> existingColumnsIds = existingAllocColumns.stream().map( e -> e.columnId ).collect( Collectors.toList() );
                    List<Long> allocColumnIds = allocColumns.stream().map( c -> c.columnId ).collect( Collectors.toList() );
                    // contains all already
                    if ( allocColumns.stream().map( c -> c.columnId ).allMatch( existingColumnsIds::contains ) ) {
                        continue;
                    } else if ( existingAllocColumns.stream().map( c -> c.columnId ).allMatch( allocColumnIds::contains ) ) {
                        // contains all & more -> replace
                        if ( allocColumns.size() > existingAllocColumns.size() ) {
                            allocColumns = existingAllocColumns;
                        }
                    } else {
                        // contains additional -> add
                        allocColumns = Stream.concat( existingAllocColumns.stream(), allocColumns.stream().filter( c -> !existingColumnsIds.contains( c.columnId ) ) ).collect( Collectors.toList() );
                    }

                }

                /*for ( LogicalColumn column : catalog.getSnapshot().rel().getColumns( catalogTable.id ) ) {
                    List<AllocationPlacement> placements = new ArrayList<>( catalog.getSnapshot().alloc().getPlacementsOfColumn( column.id ) );
                    if ( !placements.isEmpty() ) {
                        // Get first column placement which contains partition
                        relevantCcps.add( catalog.getSnapshot().alloc().getColumn( placements.get( 0 ).id, column.id ).orElseThrow() );
                        if ( log.isDebugEnabled() ) {
                            log.debug( "{} with part. {}", column.name, allocation.id );
                        }
                    }
                }*/
                placementDistribution.put( allocation.partitionId, allocColumns );
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


    @Override
    public abstract PartitionFunctionInfo getPartitionFunctionInfo();


    @Override
    public Map<Long, Map<Long, List<AllocationColumn>>> getAllPlacements( LogicalTable catalogTable, List<Long> partitionIds ) {
        Map<Long, Map<Long, List<AllocationColumn>>> adapterPlacements = new HashMap<>(); // placementId -> partitionId ; placements
        if ( partitionIds != null ) {
            for ( long partitionId : partitionIds ) {
                List<LogicalAdapter> adapters = catalog.getSnapshot().alloc().getAdaptersByPartitionGroup( catalogTable.id, partitionId );

                for ( LogicalAdapter adapter : adapters ) {
                    if ( !adapterPlacements.containsKey( adapter.id ) ) {
                        adapterPlacements.put( adapter.id, new HashMap<>() );
                    }
                    List<AllocationColumn> placements = catalog.getSnapshot().alloc().getColumnPlacementsOnAdapterPerTable( adapter.id, catalogTable.id );
                    adapterPlacements.get( placements.get( 0 ).placementId ).put( partitionId, placements );
                }
            }
        }
        return adapterPlacements;
    }

}
