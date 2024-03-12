/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.type.PolyType;


public interface PartitionManager {

    String NULL_STRING = "null";

    /**
     * Returns the Index of the partition where to place the object
     */
    long getTargetPartitionId( LogicalTable table, PartitionProperty property, String columnValue );

    boolean probePartitionGroupDistributionChange( LogicalTable table, int storeId, long columnId, int threshold );

    Map<Long, List<AllocationColumn>> getRelevantPlacements( LogicalTable table, List<AllocationEntity> allocs, List<Long> excludedAdapters );

    List<List<String>> validateAdjustPartitionGroupSetup( List<List<String>> partitionGroupQualifiers, long numPartitionGroups, List<String> partitionGroupNames, LogicalColumn partitionColumn );

    /**
     * Returns all placements of catalogEntity and partitionIds
     *
     * @param table Table to look for placements
     * @param partitionIds List of all requested partitions ids
     * @return Returns map of AdapterId  {@code ->} [Map PartitionsId {@code ->}needed Columns Placements]
     */
    Map<Long, Map<Long, List<AllocationColumn>>> getAllPlacements( LogicalTable table, List<Long> partitionIds );

    int getNumberOfPartitionsPerGroup( int numberOfPartitions );

    boolean requiresUnboundPartitionGroup();

    boolean supportsColumnOfType( PolyType type );

    /**
     * Returns an instance of PartitionFunctionInfo specifying the available parameters of the partition function.
     */
    PartitionFunctionInfo getPartitionFunctionInfo();

}
