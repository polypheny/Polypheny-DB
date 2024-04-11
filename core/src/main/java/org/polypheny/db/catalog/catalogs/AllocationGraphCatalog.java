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

package org.polypheny.db.catalog.catalogs;

import java.util.Map;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.logistic.PartitionType;

public interface AllocationGraphCatalog extends AllocationCatalog {


    /**
     * Adds a new placement on a given adapter for an existing graph.
     *
     * @param graph
     * @param placementId
     * @param partitionId
     * @param adapterId The id of the adapter on which the graph is added
     * @return The id of the new placement
     */
    AllocationGraph addAllocation( LogicalGraph graph, long placementId, long partitionId, long adapterId );

    /**
     * Deletes a specific graph placement for a given graph and adapter.
     *
     * @param id
     */
    void deleteAllocation( long id );


    AllocationPlacement addPlacement( LogicalGraph graph, long adapterId );

    void removePlacement( long id );

    AllocationPartition addPartition( LogicalGraph graph, PartitionType partitionType, String name );

    void removePartition( long id );


    Map<Long, ? extends AllocationGraph> getGraphs();

}
