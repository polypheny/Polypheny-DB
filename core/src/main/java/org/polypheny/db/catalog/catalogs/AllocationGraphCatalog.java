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
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;

public interface AllocationGraphCatalog extends AllocationCatalog {


    /**
     * Adds a new placement on a given adapter for an existing graph.
     *
     * @param adapterId The id of the adapter on which the graph is added
     * @param graphId The id of the graph for which a new placement is added
     * @return The id of the new placement
     */
    public abstract long addGraphPlacement( int adapterId, long graphId );

    /**
     * Gets a collection of graph placements for a given adapter.
     *
     * @param adapterId The id of the adapter on which the placements are placed
     * @return The collection of graph placements
     */
    public abstract List<CatalogGraphPlacement> getGraphPlacements( int adapterId );

    /**
     * Deletes a specific graph placement for a given graph and adapter.
     *
     * @param adapterId The id of the adapter on which the placement is removed
     * @param graphId The id of the graph for which the placement is removed
     */
    public abstract void deleteGraphPlacement( int adapterId, long graphId );

    /**
     * Gets a specific placement for a graph on a given adapter.
     *
     * @param graphId The id of the graph
     * @param adapterId The id of the adapter on which the placement is placed
     * @return The placement matching the conditions
     */
    public abstract CatalogGraphPlacement getGraphPlacement( long graphId, int adapterId );

}
