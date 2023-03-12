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

import java.util.Map;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;

public interface AllocationGraphCatalog extends AllocationCatalog {


    /**
     * Adds a new placement on a given adapter for an existing graph.
     *
     * @param adapterId The id of the adapter on which the graph is added
     * @param graphId The id of the graph for which a new placement is added
     * @return The id of the new placement
     */
    public abstract long addGraphPlacement( long adapterId, long graphId );

    /**
     * Deletes a specific graph placement for a given graph and adapter.
     *
     * @param adapterId The id of the adapter on which the placement is removed
     * @param graphId The id of the graph for which the placement is removed
     */
    public abstract void deleteGraphPlacement( long adapterId, long graphId );


    Map<Long, ? extends AllocationGraph> getGraphs();

}
