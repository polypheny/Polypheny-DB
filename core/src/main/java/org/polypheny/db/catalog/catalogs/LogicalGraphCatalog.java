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

import io.activej.serializer.annotations.SerializeClass;
import java.util.Map;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.impl.logical.GraphCatalog;

@SerializeClass(subclasses = { GraphCatalog.class })
public interface LogicalGraphCatalog extends LogicalCatalog {

    /**
     * Add a new alias for a given graph.
     *
     * @param graphId The id of the graph to which the alias is added
     * @param alias The alias to add
     * @param ifNotExists If the alias should only be added if it not already exists
     */
    void addGraphAlias( long graphId, String alias, boolean ifNotExists );

    /**
     * Removes a given alias for a specific graph.
     *
     * @param graphId The id of the graph for which the alias is removed
     * @param alias The alias to remove
     * @param ifExists If the alias should only be removed if it exists
     */
    void removeGraphAlias( long graphId, String alias, boolean ifExists );

    /**
     * Adds a new graph to the catalog, on the same layer as schema in relational.
     *
     * @param id
     * @param name The name of the graph
     * @param modifiable If the graph is modifiable
     * @return The id of the newly added graph
     */
    LogicalGraph addGraph( long id, String name, boolean modifiable );

    /**
     * Deletes an existing graph.
     *
     * @param id The id of the graph to delete
     */
    void deleteGraph( long id );


    Map<Long, LogicalGraph> getGraphs();

}
