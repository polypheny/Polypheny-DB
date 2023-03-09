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
import java.util.Map;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;

public interface LogicalGraphCatalog extends LogicalCatalog {

    /**
     * Add a new alias for a given graph.
     *
     * @param graphId The id of the graph to which the alias is added
     * @param alias The alias to add
     * @param ifNotExists If the alias should only be added if it not already exists
     */
    public abstract void addGraphAlias( long graphId, String alias, boolean ifNotExists );

    /**
     * Removes a given alias for a specific graph.
     *
     * @param graphId The id of the graph for which the alias is removed
     * @param alias The alias to remove
     * @param ifExists If the alias should only be removed if it exists
     */
    public abstract void removeGraphAlias( long graphId, String alias, boolean ifExists );

    /**
     * Adds a new graph to the catalog, on the same layer as schema in relational.
     *
     * @param name The name of the graph
     * @param stores The datastores on which the graph is placed
     * @param modifiable If the graph is modifiable
     * @param ifNotExists If the task fails when the graph already exists
     * @param replace If the graph should replace an existing one
     * @return The id of the newly added graph
     */
    public abstract long addGraph( String name, List<DataStore> stores, boolean modifiable, boolean ifNotExists, boolean replace );

    /**
     * Deletes an existing graph.
     *
     * @param id The id of the graph to delete
     */
    public abstract void deleteGraph( long id );


    /**
     * Additional operations for the creation of a graph entity.
     *
     * @param id The predefined id of the already added graph
     * @param stores The stores on which the graph was placed
     * @param onlyPlacement If the substitution only creates the placements and not the entites
     */
    public abstract void addGraphLogistics( long id, List<DataStore> stores, boolean onlyPlacement ) throws GenericCatalogException, UnknownTableException, UnknownColumnException;


    Map<Long, LogicalGraph> getGraphs();

}
