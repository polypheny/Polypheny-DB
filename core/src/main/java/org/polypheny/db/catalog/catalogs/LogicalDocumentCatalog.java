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
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.Pattern;

public interface LogicalDocumentCatalog extends LogicalCatalog {

    /**
     * Get the collection with the given id.
     *
     * @param collectionId The id of the collection
     * @return The requested collection
     */
    public abstract LogicalCollection getCollection( long collectionId );

    /**
     * Get a collection of collections which match the given naming pattern.
     *
     * @param namespaceId The id of the namespace to which the collection belongs
     * @param namePattern The naming pattern of the collection itself, null if all are matched
     * @return collection of collections matching conditions
     */
    public abstract List<LogicalCollection> getCollections( long namespaceId, Pattern namePattern );

    /**
     * Add a new collection with the given parameters.
     *
     * @param id ID of the collection to add, null if a new one needs to be generated
     * @param name The name of the collection
     * @param schemaId The id of the namespace to which the collection is added
     * @param currentUserId The user, which adds the collection
     * @param entity The type of entity of the collection
     * @param modifiable If the collection is modifiable
     * @return The id of the added collection
     */
    public abstract long addCollection( Long id, String name, long schemaId, int currentUserId, EntityType entity, boolean modifiable );


    /**
     * Delete a specific collection.
     *
     * @param id The id of the collection to delete
     */
    public abstract void deleteCollection( long id );

}
