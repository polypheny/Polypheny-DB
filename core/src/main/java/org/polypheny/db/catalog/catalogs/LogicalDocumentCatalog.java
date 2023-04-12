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
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.logistic.EntityType;

public interface LogicalDocumentCatalog extends LogicalCatalog {

    /**
     * Add a new collection with the given parameters.
     *
     * @param id ID of the collection to add, null if a new one needs to be generated
     * @param name The name of the collection
     * @param entity The type of entity of the collection
     * @param modifiable If the collection is modifiable
     * @return The id of the added collection
     */
    long addCollection( Long id, String name, EntityType entity, boolean modifiable );


    /**
     * Delete a specific collection.
     *
     * @param id The id of the collection to delete
     */
    void deleteCollection( long id );


    Map<Long, LogicalCollection> getCollections();

}
