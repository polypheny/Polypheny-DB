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
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.CatalogCollectionMapping;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.logistic.PlacementType;

public interface AllocationDocumentCatalog extends AllocationCatalog {

    /**
     * Added the required additional entities for the substitutions entities on different data models.
     *
     * @param schemaId The id of the namespace to which the collection belongs
     * @param name The name of the collection
     * @param stores The stores on which the collection was added
     * @param onlyPlacement If the substitution entities should be created fully or only the placements
     * @return The id of the mapping
     */
    public abstract long addCollectionLogistics( long schemaId, String name, List<DataStore> stores, boolean onlyPlacement ) throws GenericCatalogException;

    List<CatalogCollectionPlacement> getCollectionPlacementsByAdapter( long id );

    void addCollectionPlacement( long namespaceId, long adapterId, long id, PlacementType placementType );

    CatalogCollectionMapping getCollectionMapping( long id );

    void dropCollectionPlacement( long id, long adapterId );

    CatalogCollectionPlacement getCollectionPlacement( long id, long placementId );

}
