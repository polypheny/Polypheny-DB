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

package org.polypheny.db.catalog.allocation;

import io.activej.serializer.BinarySerializer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.PolyCatalog;
import org.polypheny.db.catalog.Serializable;
import org.polypheny.db.catalog.catalogs.AllocationDocumentCatalog;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.logistic.PlacementType;

public class PolyAllocDocCatalog implements Serializable, AllocationDocumentCatalog {

    @Getter
    public final LogicalNamespace namespace;

    @Getter
    public ConcurrentHashMap<Long, AllocationCollection> collections;


    public PolyAllocDocCatalog( LogicalNamespace namespace ) {
        this.namespace = namespace;
    }


    @Getter
    public BinarySerializer<PolyAllocDocCatalog> serializer = Serializable.builder.get().build( PolyAllocDocCatalog.class );


    @Override
    public PolyAllocDocCatalog copy() {
        return deserialize( serialize(), PolyAllocDocCatalog.class );
    }


    @Override
    public long addCollectionLogistics( long schemaId, String name, List<DataStore> stores, boolean onlyPlacement ) throws GenericCatalogException {
        return 0;
    }


    @Override
    public void addCollectionPlacement( long namespaceId, long adapterId, long id, PlacementType placementType ) {

    }


    @Override
    public void dropCollectionPlacement( long id, long adapterId ) {

    }


}
