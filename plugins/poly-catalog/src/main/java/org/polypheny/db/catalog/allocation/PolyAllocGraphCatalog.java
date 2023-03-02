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
import lombok.Getter;
import org.polypheny.db.catalog.Serializable;
import org.polypheny.db.catalog.catalogs.AllocationGraphCatalog;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;

public class PolyAllocGraphCatalog implements Serializable, AllocationGraphCatalog {

    @Getter
    public BinarySerializer<PolyAllocGraphCatalog> serializer = Serializable.builder.get().build( PolyAllocGraphCatalog.class );


    @Override
    public long addGraphPlacement( long adapterId, long graphId ) {
        return 0;
    }


    @Override
    public List<CatalogGraphPlacement> getGraphPlacements( long adapterId ) {
        return null;
    }


    @Override
    public void deleteGraphPlacement( long adapterId, long graphId ) {

    }


    @Override
    public CatalogGraphPlacement getGraphPlacement( long graphId, long adapterId ) {
        return null;
    }


    @Override
    public PolyAllocGraphCatalog copy() {
        return deserialize( serialize(), PolyAllocGraphCatalog.class );
    }


    @Override
    public List<? extends AllocationEntity<?>> getAllocationsOnAdapter( long id ) {
        return null;
    }

}
