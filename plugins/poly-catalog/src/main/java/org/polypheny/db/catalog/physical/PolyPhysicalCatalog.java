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

package org.polypheny.db.catalog.physical;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.polypheny.db.catalog.catalogs.PhysicalCatalog;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.schema.Namespace;

public class PolyPhysicalCatalog implements PhysicalCatalog {

    @Getter
    private final ConcurrentHashMap<Long, PhysicalEntity> physicals;

    @Getter
    private final ConcurrentHashMap<Long, Namespace> namespaces;


    public PolyPhysicalCatalog() {
        this( new ConcurrentHashMap<>(), new HashMap<>() );
    }


    public PolyPhysicalCatalog( Map<Long, PhysicalEntity> physicals, Map<Long, Namespace> namespaces ) {
        this.physicals = new ConcurrentHashMap<>( physicals );
        this.namespaces = new ConcurrentHashMap<>( namespaces );
    }


    PhysicalEntity getPhysicalEntity( long id ) {
        return physicals.get( id );
    }


    @Override
    public void addEntities( List<? extends PhysicalEntity> physicals ) {
        physicals.forEach( p -> this.physicals.put( p.id, p ) );
    }


    @Override
    public void deleteEntity( long id ) {
        physicals.remove( id );
    }


    @Override
    public void addNamespace( long adapterId, Namespace currentSchema ) {
        namespaces.put( adapterId, currentSchema );
    }


}
