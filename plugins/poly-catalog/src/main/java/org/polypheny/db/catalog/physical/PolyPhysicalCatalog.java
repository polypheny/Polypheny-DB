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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.polypheny.db.catalog.catalogs.PhysicalCatalog;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;

public class PolyPhysicalCatalog implements PhysicalCatalog {

    @Getter
    private final ConcurrentHashMap<Long, PhysicalEntity> physicals;


    public PolyPhysicalCatalog() {
        this( new ConcurrentHashMap<>() );
    }


    public PolyPhysicalCatalog( Map<Long, PhysicalEntity> physicals ) {
        this.physicals = new ConcurrentHashMap<>( physicals );

    }


    PhysicalEntity getPhysicalEntity( long id ) {
        return physicals.get( id );
    }


    @Override
    public void addPhysicalEntity( PhysicalEntity physicalEntity ) {
        physicals.put( physicalEntity.id, physicalEntity );
    }


    @Override
    public void addEntities( List<? extends PhysicalEntity> physicals ) {
        physicals.forEach( p -> this.physicals.put( p.id, p ) );
    }


    @Override
    public void deleteEntity( long id ) {
        physicals.remove( id );
    }


}
