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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.polypheny.db.catalog.PusherMap;
import org.polypheny.db.catalog.catalogs.PhysicalCatalog;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;

public class PolyPhysicalCatalog implements PhysicalCatalog {

    private final PusherMap<Long, PhysicalEntity<?>> physicals;

    private final ConcurrentHashMap<Long, PhysicalEntity<?>> logicalPhysical;
    private final ConcurrentHashMap<Long, List<PhysicalEntity<?>>> physicalsPerAdapter;


    public PolyPhysicalCatalog() {
        this( new ConcurrentHashMap<>() );
    }


    public PolyPhysicalCatalog( Map<Long, PhysicalEntity<?>> physicals ) {
        this.physicals = new PusherMap<>( physicals );

        this.logicalPhysical = new ConcurrentHashMap<>();
        this.physicals.addRowConnection( this.logicalPhysical, ( k, v ) -> v.logical.id, ( k, v ) -> v );
        this.physicalsPerAdapter = new ConcurrentHashMap<>();
        this.physicals.addConnection( m -> {
            physicalsPerAdapter.clear();
            m.forEach( ( k, v ) -> {
                if ( physicalsPerAdapter.containsKey( v.adapterId ) ) {
                    physicalsPerAdapter.get( v.adapterId ).add( v );
                } else {
                    physicalsPerAdapter.put( v.adapterId, new ArrayList<>( List.of( v ) ) );
                }
            } );
        } );
    }


    @Override
    public List<PhysicalEntity<?>> getPhysicalsOnAdapter( long id ) {
        return physicalsPerAdapter.get( id );
    }


    @Override
    public PhysicalEntity<?> getPhysicalEntity( long id ) {
        return physicals.get( id );
    }


    @Override
    public void addPhysicalEntity( PhysicalEntity<?> physicalEntity ) {
        physicals.put( physicalEntity.id, physicalEntity );
    }


    @Override
    public PhysicalEntity<?> getFromLogical( long id ) {
        return logicalPhysical.get( id );
    }

}
