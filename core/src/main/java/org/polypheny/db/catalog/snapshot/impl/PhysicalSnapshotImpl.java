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

package org.polypheny.db.catalog.snapshot.impl;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Value;
import org.polypheny.db.catalog.catalogs.PhysicalCatalog;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalGraph;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.snapshot.PhysicalSnapshot;
import org.polypheny.db.util.Pair;

@Value
public class PhysicalSnapshotImpl implements PhysicalSnapshot {

    ImmutableMap<Long, PhysicalEntity> entities;

    ImmutableMap<Pair<Long, Long>, PhysicalEntity> adapterLogicalEntity;
    ImmutableMap<Long, List<PhysicalEntity>> adapterPhysicals;

    ImmutableMap<Long, List<PhysicalEntity>> logicalToPhysicals;

    ImmutableMap<Long, List<PhysicalEntity>> allocToPhysicals;


    public PhysicalSnapshotImpl( Map<Long, PhysicalCatalog> physicalCatalogs ) {
        this.entities = ImmutableMap.copyOf( physicalCatalogs.values().stream().flatMap( c -> c.getPhysicals().entrySet().stream() ).collect( Collectors.toMap( Entry::getKey, Entry::getValue ) ) );
        this.adapterLogicalEntity = buildAdapterLogicalEntity();
        this.adapterPhysicals = buildAdapterPhysicals();
        this.logicalToPhysicals = buildLogicalToPhysicals();
        this.allocToPhysicals = buildAllocToPhysicals();
    }


    private ImmutableMap<Long, List<PhysicalEntity>> buildAllocToPhysicals() {
        Map<Long, List<PhysicalEntity>> map = new HashMap<>();
        this.entities.forEach( ( k, v ) -> {
            if ( !map.containsKey( v.allocationId ) ) {
                map.put( v.allocationId, new ArrayList<>() );
            }
            map.get( v.allocationId ).add( v );
        } );

        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, List<PhysicalEntity>> buildLogicalToPhysicals() {
        Map<Long, List<PhysicalEntity>> map = new HashMap<>();
        this.entities.forEach( ( k, v ) -> {
            if ( !map.containsKey( v.logicalId ) ) {
                map.put( v.logicalId, new ArrayList<>() );
            }
            map.get( v.logicalId ).add( v );
        } );

        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, List<PhysicalEntity>> buildAdapterPhysicals() {
        Map<Long, List<PhysicalEntity>> map = new HashMap<>();
        this.entities.forEach( ( k, v ) -> {
            if ( !map.containsKey( v.adapterId ) ) {
                map.put( v.adapterId, new ArrayList<>() );
            }
            map.get( v.adapterId ).add( v );
        } );
        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Pair<Long, Long>, PhysicalEntity> buildAdapterLogicalEntity() {
        Map<Pair<Long, Long>, PhysicalEntity> map = new HashMap<>();
        this.entities.forEach( ( k, v ) -> map.put( Pair.of( v.adapterId, v.logicalId ), v ) );
        return ImmutableMap.copyOf( map );
    }


    @Override
    public PhysicalTable getPhysicalTable( long id ) {
        if ( entities.get( id ) != null ) {
            return entities.get( id ).unwrap( PhysicalTable.class );
        }
        return null;
    }


    @Override
    public PhysicalTable getPhysicalTable( long logicalId, long adapterId ) {
        return adapterLogicalEntity.get( Pair.of( adapterId, logicalId ) ).unwrap( PhysicalTable.class );
    }


    @Override
    public PhysicalCollection getPhysicalCollection( long id ) {
        return entities.get( id ).unwrap( PhysicalCollection.class );
    }


    @Override
    public PhysicalCollection getPhysicalCollection( long logicalId, long adapterId ) {
        return adapterLogicalEntity.get( Pair.of( adapterId, logicalId ) ).unwrap( PhysicalCollection.class );
    }


    @Override
    public PhysicalGraph getPhysicalGraph( long id ) {
        return entities.get( id ).unwrap( PhysicalGraph.class );
    }


    @Override
    public PhysicalGraph getPhysicalGraph( long logicalId, long adapterId ) {
        return adapterLogicalEntity.get( Pair.of( adapterId, logicalId ) ).unwrap( PhysicalGraph.class );
    }


    @Override
    public List<PhysicalEntity> getPhysicalsOnAdapter( long adapterId ) {
        return adapterPhysicals.get( adapterId );
    }


    @Override
    public PhysicalEntity getPhysicalEntity( long id ) {
        return entities.get( id );
    }


    @Override
    public List<PhysicalEntity> fromLogical( long id ) {
        return null;
    }


    @Override
    public List<PhysicalEntity> fromAlloc( long id ) {
        return allocToPhysicals.get( id );
    }


}
