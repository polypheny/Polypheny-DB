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

package org.polypheny.db.catalog.impl;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;

public class AdapterRestore {

    @Serialize
    public final long adapterId;

    @Serialize
    public final Map<Long, List<PhysicalEntity>> physicals;

    @Serialize
    public final Map<Long, AllocationEntity> allocations;


    public AdapterRestore( long adapterId ) {
        this( adapterId, Map.of(), Map.of() );
    }


    public AdapterRestore(
            @Deserialize("adapterId") long adapterId,
            @Deserialize("physicals") Map<Long, List<PhysicalEntity>> physicals,
            @Deserialize("allocations") Map<Long, AllocationEntity> allocations ) {
        this.adapterId = adapterId;
        this.physicals = new ConcurrentHashMap<>( physicals );
        this.allocations = new ConcurrentHashMap<>( allocations );
    }


    private List<PhysicalEntity> normalize( List<PhysicalEntity> physicals ) {
        return physicals.stream().map( PhysicalEntity::normalize ).collect( Collectors.toList() );
    }


    public void addPhysicals( AllocationEntity allocation, List<PhysicalEntity> physicals ) {
        this.physicals.put( allocation.id, normalize( physicals ) );
        this.allocations.put( allocation.id, allocation );
    }


    public void activate( Adapter<?> adapter ) {
        physicals.forEach( ( allocId, physicals ) -> {
            AllocationEntity entity = allocations.get( allocId );
            switch ( entity.dataModel ) {

                case RELATIONAL:
                    adapter.restoreTable( entity.unwrap( AllocationTable.class ).orElseThrow(), physicals );
                    break;
                case DOCUMENT:
                    adapter.restoreCollection( entity.unwrap( AllocationCollection.class ).orElseThrow(), physicals );
                    break;
                case GRAPH:
                    adapter.restoreGraph( entity.unwrap( AllocationGraph.class ).orElseThrow(), physicals );
                    break;
            }

        } );
    }

}
