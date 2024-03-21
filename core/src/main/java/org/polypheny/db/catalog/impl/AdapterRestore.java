/*
 * Copyright 2019-2024 The Polypheny Project
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

import io.activej.serializer.annotations.SerializeRecord;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.prepare.Context;

@SerializeRecord
public record AdapterRestore(
        long adapterId,
        Map<Long, List<PhysicalEntity>> physicals,
        Map<Long, AllocationEntity> allocations) {

    public AdapterRestore(
            long adapterId,
            Map<Long, List<PhysicalEntity>> physicals,
            Map<Long, AllocationEntity> allocations ) {
        this.adapterId = adapterId;
        this.physicals = new ConcurrentHashMap<>( physicals );
        this.allocations = new ConcurrentHashMap<>( allocations );
    }


    public void activate( Adapter<?> adapter, Context context ) {
        physicals.forEach( ( allocId, physicals ) -> {
            AllocationEntity entity = allocations.get( allocId );
            switch ( entity.dataModel ) {
                case RELATIONAL -> adapter.restoreTable( entity.unwrap( AllocationTable.class ).orElseThrow(), physicals, context );
                case DOCUMENT -> adapter.restoreCollection( entity.unwrap( AllocationCollection.class ).orElseThrow(), physicals, context );
                case GRAPH -> adapter.restoreGraph( entity.unwrap( AllocationGraph.class ).orElseThrow(), physicals, context );
            }

        } );
    }

}
