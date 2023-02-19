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
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.NCatalog;
import org.polypheny.db.catalog.Serializable;

public class AllocationCatalog implements NCatalog, Serializable {


    @Getter
    public BinarySerializer<AllocationCatalog> serializer = Serializable.builder.get().build( AllocationCatalog.class );


    @Serialize
    public final Map<Long, HorizontalPartition> horizontals; // "rows" 1,1,1;2,2,2 -> 1,1,1 + 2,2,2

    @Serialize
    public final Map<Long, VerticalPartition> verticals; // "split-placements" a,b,c -> a,b + (a,)c


    public final IdBuilder idBuilder = new IdBuilder();


    public AllocationCatalog() {
        this( new HashMap<>(), new HashMap<>() );
    }


    public AllocationCatalog(
            @Deserialize("horizontals") Map<Long, HorizontalPartition> horizontals,
            @Deserialize("verticals") Map<Long, VerticalPartition> verticals ) {
        this.horizontals = new ConcurrentHashMap<>( horizontals );
        this.verticals = new ConcurrentHashMap<>( verticals );
    }


    @Override
    public void commit() {

    }


    @Override
    public void rollback() {

    }


    @Override
    public boolean hasUncommittedChanges() {
        return false;
    }


    @Override
    public NamespaceType getType() {
        return null;
    }


    @Override
    public AllocationCatalog copy() {
        return deserialize( serialize(), AllocationCatalog.class );
    }


    public long addVerticalPlacement( long logicalId ) {
        long id = idBuilder.getNewVerticalId();
        verticals.put( id, new VerticalPartition( id, logicalId ) );
        return id;
    }


    public long addHorizontalPlacement( long logicalId ) {
        long id = idBuilder.getNewHorizontalId();

        horizontals.put( id, new HorizontalPartition( id, logicalId ) );

        return id;
    }

}
