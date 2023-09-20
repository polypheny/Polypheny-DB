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


import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.Namespace;

@Value
@NonFinal
@Slf4j
public abstract class StoreCatalog {

    @Serialize
    public long adapterId;
    IdBuilder idBuilder = IdBuilder.getInstance();
    public ConcurrentMap<Long, Namespace> namespaces;
    @Serialize
    ConcurrentMap<Long, PhysicalEntity> physicals;

    @Serialize
    ConcurrentMap<Long, AllocationEntity> allocations;

    ConcurrentMap<Long, Set<Long>> allocToPhysicals;


    public StoreCatalog(
            @Deserialize("adapterId") long adapterId ) {
        this( adapterId, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>() );
    }


    public StoreCatalog(
            long adapterId,
            @Deserialize("namespace") Map<Long, Namespace> namespaces,
            @Deserialize("tables") Map<Long, ? extends PhysicalEntity> entities,
            @Deserialize("allocations") Map<Long, ? extends AllocationEntity> allocations,
            @Deserialize("allocRelations") Map<Long, Set<Long>> allocToPhysicals ) {
        this.adapterId = adapterId;
        this.namespaces = new ConcurrentHashMap<>( namespaces );
        this.physicals = new ConcurrentHashMap<>( entities );
        this.allocations = new ConcurrentHashMap<>( allocations );
        this.allocToPhysicals = new ConcurrentHashMap<>( allocToPhysicals );
    }


    public Expression asExpression() {
        return Expressions.call( Catalog.CATALOG_EXPRESSION, "getStoreSnapshot", Expressions.constant( adapterId ) );
    }


    public void addNamespace( long namespaceId, Namespace namespace ) {
        this.namespaces.put( namespaceId, namespace );
    }


    public void removeNamespace( long namespaceId ) {
        this.namespaces.remove( namespaceId );
    }


    public Namespace getNamespace( long id ) {
        return namespaces.get( id );
    }


    public PhysicalEntity getPhysical( long id ) {
        return physicals.get( id );
    }


    public AllocationEntity getAlloc( long id ) {
        return allocations.get( id );
    }


    public List<PhysicalEntity> getPhysicalsFromAllocs( long allocId ) {
        Set<Long> entities = allocToPhysicals.get( allocId );
        if ( entities == null ) {
            return null;
        }
        return entities.stream().map( physicals::get ).collect( Collectors.toList() );
    }


    public void addPhysical( AllocationEntity allocation, PhysicalEntity... physicalEntities ) {
        Set<Long> physicals = Arrays.stream( physicalEntities ).map( p -> p.id ).collect( Collectors.toSet() );
        if ( allocToPhysicals.containsKey( allocation.id ) ) {
            physicals.addAll( allocToPhysicals.get( allocation.id ) );
            log.warn( "should only overwrite" );
        }

        allocToPhysicals.put( allocation.id, physicals );
        allocations.put( allocation.id, allocation );
        List.of( physicalEntities ).forEach( p -> this.physicals.put( p.id, p ) );
    }


    public void replacePhysical( PhysicalEntity... physicalEntities ) {
        AllocationEntity alloc = getAlloc( physicalEntities[0].allocationId );
        if ( alloc == null ) {
            throw new GenericRuntimeException( "Error on handling store" );
        }
        addPhysical( alloc, physicalEntities );
    }


    public void removePhysical( long allocId ) {
        allocations.remove( allocId );
        if ( !allocToPhysicals.containsKey( allocId ) ) {
            return;
        }
        Set<Long> physicals = allocToPhysicals.get( allocId );
        physicals.forEach( allocToPhysicals::remove );
    }

}
