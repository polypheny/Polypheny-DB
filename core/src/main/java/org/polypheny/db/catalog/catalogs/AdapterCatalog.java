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

package org.polypheny.db.catalog.catalogs;


import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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
import org.polypheny.db.catalog.entity.physical.PhysicalField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.util.Pair;

@Value
@NonFinal
@Slf4j
@SerializeClass(subclasses = { DocAdapterCatalog.class, RelAdapterCatalog.class, GraphAdapterCatalog.class })
public abstract class AdapterCatalog {

    IdBuilder idBuilder = IdBuilder.getInstance();

    @Serialize
    public long adapterId;

    @Serialize
    public ConcurrentMap<Long, Namespace> namespaces;

    @Serialize
    public ConcurrentMap<Long, PhysicalEntity> physicals;

    @Serialize
    public ConcurrentMap<Long, AllocationEntity> allocations;

    @Serialize
    public ConcurrentMap<Long, SortedSet<Long>> allocToPhysicals;

    @Serialize
    public ConcurrentMap<Pair<Long, Long>, PhysicalField> fields; // allocId, fieldId


    public AdapterCatalog( long adapterId ) {
        this( adapterId, Map.of(), Map.of(), Map.of(), Map.of(), Map.of() );
    }


    public AdapterCatalog(
            long adapterId,
            Map<Long, Namespace> namespaces,
            Map<Long, PhysicalEntity> physicals,
            Map<Long, AllocationEntity> allocations,
            Map<Long, SortedSet<Long>> allocToPhysicals,
            Map<Pair<Long, Long>, PhysicalField> fields ) {
        this.adapterId = adapterId;
        this.namespaces = new ConcurrentHashMap<>( namespaces );
        this.physicals = new ConcurrentHashMap<>( physicals );
        this.allocations = new ConcurrentHashMap<>( allocations );
        this.allocToPhysicals = new ConcurrentHashMap<>( allocToPhysicals );
        this.fields = new ConcurrentHashMap<>( fields );
    }


    public Expression asExpression() {
        return Expressions.call( Catalog.CATALOG_EXPRESSION, "getAdapterCatalog", Expressions.constant( adapterId ) );
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
        return entities.stream().map( physicals::get ).toList();
    }


    public void addPhysical( AllocationEntity allocation, PhysicalEntity... physicalEntities ) {
        SortedSet<Long> physicals = Arrays.stream( physicalEntities ).sorted().map( p -> p.id ).collect( Collectors.toCollection( TreeSet::new ) );

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


    public void removeAllocAndPhysical( long allocId ) {
        allocations.remove( allocId );
        if ( !allocToPhysicals.containsKey( allocId ) ) {
            return;
        }
        Set<Long> physicals = allocToPhysicals.get( allocId );
        allocToPhysicals.remove( allocId );
        for ( long physicalId : physicals ) {
            PhysicalEntity physical = this.physicals.get( physicalId );
            // we might have created substitution allocations, which we want to remove as well
            if ( physical == null ) {
                continue;
            }
            allocToPhysicals.remove( physical.allocationId );
            allocations.remove( physical.allocationId );

            // remove fields
            List<PhysicalField> removeFields = fields.entrySet().stream().filter( f -> f.getKey().getKey() == physical.allocationId ).map( Entry::getValue ).toList();
            removeFields.forEach( field -> fields.remove( Pair.of( field.allocId, field.id ) ) );
        }
        physicals.forEach( this.physicals::remove );
    }


    public abstract void renameLogicalColumn( long id, String newFieldName );

}
