/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.catalog.entity;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.EntityType;


public class CatalogCollection implements CatalogObject {

    private static final long serialVersionUID = -6490762948368178584L;

    public final long id;
    public final ImmutableList<Integer> placements;
    public final String name;
    public final long databaseId;
    public final long namespaceId;
    public final EntityType entityType;
    public final String physicalName;


    public CatalogCollection( long databaseId, long namespaceId, long id, String name, @NonNull Collection<Integer> placements, EntityType type, String physicalName ) {
        this.id = id;
        this.databaseId = databaseId;
        this.namespaceId = namespaceId;
        this.name = name;
        this.placements = ImmutableList.copyOf( placements );
        this.entityType = type;
        this.physicalName = physicalName;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }


    public CatalogCollection addPlacement( int adapterId ) {
        List<Integer> placements = new ArrayList<>( this.placements );
        placements.add( adapterId );
        return new CatalogCollection( databaseId, namespaceId, id, name, placements, EntityType.ENTITY, physicalName );
    }


    public CatalogCollection removePlacement( int adapterId ) {
        List<Integer> placements = this.placements.stream().filter( id -> id != adapterId ).collect( Collectors.toList() );
        return new CatalogCollection( databaseId, namespaceId, id, name, placements, EntityType.ENTITY, physicalName );
    }


    @SneakyThrows
    public String getNamespaceName() {
        return Catalog.getInstance().getSchema( namespaceId ).name;
    }


    public CatalogCollection setPhysicalName( String physicalCollectionName ) {
        return new CatalogCollection( databaseId, namespaceId, id, name, placements, entityType, physicalCollectionName );
    }


}
