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

package org.polypheny.db.catalog.entity.logical;

import com.drew.lang.annotations.NotNull;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogObject;

@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = false)
public class LogicalGraph extends CatalogEntity implements CatalogObject, Comparable<LogicalGraph>, Logical {

    private static final long serialVersionUID = 7343856827901459672L;

    public final long databaseId;
    public final ImmutableList<Integer> placements;
    public final int ownerId;
    public final boolean modifiable;

    public final boolean caseSensitive;


    public LogicalGraph( long databaseId, long id, String name, int ownerId, boolean modifiable, @NonNull Collection<Integer> placements, boolean caseSensitive ) {
        super( id, name, EntityType.ENTITY, NamespaceType.GRAPH );
        this.ownerId = ownerId;
        this.databaseId = databaseId;
        this.modifiable = modifiable;
        this.placements = ImmutableList.copyOf( placements );
        this.caseSensitive = caseSensitive;
    }


    public LogicalGraph( LogicalGraph graph ) {
        this( graph.databaseId, graph.id, graph.name, graph.ownerId, graph.modifiable, graph.placements, graph.caseSensitive );
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }


    @Override
    public int compareTo( @NotNull LogicalGraph o ) {
        if ( o != null ) {
            return (int) (this.id - o.id);
        }
        return -1;
    }


    public LogicalGraph addPlacement( int adapterId ) {
        List<Integer> placements = new ArrayList<>( this.placements );
        placements.add( adapterId );
        return toBuilder().placements( ImmutableList.copyOf( placements ) ).build();
    }


    public LogicalGraph removePlacement( int adapterId ) {
        return toBuilder().placements( ImmutableList.copyOf( placements.stream().filter( i -> i != adapterId ).collect( Collectors.toList() ) ) ).build();
    }


}
