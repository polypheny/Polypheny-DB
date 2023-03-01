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

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.With;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogObject;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;

@EqualsAndHashCode(callSuper = true)
@Value
@With
public class LogicalCollection extends LogicalEntity implements CatalogObject {

    private static final long serialVersionUID = -6490762948368178584L;

    @Getter
    public long id;
    public ImmutableList<Integer> placements;
    public String name;
    public long namespaceId;
    public EntityType entityType;
    public String physicalName;


    public LogicalCollection(
            long namespaceId,
            String namespaceName,
            long id,
            String name,
            @NonNull Collection<Integer> placements,
            EntityType type,
            String physicalName ) {
        super( id, name, namespaceId, namespaceName, EntityType.ENTITY, NamespaceType.DOCUMENT );
        this.id = id;
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


    public LogicalCollection addPlacement( int adapterId ) {
        List<Integer> placements = new ArrayList<>( this.placements );
        placements.add( adapterId );
        return new LogicalCollection( id, name, namespaceId, namespaceName, placements, EntityType.ENTITY, physicalName );
    }


    public LogicalCollection removePlacement( int adapterId ) {
        List<Integer> placements = this.placements.stream().filter( id -> id != adapterId ).collect( Collectors.toList() );
        return new LogicalCollection( id, name, namespaceId, namespaceName, placements, EntityType.ENTITY, physicalName );
    }





    @Override
    public Expression asExpression() {
        return Expressions.call( Catalog.CATALOG_EXPRESSION, "getCollection", Expressions.constant( id ) );
    }

}
