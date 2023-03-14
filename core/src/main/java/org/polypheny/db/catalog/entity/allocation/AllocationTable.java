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

package org.polypheny.db.catalog.entity.allocation;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.PlacementType;

@EqualsAndHashCode(callSuper = true)
@Value
public class AllocationTable extends AllocationEntity {

    @Serialize
    public List<CatalogColumnPlacement> placements;


    public AllocationTable(
            @Deserialize("id") long id,
            @Deserialize("logicalId") long logicalId,
            @Deserialize("namespaceId") long namespaceId,
            @Deserialize("adapterId") long adapterId,
            @Deserialize("placements") List<CatalogColumnPlacement> placements ) {
        super( id, logicalId, namespaceId, adapterId, NamespaceType.RELATIONAL );
        this.placements = placements;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( Catalog.CATALOG_EXPRESSION, "getAllocTable", Expressions.constant( id ) );
    }


    public Map<Long, String> getColumnNames() {
        return getColumns().values().stream().collect( Collectors.toMap( c -> c.id, c -> c.name ) );
    }


    public Map<Long, LogicalColumn> getColumns() {
        return Catalog.getInstance().getSnapshot().getRelSnapshot( namespaceId ).getColumns( logicalId ).stream().collect( Collectors.toMap( c -> c.id, c -> c ) );
    }


    public String getNamespaceName() {
        return Catalog.getInstance().getSnapshot().getNamespace( namespaceId ).name;
    }


    public AllocationTable withAddedColumn( long columnId, PlacementType placementType, String physicalSchemaName, String physicalTableName, String physicalColumnName, int position ) {
        List<CatalogColumnPlacement> placements = new ArrayList<>( this.placements );
        placements.add( new CatalogColumnPlacement( namespaceId, id, columnId, adapterId, placementType, physicalSchemaName, physicalColumnName, position ) );

        return new AllocationTable( id, logicalId, namespaceId, adapterId, placements );
    }


    public AllocationTable withRemovedColumn( long columnId ) {
        List<CatalogColumnPlacement> placements = new ArrayList<>( this.placements );
        return new AllocationTable( id, logicalId, namespaceId, adapterId, placements.stream().filter( p -> p.columnId != columnId ).collect( Collectors.toList() ) );
    }


    public Map<Long, AlgDataType> getColumnTypes() {
        return null;
    }


    public Map<String, Long> getColumnNamesId() {
        return getColumnNames().entrySet().stream().collect( Collectors.toMap( Entry::getValue, Entry::getKey ) );
    }


    public List<Long> getColumnOrder() {
        List<CatalogColumnPlacement> columns = new ArrayList<>( placements );
        columns.sort( ( a, b ) -> Math.toIntExact( a.physicalPosition - b.physicalPosition ) );

        return columns.stream().map( c -> c.columnId ).collect( Collectors.toList() );
    }

}
