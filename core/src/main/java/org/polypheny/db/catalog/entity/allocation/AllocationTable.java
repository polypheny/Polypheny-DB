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
import org.polypheny.db.catalog.entity.AllocationColumn;
import org.polypheny.db.catalog.logistic.NamespaceType;

@EqualsAndHashCode(callSuper = true)
@Value
public class AllocationTable extends AllocationEntity {


    public AllocationTable(
            @Deserialize("id") long id,
            @Deserialize("logicalId") long logicalId,
            @Deserialize("namespaceId") long namespaceId,
            @Deserialize("adapterId") long adapterId ) {
        super( id, logicalId, namespaceId, adapterId, NamespaceType.RELATIONAL );
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
        return getColumns().values().stream().collect( Collectors.toMap( c -> c.columnId, AllocationColumn::getLogicalColumnName ) );
    }


    public Map<Long, AllocationColumn> getColumns() {
        return Catalog.snapshot().alloc().getColumns( id ).stream().collect( Collectors.toMap( c -> c.columnId, c -> c ) );
    }


    public String getNamespaceName() {
        return Catalog.getInstance().getSnapshot().getNamespace( namespaceId ).name;
    }


    public Map<Long, AlgDataType> getColumnTypes() {
        return getColumns().values().stream().collect( Collectors.toMap( c -> c.columnId, AllocationColumn::getAlgDataType ) );
    }


    public Map<String, Long> getColumnNamesId() {
        return getColumnNames().entrySet().stream().collect( Collectors.toMap( Entry::getValue, Entry::getKey ) );
    }


    public List<Long> getColumnOrder() {
        List<AllocationColumn> columns = new ArrayList<>( getColumns().values() );
        columns.sort( ( a, b ) -> Math.toIntExact( a.position - b.position ) );

        return columns.stream().map( c -> c.columnId ).collect( Collectors.toList() );
    }

}
