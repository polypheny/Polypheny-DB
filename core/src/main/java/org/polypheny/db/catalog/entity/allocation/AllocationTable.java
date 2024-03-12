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

package org.polypheny.db.catalog.entity.allocation;

import io.activej.serializer.annotations.Deserialize;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;

@EqualsAndHashCode(callSuper = true)
@Value
@SuperBuilder(toBuilder = true)
public class AllocationTable extends AllocationEntity {


    public AllocationTable(
            @Deserialize("id") long id,
            @Deserialize("placementId") long placementId,
            @Deserialize("partitionId") long partitionId,
            @Deserialize("logicalId") long logicalId,
            @Deserialize("namespaceId") long namespaceId,
            @Deserialize("adapterId") long adapterId ) {
        super( id, placementId, partitionId, logicalId, namespaceId, adapterId, DataModel.RELATIONAL );
    }


    @Override
    public AlgDataType getTupleType() {
        final AlgDataTypeFactory.Builder fieldInfo = AlgDataTypeFactory.DEFAULT.builder();

        for ( AllocationColumn column : getColumns().stream().sorted( Comparator.comparingInt( a -> a.position ) ).toList() ) {
            LogicalColumn lColumn = Catalog.snapshot().rel().getColumn( column.columnId ).orElseThrow();
            AlgDataType sqlType = column.getAlgDataType();
            fieldInfo.add( column.columnId, lColumn.name, lColumn.name, sqlType ).nullable( lColumn.nullable );
        }

        return AlgDataTypeImpl.proto( fieldInfo.build() ).apply( AlgDataTypeFactory.DEFAULT );
    }


    @Override
    public PolyValue[] getParameterArray() {
        return new PolyString[0];
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( Catalog.CATALOG_EXPRESSION, "getAllocTable", Expressions.constant( id ) );
    }


    public Map<Long, String> getColumnNames() {
        return getColumns().stream().collect( Collectors.toMap( c -> c.columnId, AllocationColumn::getLogicalColumnName ) );
    }


    public List<AllocationColumn> getColumns() {
        return Catalog.snapshot().alloc().getColumns( placementId ).stream().sorted( Comparator.comparingLong( a -> a.position ) ).collect( Collectors.toList() );
    }


    public String getNamespaceName() {
        return Catalog.getInstance().getSnapshot().getNamespace( namespaceId ).orElseThrow().name;
    }


    public List<Long> getColumnIds() {
        return getColumns().stream().map( c -> c.columnId ).collect( Collectors.toList() );
    }


    @Override
    public Entity withName( String name ) {
        return toBuilder().name( name ).build();
    }

}
