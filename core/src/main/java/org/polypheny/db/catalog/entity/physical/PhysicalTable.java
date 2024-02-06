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

package org.polypheny.db.catalog.entity.physical;

import com.google.common.collect.ImmutableList;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.type.entity.PolyValue;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
@SuperBuilder(toBuilder = true)
public class PhysicalTable extends PhysicalEntity {


    @Serialize
    public ImmutableList<PhysicalColumn> columns;


    public PhysicalTable(
            @Deserialize("id") long id,
            @Deserialize("allocationId") long allocationId,
            @Deserialize("logicalId") long logicalId,
            @Deserialize("name") String name,
            @Deserialize("columns") List<PhysicalColumn> columns,
            @Deserialize("namespaceId") long namespaceId,
            @Deserialize("namespaceName") String namespaceName,
            @Deserialize("uniqueFieldIds") List<Long> uniqueFieldIds,
            @Deserialize("adapterId") long adapterId ) {
        super( id, allocationId, logicalId, name, namespaceId, namespaceName, uniqueFieldIds, DataModel.RELATIONAL, adapterId );
        this.columns = ImmutableList.copyOf( columns.stream().sorted( Comparator.comparingInt( a -> a.position ) ).collect( Collectors.toList() ) );
    }


    @Override
    public AlgDataType getRowType() {
        return buildProto().apply( AlgDataTypeFactory.DEFAULT );
    }


    public AlgProtoDataType buildProto() {
        final AlgDataTypeFactory.Builder fieldInfo = AlgDataTypeFactory.DEFAULT.builder();

        for ( PhysicalColumn column : columns.stream().sorted( Comparator.comparingInt( a -> a.position ) ).toList() ) {
            AlgDataType sqlType = column.getAlgDataType( AlgDataTypeFactory.DEFAULT );
            fieldInfo.add( column.id, column.logicalName, column.name, sqlType ).nullable( column.nullable );
        }

        return AlgDataTypeImpl.proto( fieldInfo.build() );
    }


    @Override
    public PolyValue[] getParameterArray() {
        return new PolyValue[0];
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( Expressions.convert_( Expressions.call( Catalog.PHYSICAL_EXPRESSION.apply( adapterId ), "get" ), AdapterCatalog.class ), "getPhysical", Expressions.constant( id ) );
    }


    public List<String> getColumnNames() {
        return columns.stream().map( c -> c.name ).collect( Collectors.toList() );
    }


    public List<Long> getColumnIds() {
        return columns.stream().map( c -> c.id ).collect( Collectors.toList() );
    }


    @Override
    public PhysicalEntity normalize() {
        return new PhysicalTable( id, allocationId, logicalId, name, columns, namespaceId, namespaceName, uniqueFieldIds, adapterId );
    }


    public ImmutableList<Long> getPrimaryColumns() {
        return Catalog.snapshot().rel().getPrimaryKey( Catalog.snapshot().rel().getTable( logicalId ).orElseThrow().primaryKey ).orElseThrow().columnIds;
    }

}
