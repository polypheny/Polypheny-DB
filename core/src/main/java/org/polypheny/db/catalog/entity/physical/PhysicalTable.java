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

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.type.PolyTypeFactoryImpl;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
public class PhysicalTable extends PhysicalEntity {


    public ImmutableMap<Long, String> columns;

    public String namespaceName;
    public ImmutableMap<Long, AlgDataType> types;


    public PhysicalTable( long id, String name, long namespaceId, String namespaceName, long adapterId, Map<Long, String> columns, Map<Long, AlgDataType> types ) {
        super( id, name, namespaceId, namespaceName, EntityType.ENTITY, NamespaceType.RELATIONAL, adapterId );
        this.namespaceName = namespaceName;
        this.columns = ImmutableMap.copyOf( columns );
        this.types = ImmutableMap.copyOf( types );
    }


    public PhysicalTable( AllocationTable table, String name, String namespaceName, Map<Long, String> columns, Map<Long, AlgDataType> types ) {
        this( table.id, name, table.namespaceId, namespaceName, table.adapterId, columns, types );
    }


    @Override
    public AlgDataType getRowType() {
        return buildProto().apply( AlgDataTypeFactory.DEFAULT );
    }


    public AlgProtoDataType buildProto() {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();

        for ( long id : columns.keySet() ) {
            fieldInfo.add( columns.get( id ), columns.get( id ), types.get( id ) ).nullable( types.get( id ).isNullable() );
        }

        return AlgDataTypeImpl.proto( fieldInfo.build() );
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( Catalog.CATALOG_EXPRESSION, "getPhysicalTable", Expressions.constant( id ) );
    }

}
