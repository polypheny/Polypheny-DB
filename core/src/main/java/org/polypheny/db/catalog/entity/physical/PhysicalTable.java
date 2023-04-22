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
import java.io.Serializable;
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
import org.polypheny.db.catalog.catalogs.StoreCatalog;
import org.polypheny.db.catalog.logistic.NamespaceType;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
@SuperBuilder(toBuilder = true)
public class PhysicalTable extends PhysicalEntity {


    public ImmutableList<PhysicalColumn> columns;


    public PhysicalTable(
            long id,
            long allocationId,
            String name,
            List<PhysicalColumn> columns,
            long namespaceId,
            String namespaceName,
            long adapterId ) {
        super( id, allocationId, name, namespaceId, namespaceName, NamespaceType.RELATIONAL, adapterId );
        this.columns = ImmutableList.copyOf( columns );
    }


    @Override
    public AlgDataType getRowType() {
        return buildProto().apply( AlgDataTypeFactory.DEFAULT );
    }


    public AlgProtoDataType buildProto() {
        final AlgDataTypeFactory.Builder fieldInfo = AlgDataTypeFactory.DEFAULT.builder();

        for ( PhysicalColumn column : columns.stream().sorted( Comparator.comparingInt( a -> a.position ) ).collect( Collectors.toList() ) ) {
            AlgDataType sqlType = column.getAlgDataType( AlgDataTypeFactory.DEFAULT );
            fieldInfo.add( column.logicalName, column.name, sqlType ).nullable( column.nullable );
        }

        return AlgDataTypeImpl.proto( fieldInfo.build() );
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }


    @Override
    public Expression asExpression() {
        //return Expressions.call( Catalog.PHYSICAL_EXPRESSION, "getPhysicalTable", Expressions.constant( id ) );
        return Expressions.call( Expressions.convert_( Expressions.call( Catalog.PHYSICAL_EXPRESSION.apply( adapterId ), "get" ), StoreCatalog.class ), "getPhysical", Expressions.constant( id ) );
    }

}
