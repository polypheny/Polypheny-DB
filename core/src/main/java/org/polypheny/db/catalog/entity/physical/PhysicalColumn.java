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

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.type.PolyType;

@EqualsAndHashCode(callSuper = true)
@Value
public class PhysicalColumn extends CatalogEntity {

    public long adapterId;
    @Serialize
    public int position;

    @Serialize
    public PolyType type;

    @Serialize
    public PolyType collectionsType;

    @Serialize
    public Integer length; // JDBC length or precision depending on type

    @Serialize
    public Integer scale; // decimal digits

    @Serialize
    public Integer dimension;

    @Serialize
    public Integer cardinality;

    @Serialize
    public boolean nullable;

    @Serialize
    public Collation collation;

    @Serialize
    public CatalogDefaultValue defaultValue;


    public PhysicalColumn(
            @Deserialize("id") final long id,
            @Deserialize("name") final String name,
            @Deserialize("tableId") final long tableId,
            @Deserialize("adapterId") final long adapterId,
            @Deserialize("position") final int position,
            @Deserialize("type") @NonNull final PolyType type,
            @Deserialize("collectionsType") final PolyType collectionsType,
            @Deserialize("length") final Integer length,
            @Deserialize("scale") final Integer scale,
            @Deserialize("dimension") final Integer dimension,
            @Deserialize("cardinality") final Integer cardinality,
            @Deserialize("nullable") final boolean nullable,
            @Deserialize("collation") final Collation collation,
            @Deserialize("defaultValue") CatalogDefaultValue defaultValue ) {
        super( id, name, tableId, EntityType.ENTITY, NamespaceType.RELATIONAL, true );
        this.adapterId = adapterId;
        this.position = position;
        this.type = type;
        this.collectionsType = collectionsType;
        this.length = length;
        this.scale = scale;
        this.dimension = dimension;
        this.cardinality = cardinality;
        this.nullable = nullable;
        this.collation = collation;
        this.defaultValue = defaultValue;

    }


    public PhysicalColumn(
            final String name,
            final long tableId,
            final long adapterId,
            final int position,
            LogicalColumn column ) {
        this( column.id, name, tableId, adapterId, position, column.type, column.collectionsType, column.length, column.scale, column.dimension, column.cardinality, column.nullable, column.collation, column.defaultValue );
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }


    @Override
    public Expression asExpression() {
        return null;
    }


    @Override
    public State getCatalogType() {
        return State.PHYSICAL;
    }


    public AlgDataType getAlgDataType( final AlgDataTypeFactory typeFactory ) {
        // todo merge with LogicalColumn
        AlgDataType elementType;
        if ( this.length != null && this.scale != null && this.type.allowsPrecScale( true, true ) ) {
            elementType = typeFactory.createPolyType( this.type, this.length, this.scale );
        } else if ( this.length != null && this.type.allowsPrecNoScale() ) {
            elementType = typeFactory.createPolyType( this.type, this.length );
        } else {
            assert this.type.allowsNoPrecNoScale();
            elementType = typeFactory.createPolyType( this.type );
        }

        if ( collectionsType == PolyType.ARRAY ) {
            elementType = typeFactory.createArrayType( elementType, cardinality != null ? cardinality : -1, dimension != null ? dimension : -1 );
        } else if ( collectionsType == PolyType.MAP ) {
            elementType = typeFactory.createMapType( typeFactory.createPolyType( PolyType.ANY ), elementType );
        }

        return typeFactory.createTypeWithNullability( elementType, nullable );
    }

}
