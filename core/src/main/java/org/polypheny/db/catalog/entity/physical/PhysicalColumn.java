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
import io.activej.serializer.annotations.SerializeNullable;
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.entity.LogicalDefaultValue;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.type.PolyType;

@EqualsAndHashCode(callSuper = true)
@Value
@SuperBuilder(toBuilder = true)
public class PhysicalColumn extends PhysicalField {

    @Serialize
    public int position; // normalized to start with 0

    @Serialize
    public PolyType type;

    @Serialize
    @SerializeNullable
    @Nullable
    public PolyType collectionsType;

    @Serialize
    @Nullable
    @SerializeNullable
    public Integer length; // JDBC length or precision depending on type

    @Serialize
    @Nullable
    @SerializeNullable
    public Integer scale; // decimal digits

    @Serialize
    @Nullable
    @SerializeNullable
    public Integer dimension;

    @Serialize
    @Nullable
    @SerializeNullable
    public Integer cardinality;

    @Serialize
    public boolean nullable;

    @Serialize
    @Nullable
    @SerializeNullable
    public Collation collation;

    @Serialize
    @Nullable
    @SerializeNullable
    public LogicalDefaultValue defaultValue;


    public PhysicalColumn(
            @Deserialize("id") final long id,
            @Deserialize("name") final String name,
            @Deserialize("logicalName") final String logicalName,
            @Deserialize("allocId") final long allocId,
            @Deserialize("entityId") final long tableId,
            @Deserialize("adapterId") final long adapterId,
            @Deserialize("position") final int position,
            @Deserialize("type") @NotNull final PolyType type,
            @Deserialize("collectionsType") final PolyType collectionsType,
            @Deserialize("length") final Integer length,
            @Deserialize("scale") final Integer scale,
            @Deserialize("dimension") final Integer dimension,
            @Deserialize("cardinality") final Integer cardinality,
            @Deserialize("nullable") final boolean nullable,
            @Deserialize("collation") final Collation collation,
            @Deserialize("defaultValue") LogicalDefaultValue defaultValue ) {
        super( id, name, logicalName, allocId, tableId, adapterId, DataModel.RELATIONAL, true );
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
            long allocTableId,
            final long adapterId,
            final int position,
            LogicalColumn column ) {
        this(
                column.id,
                name,
                column.name,
                allocTableId,
                tableId,
                adapterId,
                position,
                column.type,
                column.collectionsType,
                column.length,
                column.scale,
                column.dimension,
                column.cardinality,
                column.nullable,
                column.collation,
                column.defaultValue );
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
    public State getLayer() {
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
