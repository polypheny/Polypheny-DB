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

package org.polypheny.db.catalog.entity.physical;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.SuperBuilder;
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
            @Deserialize("logicalEntityId") final long logicalEntityId,
            @Deserialize("adapterId") final long adapterId,
            @Deserialize("position") final int position,
            @Deserialize("type") @NotNull final PolyType type,
            @Deserialize("collectionsType") final @Nullable PolyType collectionsType,
            @Deserialize("length") final @Nullable Integer length,
            @Deserialize("scale") final @Nullable Integer scale,
            @Deserialize("dimension") final @Nullable Integer dimension,
            @Deserialize("cardinality") final @Nullable Integer cardinality,
            @Deserialize("nullable") final boolean nullable,
            @Deserialize("collation") final @Nullable Collation collation,
            @Deserialize("defaultValue") @Nullable LogicalDefaultValue defaultValue ) {
        super( id, name, logicalName, allocId, logicalEntityId, adapterId, DataModel.RELATIONAL, true );
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


    public AlgDataType getAlgDataType( final AlgDataTypeFactory typeFactory ) {
        return LogicalColumn.getAlgDataType( typeFactory, this.length, this.scale, this.type, collectionsType, cardinality, dimension, nullable );
    }

}
