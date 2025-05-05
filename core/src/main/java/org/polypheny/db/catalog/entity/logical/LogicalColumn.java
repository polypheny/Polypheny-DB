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

package org.polypheny.db.catalog.entity.logical;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import java.io.Serial;
import java.util.concurrent.atomic.AtomicLong;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalDefaultValue;
import org.polypheny.db.catalog.entity.PolyObject;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.type.PolyType;


@Value
@SuperBuilder(toBuilder = true)
@NonFinal
public class LogicalColumn implements PolyObject, Comparable<LogicalColumn> {

    @Serial
    private static final long serialVersionUID = -4792846455300897399L;

    @Serialize
    @JsonProperty
    public long id;

    @Serialize
    @JsonProperty
    public String name;

    @Serialize
    @JsonProperty
    public long tableId;

    @Serialize
    @JsonProperty
    public long namespaceId;

    @Serialize
    @JsonProperty
    public int position;

    @Serialize
    @JsonProperty
    public PolyType type;

    @Serialize
    @JsonProperty
    public @SerializeNullable PolyType collectionsType;

    @Serialize
    @JsonProperty
    public @SerializeNullable Integer length; // JDBC length or precision depending on type

    @Serialize
    @JsonProperty
    public @SerializeNullable Integer scale; // decimal digits

    @Serialize
    @JsonProperty
    public @SerializeNullable Integer dimension;

    @Serialize
    @JsonProperty
    public @SerializeNullable Integer cardinality;

    @Serialize
    @JsonProperty
    public boolean nullable;

    @Serialize
    @JsonProperty
    public boolean autoIncrement;

    public AtomicLong currentValue = new AtomicLong(0l);

    @Serialize
    @JsonProperty
    public @SerializeNullable Collation collation;

    @Serialize
    @JsonProperty
    @SerializeNullable
    public LogicalDefaultValue defaultValue;

    public DataModel dataModel = DataModel.RELATIONAL;


    public LogicalColumn(
            @Deserialize("id") final long id,
            @Deserialize("name") @NonNull final String name,
            @Deserialize("tableId") final long tableId,
            @Deserialize("namespaceId") final long namespaceId,
            @Deserialize("position") final int position,
            @Deserialize("type") @NonNull final PolyType type,
            @Deserialize("collectionsType") final PolyType collectionsType,
            @Deserialize("length") final Integer length,
            @Deserialize("scale") final Integer scale,
            @Deserialize("dimension") final Integer dimension,
            @Deserialize("cardinality") final Integer cardinality,
            @Deserialize("nullable") final boolean nullable,
            @Deserialize("collation") final Collation collation,
            @Deserialize("defaultValue") final LogicalDefaultValue defaultValue,
            @Deserialize("autoIncrement") final boolean autoIncrement) {
        this.id = id;
        this.name = name;
        this.tableId = tableId;
        this.namespaceId = namespaceId;
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
        this.autoIncrement = autoIncrement;
    }


    public AlgDataType getAlgDataType( final AlgDataTypeFactory typeFactory ) {
        return getAlgDataType( typeFactory, this.length, this.scale, this.type, collectionsType, cardinality, dimension, nullable );
    }


    public static AlgDataType getAlgDataType( AlgDataTypeFactory typeFactory, Integer length, Integer scale, PolyType type, PolyType collectionsType, Integer cardinality, Integer dimension, boolean nullable ) {
        AlgDataType elementType;
        if ( length != null && scale != null && type.allowsPrecScale( true, true ) ) {
            elementType = typeFactory.createPolyType( type, length, scale );
        } else if ( length != null && type.allowsPrecNoScale() ) {
            elementType = typeFactory.createPolyType( type, length );
        } else {
            assert type.allowsNoPrecNoScale();
            elementType = typeFactory.createPolyType( type );
        }

        if ( collectionsType == PolyType.ARRAY ) {
            elementType = typeFactory.createArrayType( elementType, cardinality != null ? cardinality : -1, dimension != null ? dimension : -1 );
        } else if ( collectionsType == PolyType.MAP ) {
            elementType = typeFactory.createMapType( typeFactory.createPolyType( PolyType.ANY ), elementType );
        }

        return typeFactory.createTypeWithNullability( elementType, nullable );
    }


    public String getNamespaceName() {
        return Catalog.snapshot().getNamespace( namespaceId ).orElseThrow().name;
    }


    public String getTableName() {
        return Catalog.snapshot().rel().getTable( tableId ).orElseThrow().name;
    }


    @Override
    public int compareTo( LogicalColumn o ) {
        int comp = Long.compare( this.namespaceId, o.namespaceId );
        if ( comp == 0 ) {
            comp = Long.compare( this.tableId, o.tableId );
            if ( comp == 0 ) {
                return Long.compare( this.id, o.id );
            } else {
                return comp;
            }

        } else {
            return comp;
        }

    }


    /**
     * @param columnSize precision or length
     * @param bufferLength always null
     * @param decimalDigits scale
     * @param sqlDataType always null
     * @param sqlDatetimeSub always null
     * @param charOctetLength always null
     * @param ordinalPosition position
     */

    public record PrimitiveCatalogColumn( String tableCat, String tableSchem, String tableName, String columnName, int dataType, String typeName, Integer columnSize, Integer bufferLength, Integer decimalDigits, Integer numPrecRadix, int nullable, String remarks, String columnDef, Integer sqlDataType, Integer sqlDatetimeSub, Integer charOctetLength, int ordinalPosition, String isNullable, String collation ) {

    }


}
