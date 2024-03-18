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

package org.polypheny.db.webui.models.catalog.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.type.PolyType;

@EqualsAndHashCode(callSuper = true)
@Value
public class ColumnModel extends FieldModel {

    @JsonProperty
    @JsonSerialize(using = PolyTypeSerializer.class)
    public PolyType type;

    @JsonProperty
    @JsonSerialize(using = PolyTypeSerializer.class)
    public PolyType collectionsType;

    @JsonProperty
    public Integer precision;

    @JsonProperty
    public Integer scale;

    @JsonProperty
    public String defaultValue;

    @JsonProperty
    public Integer dimension;

    @JsonProperty
    public Integer cardinality;

    @JsonProperty
    public boolean nullable;

    @JsonProperty
    public int position;


    public ColumnModel(
            @JsonProperty("id") @Nullable Long id,
            @JsonProperty("name") @Nullable String name,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("type") PolyType type,
            @JsonProperty("collectionsType") PolyType collectionsType,
            @JsonProperty("precision") Integer precision,
            @JsonProperty("scale") Integer scale,
            @JsonProperty("defaultValue") String defaultValue,
            @JsonProperty("dimension") Integer dimension,
            @JsonProperty("cardinality") Integer cardinality,
            @JsonProperty("nullable") boolean nullable,
            @JsonProperty("position") int position ) {
        super( id, name, tableId );
        this.type = type;
        this.nullable = nullable;
        this.position = position;
        this.collectionsType = collectionsType;
        this.precision = precision;
        this.scale = scale;
        this.defaultValue = defaultValue;
        this.dimension = dimension;
        this.cardinality = cardinality;
    }


    public static ColumnModel from( LogicalColumn column ) {
        return new ColumnModel(
                column.id,
                column.name,
                column.tableId,
                column.type,
                column.collectionsType,
                column.length,
                column.scale,
                column.defaultValue == null ? null : column.defaultValue.value.toJson(),
                column.dimension,
                column.cardinality,
                column.nullable,
                column.position );
    }


    public static class PolyTypeSerializer extends JsonSerializer<PolyType> {

        @Override
        public void serialize( PolyType value, JsonGenerator gen, SerializerProvider serializers ) throws IOException {
            gen.writeStartObject();
            gen.writeFieldName( "name" );
            gen.writeString( value.getName() );
            gen.writeFieldName( "signatures" );
            gen.writeNumber( value.getSignatures() );
            gen.writeEndObject();
        }

    }

}
