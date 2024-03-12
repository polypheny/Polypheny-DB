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

    @JsonSerialize(using = PolyTypeSerializer.class)
    public PolyType type;

    @JsonSerialize(using = PolyTypeSerializer.class)
    public PolyType collectionsType;
    public Integer precision;
    public Integer scale;
    public String defaultValue;
    public Integer dimension;
    public Integer cardinality;
    public boolean nullable;
    public int position;


    public ColumnModel(
            @Nullable Long id,
            @Nullable String name,
            long tableId,
            PolyType type,
            PolyType collectionsType,
            Integer precision,
            Integer scale,
            String defaultValue,
            Integer dimension,
            Integer cardinality,
            boolean nullable,
            int position ) {
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
