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

package org.polypheny.db.backup.webui.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.Value;
import org.polypheny.db.type.entity.PolyTimestamp;

@Value
public class ManifestModel {


    @JsonProperty
    List<ElementModel> elements; // 3 namespaces at max with entities and columns as children and sub-children

    @JsonProperty
    long id;

    @JsonProperty
    @JsonSerialize(using = PolyTimestampSerializer.class)
    PolyTimestamp timestamp;


    public ManifestModel(
            @JsonProperty("id") long id,
            @JsonProperty("elements") List<ElementModel> elements,
            @JsonProperty("timestamp") PolyTimestamp timestamp ) {
        this.id = id;
        this.elements = elements;
        this.timestamp = timestamp;
    }


    private static class PolyTimestampSerializer extends JsonSerializer<PolyTimestamp> {

        @Override
        public void serializeWithType( PolyTimestamp value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer ) throws IOException {
            serialize( value, gen, serializers );
        }


        @Override
        public void serialize( PolyTimestamp value, JsonGenerator gen, SerializerProvider serializers ) throws IOException {
            gen.writeString( value.toHumanReadable() );
        }

    }


    // dummy method to delete later
    public static ManifestModel getDummy() {
        List<ElementModel> elements = new ArrayList<>();
        elements.addAll( ElementModel.getDummyRels() );
        elements.add( ElementModel.getDummyDoc() );
        elements.add( ElementModel.getDummyGraph() );
        return new ManifestModel( -1, elements, new PolyTimestamp( 1L ) );
    }


}
