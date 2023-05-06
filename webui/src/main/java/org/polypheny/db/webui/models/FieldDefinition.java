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

package org.polypheny.db.webui.models;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;

@Value
@Accessors(chain = true)
@SuperBuilder
@NonFinal
public class FieldDefinition {

    public String name;
    // for both
    public String dataType; //varchar/int/etc


    public static TypeAdapter<FieldDefinition> serializer = new TypeAdapter<>() {
        @Override
        public void write( JsonWriter out, FieldDefinition col ) throws IOException {
            out.beginObject();
            out.name( "name" );
            out.value( col.name );
            out.name( "dataType" );
            out.value( col.dataType );
            out.endObject();
        }


        @Override
        public FieldDefinition read( JsonReader in ) throws IOException {
            if ( in.peek() == null ) {
                in.nextNull();
                return null;
            }
            in.beginObject();
            FieldDefinitionBuilder<?, ?> builder = FieldDefinition.builder();
            while ( in.peek() != JsonToken.END_OBJECT ) {
                switch ( in.nextName() ) {
                    case "name":
                        builder.name( in.nextString() );
                        break;
                    case "dataType":
                        builder.dataType( in.nextString() );
                        break;
                }
            }
            in.endObject();

            return builder.build();
        }

    };


}
