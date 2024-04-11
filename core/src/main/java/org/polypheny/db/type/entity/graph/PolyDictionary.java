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

package org.polypheny.db.type.entity.graph;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.def.SimpleSerializerDef;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyDictionary.PolyDictionaryDeserializer;
import org.polypheny.db.type.entity.graph.PolyDictionary.PolyDictionarySerializer;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.BuiltInMethod;

@Slf4j
@JsonDeserialize(using = PolyDictionaryDeserializer.class)
@JsonSerialize(using = PolyDictionarySerializer.class)
public class PolyDictionary extends PolyMap<PolyString, PolyValue> {


    public PolyDictionary( @JsonProperty("map") Map<PolyString, PolyValue> map ) {
        super( map, MapType.DICTIONARY );
    }


    public PolyDictionary() {
        this( Map.of() );
    }


    public static PolyDictionary ofDict( Map<PolyString, PolyValue> map ) {
        return new PolyDictionary( map );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyDictionary.class, Expressions.call(
                BuiltInMethod.MAP_OF_ENTRIES.method,
                EnumUtils.expressionList(
                        entrySet()
                                .stream()
                                .map( p -> Expressions.call(
                                        BuiltInMethod.PAIR_OF.method,
                                        p.getKey().asExpression(),
                                        p.getValue().asExpression() ) ).collect( Collectors.toList() ) ) ) );
    }


    @Override
    public @NotNull String toTypedJson() {
        try {
            return JSON_WRAPPER.writerFor( new TypeReference<PolyDictionary>() {
            } ).writeValueAsString( this );
        } catch ( JsonProcessingException e ) {
            log.warn( "Error on serializing typed JSON." );
            return PolyNull.NULL.toTypedJson();
        }
    }


    public static PolyDictionary fromString( String json ) {
        return PolyValue.fromTypedJson( json, PolyDictionary.class );
    }


    public static class PolyDictionarySerializerDef extends SimpleSerializerDef<PolyDictionary> {

        @Override
        protected BinarySerializer<PolyDictionary> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyDictionary item ) {
                    out.writeLong( item.size() );
                    for ( Entry<PolyString, PolyValue> entry : item.entrySet() ) {
                        out.writeUTF8( PolySerializable.serialize( serializer, entry.getKey() ) );
                        out.writeUTF8( PolySerializable.serialize( serializer, entry.getValue() ) );
                    }
                }


                @Override
                public PolyDictionary decode( BinaryInput in ) throws CorruptedDataException {
                    Map<PolyString, PolyValue> map = new HashMap<>();
                    long size = in.readLong();
                    for ( long i = 0; i < size; i++ ) {
                        map.put(
                                PolySerializable.deserialize( in.readUTF8(), serializer ).asString(),
                                PolySerializable.deserialize( in.readUTF8(), serializer ) );
                    }
                    return new PolyDictionary( map );
                }
            };
        }

    }


    static class PolyDictionarySerializer extends JsonSerializer<PolyDictionary> {


        @Override
        public void serializeWithType( PolyDictionary value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer ) throws IOException {
            serialize( value, gen, serializers );
        }


        /**
         * [{_k:{}, _v{}},{_k:{}, _v{}},...]
         */
        @Override
        public void serialize( PolyDictionary value, JsonGenerator gen, SerializerProvider serializers ) throws IOException {
            gen.writeStartObject();
            gen.writeFieldName( "@type" );
            gen.writeString( value.mapType.name() );
            gen.writeFieldName( "_ps" );
            gen.writeStartArray();
            for ( Entry<?, ?> pair : value.entrySet() ) {
                gen.writeStartArray();
                gen.writeString( pair.getKey().toString() );
                serializers.findValueSerializer( pair.getValue().getClass() ).serializeWithType( pair.getValue(), gen, serializers, serializers.findTypeSerializer( JSON_WRAPPER.constructType( pair.getValue().getClass() ) ) );
                gen.writeEndArray();
            }
            gen.writeEndArray();
            gen.writeEndObject();
        }

    }


    static class PolyDictionaryDeserializer<K extends PolyValue, V extends PolyValue> extends StdDeserializer<PolyDictionary> {


        protected PolyDictionaryDeserializer() {
            super( PolyMap.class );
        }


        @Override
        public Object deserializeWithType( JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer ) throws IOException {
            return deserialize( p, ctxt );
        }


        @Override
        public PolyDictionary deserialize( JsonParser p, DeserializationContext ctxt ) throws IOException {
            Map<PolyString, PolyValue> values = new HashMap<>();
            ObjectMapper mapper = (ObjectMapper) p.getCodec();

            JsonToken token;
            while ( (token = p.nextToken()) != JsonToken.END_OBJECT ) {
                if ( token == JsonToken.FIELD_NAME && p.currentName().equals( "_ps" ) ) {
                    p.nextToken(); // Move to the start of the array
                    while ( p.nextToken() != JsonToken.END_ARRAY ) {
                        p.nextToken(); // open array
                        PolyString key = PolyString.of( p.getValueAsString() );
                        p.nextToken(); // Move to next value
                        PolyValue value = mapper.readValue( p, ctxt.constructType( PolyValue.class ) );
                        values.put( key, value );
                        p.nextToken(); // close array
                    }

                }
            }
            return PolyDictionary.ofDict( values );
        }


    }


}
