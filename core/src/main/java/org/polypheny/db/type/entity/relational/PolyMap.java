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

package org.polypheny.db.type.entity.relational;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.def.SimpleSerializerDef;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Delegate;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.relational.PolyMap.PolyMapDeserializer;
import org.polypheny.db.type.entity.relational.PolyMap.PolyMapSerializer;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
@Slf4j
@JsonSerialize(using = PolyMapSerializer.class)
@JsonDeserialize(using = PolyMapDeserializer.class)
public class PolyMap<K extends PolyValue, V extends PolyValue> extends PolyValue implements Map<K, V> {

    public static final PolyMap<?,?> EMPTY_MAP = PolyMap.of( Map.of() );

    @Delegate
    @Serialize
    @JsonSerialize(using = PolyMapSerializer.class)
    @JsonDeserialize(using = PolyMapDeserializer.class)
    public Map<K, V> map;

    @Serialize
    public MapType mapType;


    @JsonCreator
    public PolyMap( @JsonProperty("map") Map<K, V> map, @JsonProperty("type") MapType type ) {
        this( map, PolyType.MAP, type );
    }


    public PolyMap( Map<K, V> map, PolyType type, MapType mapType ) {
        super( type );
        this.mapType = mapType;
        this.map = new HashMap<>( map );
    }


    public static <K extends PolyValue, V extends PolyValue> PolyMap<K, V> of( Map<K, V> map, MapType mapType ) {
        return new PolyMap<>( map, PolyType.MAP, mapType );
    }


    public static <K extends PolyValue, V extends PolyValue> PolyMap<K, V> of( Map<K, V> map ) {
        return new PolyMap<>( map, PolyType.MAP, MapType.MAP );
    }


    @Override
    public @Nullable String toJson() {
        return map == null ? JsonToken.VALUE_NULL.asString() : "{" +
                map.entrySet().stream()
                        .map( e -> (e.getKey().isString() ? e.getKey().asString().toQuotedJson() : e.getKey().toJson()) + ":" +
                                (e.getValue() == null ? JsonToken.VALUE_NULL.asString() :
                                        (e.getValue().isString()
                                                ? e.getValue().asString().toQuotedJson()
                                                : e.getValue().toJson())) ).collect( Collectors.joining( "," ) )
                + "}";
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }
        Map<PolyValue, PolyValue> other = o.asMap();

        if ( map.size() != other.size() ) {

            return map.size() > other.size() ? 1 : -1;
        }

        for ( Entry<PolyValue, PolyValue> entry : other.entrySet() ) {
            if ( other.containsKey( entry.getKey() ) ) {
                int i = entry.getValue().compareTo( other.get( entry.getKey() ) );
                if ( i != 0 ) {
                    return i;
                }
            } else {
                return -1;
            }

        }

        return 0;
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( PolyMap.class, "of", Expressions.call(
                BuiltInMethod.MAP_OF_ENTRIES.method,
                EnumUtils.expressionList(
                        entrySet()
                                .stream()
                                .map( p -> Expressions.call(
                                        BuiltInMethod.PAIR_OF.method,
                                        p.getKey().asExpression(),
                                        p.getValue().asExpression() ) ).collect( Collectors.toList() ) ) ) );
    }


    public boolean isDocument() {
        return mapType == MapType.DOCUMENT;
    }


    @NotNull
    public PolyDocument asDocument() {
        if ( isDocument() ) {
            return (PolyDocument) this;
        }
        throw cannotParse( this, PolyDocument.class );
    }


    @Override
    public Object toJava() {
        return map == null ? null : map.entrySet().stream().collect( Collectors.toMap( e -> e.getKey().toJava(), e -> e.getValue().toJava() ) );
    }


    public boolean isDictionary() {
        return mapType == MapType.DICTIONARY;
    }


    @NotNull
    public PolyDictionary asDictionary() {
        if ( isDictionary() ) {
            return (PolyDictionary) this;
        }
        throw cannotParse( this, PolyDictionary.class );
    }


    @Override
    public @NotNull String toTypedJson() {
        try {
            return JSON_WRAPPER.writerFor( new TypeReference<PolyMap<PolyValue, PolyValue>>() {
            } ).writeValueAsString( this );
        } catch ( JsonProcessingException e ) {
            log.warn( "Error on serializing typed JSON." );
            return null;
        }
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyMap.class );
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    public enum MapType {
        MAP, // all keys allowed
        DOCUMENT, // keys are PolyString, allows nesting
        DICTIONARY // keys are PolyString, no nesting
    }


    public static class PolyMapSerializerDef extends SimpleSerializerDef<PolyMap<?, ?>> {

        @Override
        protected BinarySerializer<PolyMap<? extends PolyValue, ? extends PolyValue>> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyMap<? extends PolyValue, ? extends PolyValue> item ) {
                    out.writeLong( item.size() );
                    for ( Entry<? extends PolyValue, ? extends PolyValue> entry : item.entrySet() ) {
                        out.writeUTF8( PolySerializable.serialize( serializer, entry.getKey() ) );
                        out.writeUTF8( PolySerializable.serialize( serializer, entry.getValue() ) );
                    }
                }


                @Override
                public PolyMap<? extends PolyValue, ? extends PolyValue> decode( BinaryInput in ) throws CorruptedDataException {
                    Map<PolyValue, PolyValue> map = new HashMap<>();
                    long size = in.readLong();
                    for ( long i = 0; i < size; i++ ) {
                        map.put(
                                PolySerializable.deserialize( in.readUTF8(), serializer ),
                                PolySerializable.deserialize( in.readUTF8(), serializer ) );
                    }
                    return PolyMap.of( map, MapType.MAP );
                }
            };
        }

    }


    static class PolyMapSerializer extends JsonSerializer<PolyMap<?, ?>> {

        @Override
        public void serializeWithType( PolyMap<?, ?> value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer ) throws IOException {
            serialize( value, gen, serializers );
        }


        /**
         * [{_k:{}, _v{}},{_k:{}, _v{}},...]
         */
        @Override
        public void serialize( PolyMap<?, ?> value, JsonGenerator gen, SerializerProvider serializers ) throws IOException {
            gen.writeStartObject();
            gen.writeFieldName( "@class" );
            gen.writeString( value.getClass().getCanonicalName() );
            gen.writeFieldName( "_ps" );
            gen.writeStartArray();
            for ( Entry<?, ?> pair : value.entrySet() ) {
                gen.writeStartObject();
                gen.writeFieldName( "_k" );
                serializers.findValueSerializer( pair.getKey().getClass() ).serializeWithType( pair.getKey(), gen, serializers, serializers.findTypeSerializer( JSON_WRAPPER.constructType( pair.getKey().getClass() ) ) );
                gen.writeFieldName( "_v" );
                serializers.findValueSerializer( pair.getValue().getClass() ).serializeWithType( pair.getValue(), gen, serializers, serializers.findTypeSerializer( JSON_WRAPPER.constructType( pair.getValue().getClass() ) ) );
                gen.writeEndObject();
            }
            gen.writeEndArray();
            gen.writeEndObject();
        }

    }


    static class PolyMapDeserializer extends StdDeserializer<PolyMap<?, ?>> {


        protected PolyMapDeserializer() {
            super( PolyMap.class );
        }


        @Override
        public Object deserializeWithType( JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer ) throws IOException {
            return deserialize( p, ctxt );
        }


        @Override
        public PolyMap<?, ?> deserialize( JsonParser p, DeserializationContext ctxt ) throws IOException, JacksonException {
            JsonNode node = p.getCodec().readTree( p );
            Map<PolyValue, PolyValue> values = new HashMap<>();
            ArrayNode elements = node.withArray( "_ps" );
            for ( JsonNode element : elements ) {
                Pair<PolyValue, PolyValue> el = deserializeElement( ctxt, element );
                values.put( el.getKey(), el.getValue() );
            }
            return PolyMap.of( values, MapType.MAP );
        }


        private Pair<PolyValue, PolyValue> deserializeElement( DeserializationContext ctxt, JsonNode element ) throws IOException {
            PolyValue key = ctxt.readTreeAsValue( element.get( "_k" ), PolyValue.class );
            PolyValue value = ctxt.readTreeAsValue( element.get( "_v" ), PolyValue.class );
            return Pair.of( key, value );
        }

    }

}
