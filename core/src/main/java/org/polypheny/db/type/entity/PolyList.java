/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.type.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.collect.Lists;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.def.SimpleSerializerDef;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList.PolyListDeserializer;
import org.polypheny.db.type.entity.PolyList.PolyListSerializer;
import org.polypheny.db.util.Pair;

@Slf4j
@EqualsAndHashCode(callSuper = false)
@Value
@JsonSerialize(using = PolyListSerializer.class)
@JsonDeserialize(using = PolyListDeserializer.class)
public class PolyList<E extends PolyValue> extends PolyValue implements List<E> {

    public static final PolyList<?> EMPTY_LIST = new PolyList<>();

    @Serialize
    @JsonIgnore
    @Delegate
    public List<E> value;


    /**
     * Creates a PolyList, which is the PolyValue implementation of a List,
     * where the List, as well as all the elements are comparable.
     *
     * @param value The value of the PolyList
     */
    @JsonCreator
    public PolyList( @JsonProperty("value") @Deserialize("value") List<E> value ) {
        super( PolyType.ARRAY );
        this.value = new ArrayList<>( value );
    }


    public static <E extends PolyValue> PolyList<E> copyOf( List<E> value ) {
        return new PolyList<>( value );
    }


    public static <E extends PolyValue> PolyList<E> copyOf( Iterable<E> value ) {
        return copyOf( Lists.newArrayList( value ) );
    }


    public static <E extends PolyValue> PolyList<E> copyOf( Iterator<E> iterator ) {
        return copyOf( Lists.newArrayList( iterator ) );
    }


    @SafeVarargs
    public PolyList( E... value ) {
        this( Arrays.asList( value ) );
    }


    @Override
    public @NotNull String toTypedJson() {
        try {
            return JSON_WRAPPER.writerFor( new TypeReference<PolyList<PolyValue>>() {
            } ).writeValueAsString( this );
        } catch ( JsonProcessingException e ) {
            log.warn( "Error on serializing typed JSON." );
            return PolyNull.NULL.toTypedJson();
        }
    }


    public static PolyList<?> convert( @Nullable Object object ) {
        if ( object == null ) {
            return null;
        }

        if ( object instanceof PolyValue ) {
            if ( ((PolyValue) object).isList() ) {
                return ((PolyValue) object).asList();
            }
        }

        throw new GenericRuntimeException( "Could not convert List" );
    }


    @Override
    public String toString() {
        return value == null ? "null" : value.toString();
    }


    public static <E extends PolyValue> PolyList<E> of( Collection<E> value ) {
        return new PolyList<>( new ArrayList<>( value ) );
    }


    @SafeVarargs
    public static <E extends PolyValue> PolyList<E> ofElements( E... value ) {
        return new PolyList<>( value );
    }


    @SuppressWarnings("unused")
    public static <E extends PolyValue> PolyList<E> ofNullable( Collection<E> value ) {
        return value == null ? null : new PolyList<>( new ArrayList<>( value ) );
    }


    @SafeVarargs
    public static <E extends PolyValue> PolyList<E> of( E... values ) {
        return new PolyList<>( values );
    }


    @SuppressWarnings("unused")
    public static <E extends PolyValue> PolyList<E> ofArray( E[] values ) {
        return new PolyList<>( values );
    }


    /**
     * Required due to limitation of call, where interfaces lead to errors.
     */
    @SuppressWarnings("unused")
    @SafeVarargs
    public static <E extends PolyValue> PolyList<E> ofExpression( E... values ) {
        return new PolyList<>( values );
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( PolyList.class, "ofExpression", value.stream().map( e -> e == null ? Expressions.constant( null ) : e.asExpression() ).toList() );
    }


    @Override
    public @Nullable String toJson() {
        return value == null ? JsonToken.VALUE_NULL.asString() : "[" + value.stream().map( e -> {
            if ( e == null ) {
                return JsonToken.VALUE_NULL.asString();
            } else if ( e.isString() ) {
                return e.asString().toQuotedJson();
            } else if ( e.isDate() || e.isTime() || e.isTimestamp() ) {
                return "\"" + e.toJson() + "\"";
            } else {
                return e.toJson();
            }
        } ).collect( Collectors.joining( "," ) ) + "]";
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        if ( value.size() != o.asList().value.size() ) {
            return Long.compare( value.size(), o.asList().value.size() );
        }

        for ( Pair<E, ?> pair : Pair.zip( value, o.asList().value ) ) {
            if ( pair.left.compareTo( (PolyValue) pair.right ) != 0 ) {
                return pair.left.compareTo( (PolyValue) pair.right );
            }
        }

        return 0;
    }


    @Override
    public PolySerializable copy() {
        return null;
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    @Override
    public Object toJava() {
        return value == null ? null : value.stream().map( PolyValue::toJava ).collect( Collectors.toList() );
    }


    public static class PolyListSerializerDef extends SimpleSerializerDef<PolyList<?>> {

        @Override
        protected BinarySerializer<PolyList<?>> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyList<?> item ) {
                    if ( item == null ) {
                        out.writeBoolean( true );
                        return;
                    }
                    out.writeBoolean( false );
                    out.writeLong( item.size() );
                    for ( PolyValue entry : item ) {
                        out.writeUTF8( PolySerializable.serialize( serializer, entry ) );
                    }
                }


                @Override
                public PolyList<?> decode( BinaryInput in ) throws CorruptedDataException {
                    if ( in.readBoolean() ) {
                        return null;
                    }
                    List<PolyValue> list = new ArrayList<>();
                    long size = in.readLong();
                    for ( long i = 0; i < size; i++ ) {
                        list.add( PolySerializable.deserialize( in.readUTF8(), serializer ) );
                    }
                    return PolyList.of( list );
                }
            };
        }

    }


    public static class PolyListDeserializer extends StdDeserializer<PolyList<? extends PolyValue>> {


        protected PolyListDeserializer() {
            super( PolyList.class );
        }


        @Override
        public Object deserializeWithType( JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer ) throws IOException {
            return deserialize( p, ctxt );
        }


        @Override
        public PolyList<PolyValue> deserialize( JsonParser p, DeserializationContext ctxt ) throws IOException {
            Boolean isNull = null;
            List<PolyValue> values = List.of();
            JsonToken token = p.hasToken( JsonToken.START_OBJECT ) ? p.nextToken() : p.currentToken();
            while ( token == JsonToken.FIELD_NAME ) {
                String field = p.currentName();
                p.nextToken();
                switch ( field ) {
                    case "isNull" -> {
                        isNull = p.hasToken( JsonToken.VALUE_TRUE );
                        p.skipChildren();
                    }
                    case "_es" -> values = readElements( p, ctxt );
                    default -> p.skipChildren();
                }
                token = p.nextToken();
            }
            if ( token != JsonToken.END_OBJECT ) {
                ctxt.reportWrongTokenException( PolyList.class, JsonToken.END_OBJECT, "Expected a typed list object" );
            }
            if ( isNull == null ) {
                return ctxt.reportInputMismatch( PolyList.class, "Missing list field 'isNull'" );
            }
            if ( isNull ) {
                return new PolyList<>( (List<PolyValue>) null );
            }
            return PolyList.of( values );
        }


        private List<PolyValue> readElements( JsonParser p, DeserializationContext ctxt ) throws IOException {
            if ( !p.hasToken( JsonToken.START_ARRAY ) ) {
                ctxt.reportWrongTokenException( PolyList.class, JsonToken.START_ARRAY, "Expected an array of list elements" );
            }
            List<PolyValue> values = new ArrayList<>();
            while ( p.nextToken() != JsonToken.END_ARRAY ) {
                if ( p.currentToken() == null ) {
                    return ctxt.reportInputMismatch( PolyList.class, "Incomplete list element array" );
                }
                values.add( readTypedJsonValue( p, ctxt ) );
            }
            return values;
        }

    }


    public static class PolyListSerializer extends JsonSerializer<PolyList<PolyValue>> {

        @Override
        public void serializeWithType( PolyList<PolyValue> value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer ) throws IOException {
            serialize( value, gen, serializers );
        }


        /**
         * [{_k:{}, _v{}},{_k:{}, _v{}},...]
         */
        @Override
        public void serialize( PolyList<PolyValue> values, JsonGenerator gen, SerializerProvider serializers ) throws IOException {
            gen.writeStartObject();
            gen.writeFieldName( "isNull" );
            if ( values == null ) {
                gen.writeBoolean( true );
                return;
            }
            gen.writeBoolean( false );

            gen.writeFieldName( "@type" );
            gen.writeString( "LIST" );
            gen.writeFieldName( "_es" );
            gen.writeStartArray();
            for ( PolyValue value : values ) {
                if ( value == null ) {
                    serializers.findValueSerializer( PolyNull.class ).serializeWithType( PolyNull.NULL, gen, serializers, serializers.findTypeSerializer( JSON_WRAPPER.constructType( PolyNull.class ) ) );
                    continue;
                }
                serializers.findValueSerializer( value.getClass() ).serializeWithType( value, gen, serializers, serializers.findTypeSerializer( JSON_WRAPPER.constructType( value.getClass() ) ) );
            }
            gen.writeEndArray();
            gen.writeEndObject();
        }

    }


}
