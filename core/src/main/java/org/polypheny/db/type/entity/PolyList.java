/*
 * Copyright 2019-2026 The Polypheny Project
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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
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
import com.google.common.collect.Lists;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.def.SimpleSerializerDef;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;


@JsonSerialize(using = PolyList.PolyListSerializer.class)
@JsonDeserialize(using = PolyList.PolyListDeserializer.class)
public abstract class PolyList<E extends PolyValue> extends PolyValue implements List<E> {

    public PolyList( PolyType type ) {
        super( type );
    }

    @SuppressWarnings("unchecked")
    public static <E extends PolyValue> PolyList<E> copyOf( List<E> value ) {
        if ( value == null ) return null;
        if ( value instanceof PolyFloatList<?> fl ) {
            return (PolyList<E>) (PolyList<?>) new PolyFloatList<>( Arrays.copyOf( fl.getRawValue(), fl.size() ) );
        }
        if ( value.isEmpty() ) return  emptyList();
        return new PolyListImpl<>( value );
    }


    @SuppressWarnings("unchecked")
    public static <E extends PolyValue> PolyList<E> emptyList() {
        return (PolyList<E>) PolyListImpl.EMPTY_LIST;
    }


    public static <E extends PolyValue> PolyList<E> copyOf( Iterable<E> value ) {
        return copyOf( Lists.newArrayList( value ) );
    }


    public static <E extends PolyValue> PolyList<E> copyOf( Iterator<E> iterator ) {
        return copyOf( Lists.newArrayList( iterator ) );
    }


    public static <E extends PolyValue> PolyList<E> of( Collection<E> value ) {
        if ( value instanceof PolyFloatList<?> ) return copyOf( (PolyList<E>) value );
        return copyOf( new ArrayList<>( value ) );
    }


    @SuppressWarnings( "unchecked" )
    public static PolyList<PolyValue> of( float[] floats ) {
        return  (PolyList<PolyValue>) (PolyList<?>) new PolyFloatList<>( floats );
    }


    @SafeVarargs
    public static <E extends PolyValue> PolyList<E> ofElements( E... value ) {
        return copyOf( Arrays.asList( value ) );
    }


    public static <E extends PolyValue> PolyList<E> ofNullable( Collection<E> value ) {
        if ( value == null ) return null;
        if ( value instanceof PolyFloatList<?> ) return copyOf( (PolyList<E>) value );
        return copyOf( new ArrayList<>( value ) );
    }


    @SafeVarargs
    public static <E extends PolyValue> PolyList<E> of( E... values ) {
        return copyOf( Arrays.asList( values ) );
    }


    @SuppressWarnings("unused")
    public static <E extends PolyValue> PolyList<E> ofArray( E[] values ) {
        return copyOf( Arrays.asList( values ) );
    }


    /**
     * Required due to limitation of call, where interfaces lead to errors.
     */
    @SuppressWarnings("unused")
    @SafeVarargs
    public static <E extends PolyValue> PolyList<E> ofExpression( E... values ) {
        return copyOf( Arrays.asList( values ) );
    }


    public static PolyList<?> convert(@Nullable Object object ) {
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

    public static class PolyListSerializerDef extends SimpleSerializerDef<PolyList<?>> {

        @Override
        protected BinarySerializer<PolyList<?>> createSerializer(int version, CompatibilityLevel compatibilityLevel ) {
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
                    return PolyList.copyOf( list );
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
        @SuppressWarnings("unchecked")
        public PolyList<PolyValue> deserialize(JsonParser p, DeserializationContext ctxt ) throws IOException {
            JsonNode node = p.getCodec().readTree( p );
            JsonNode nField = node.get( "isNull" );
            boolean isNull = nField.booleanValue();
            if ( isNull ) {
                return PolyList.copyOf( (List<PolyValue>) null );
            }
            JsonNode typeNode = node.get( "@type" );
            String listType = typeNode != null ? typeNode.asText() : "LIST";
            if ( "FLOAT_LIST".equals( listType ) ) {
                JsonNode fsNode = node.get( "_fs" );
                if ( fsNode == null || !fsNode.isArray() ) {
                    throw new IOException( "Malformed FLOAT_LIST: missing '_fs' array field"
                    );
                }
                ArrayNode floatNodes = (ArrayNode) fsNode;
                float[] floats = new float[floatNodes.size()];
                for ( int i = 0; i < floatNodes.size(); i++ ) {
                    floats[i] = (float) floatNodes.get( i ).doubleValue();
                }
                return (PolyList<PolyValue>) (PolyList<?>) new PolyFloatList<>( floats );
            }
            List<PolyValue> values = new ArrayList<>();
            ArrayNode elements = node.withArray( "_es" );
            for ( JsonNode element : elements ) {
                PolyValue el = deserializeElement( ctxt, element );
                values.add( el );
            }
            return PolyList.copyOf( values );
        }


        private PolyValue deserializeElement( DeserializationContext ctxt, JsonNode element ) throws IOException {
            return ctxt.readTreeAsValue( element, PolyValue.class );
        }

    }


    public static class PolyListSerializer extends JsonSerializer<PolyList<PolyValue>> {

        @Override
        public void serializeWithType(PolyList<PolyValue> value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer ) throws IOException {
            serialize( value, gen, serializers );
        }


        /**
         * [{_k:{}, _v{}},{_k:{}, _v{}},...]
         */
        @Override
        public void serialize(PolyList<PolyValue> values, JsonGenerator gen, SerializerProvider serializers ) throws IOException {
            gen.writeStartObject();
            gen.writeFieldName( "isNull" );
            if ( values == null ) {
                gen.writeBoolean( true );
                return;
            }
            gen.writeBoolean( false );
            // PolyFloatList case
            if ( values instanceof PolyFloatList<?> pfl) {
                gen.writeFieldName( "@type" );
                gen.writeString( "FLOAT_LIST" );
                gen.writeFieldName( "_fs" );
                gen.writeStartArray();
                for ( float f : pfl.getRawValue() ) {
                    gen.writeNumber( f );
                }
                gen.writeEndArray();
                gen.writeEndObject();
                return;
            }
            // Generic case
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
