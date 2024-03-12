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

package org.polypheny.db.type.entity.document;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.def.SimpleSerializerDef;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument.PolyDocumentDeserializer;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.Pair;

@Slf4j
@EqualsAndHashCode(callSuper = true)
@JsonDeserialize(using = PolyDocumentDeserializer.class)
public class PolyDocument extends PolyMap<PolyString, PolyValue> {

    public static final PolyDocument EMPTY_DOCUMENT = PolyDocument.ofDocument( Map.of() );

    public boolean isUnset; // documents can contain documents and so on, additionally such a sub document, can not only be null, but also unset


    public PolyDocument( @JsonProperty("map") @Deserialize("map") Map<PolyString, PolyValue> value, @JsonProperty("isUnset") @Deserialize("isUnset") boolean isUnset ) {
        super( value, PolyType.DOCUMENT, MapType.DOCUMENT );
        this.isUnset = isUnset;
    }


    public PolyDocument( Map<PolyString, PolyValue> value ) {
        this( value, false );
    }


    public PolyDocument( PolyString key, PolyValue value ) {
        this( new HashMap<>() {{
            put( key, value );
        }} );
    }


    public static PolyDocument ofUnset() {
        return new PolyDocument( new HashMap<>(), true );
    }


    public static PolyDocument ofDocument( Map<PolyString, PolyValue> value ) {
        return new PolyDocument( value );
    }


    @SafeVarargs
    public PolyDocument( Pair<PolyString, PolyValue>... value ) {
        this( Map.ofEntries( value ) );
    }


    public static PolyDocument parse( String string ) {
        throw new GenericRuntimeException( "error on parsing Document" );
    }


    public static PolyDocument convert( Object value ) {
        if ( value == null ) {
            return null;
        }
        if ( value instanceof PolyValue ) {
            if ( ((PolyValue) value).isDocument() ) {
                return ((PolyValue) value).asDocument();
            }
        }
        throw new NotImplementedException( "convert value to Document" );
    }


    @Override
    public @NotNull String toTypedJson() {
        try {
            return JSON_WRAPPER.writerFor( new TypeReference<PolyDocument>() {
            } ).writeValueAsString( this );
        } catch ( JsonProcessingException e ) {
            log.warn( "Error on serializing typed JSON." );
            return null;
        }
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyDocument.class, super.asExpression(), Expressions.constant( isUnset ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyDocument.class );
    }


    public static class PolyDocumentSerializerDef extends SimpleSerializerDef<PolyDocument> {

        @Override
        protected BinarySerializer<PolyDocument> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyDocument item ) {
                    out.writeBoolean( item.isUnset );
                    if ( item.isUnset ) {
                        return;
                    }
                    out.writeLong( item.size() );
                    for ( Entry<PolyString, PolyValue> entry : item.entrySet() ) {
                        out.writeUTF8( PolySerializable.serialize( serializer, entry.getKey() ) );
                        out.writeUTF8( PolySerializable.serialize( serializer, entry.getValue() ) );
                    }
                }


                @Override
                public PolyDocument decode( BinaryInput in ) throws CorruptedDataException {
                    Map<PolyString, PolyValue> map = new HashMap<>();
                    if ( in.readBoolean() ) {
                        return PolyDocument.ofUnset();
                    }
                    long size = in.readLong();
                    for ( long i = 0; i < size; i++ ) {
                        map.put(
                                PolySerializable.deserialize( in.readUTF8(), serializer ).asString(),
                                PolySerializable.deserialize( in.readUTF8(), serializer ) );
                    }
                    return PolyDocument.ofDocument( map );
                }
            };
        }

    }


    @Override
    public String toString() {
        return "{" + map.entrySet().stream().map( e -> String.format( "%s:%s", e.getKey(), e.getValue() ) ).collect( Collectors.joining( "," ) ) + "}";
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
            return PolyMap.of( values, MapType.DOCUMENT );
        }


        private Pair<PolyValue, PolyValue> deserializeElement( DeserializationContext ctxt, JsonNode element ) throws IOException {
            PolyValue key = ctxt.readTreeAsValue( element.get( "_k" ), PolyValue.class );
            PolyValue value = ctxt.readTreeAsValue( element.get( "_v" ), PolyValue.class );
            return Pair.of( key, value );
        }

    }


    static class PolyDocumentDeserializer extends StdDeserializer<PolyMap<?, ?>> {


        protected PolyDocumentDeserializer() {
            super( PolyDocument.class );
        }


        @Override
        public Object deserializeWithType( JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer ) throws IOException {
            return deserialize( p, ctxt );
        }


        @Override
        public PolyDocument deserialize( JsonParser p, DeserializationContext ctxt ) throws IOException {
            JsonNode node = p.getCodec().readTree( p );
            PolyMap<PolyString, PolyValue> value = ctxt.readTreeAsValue( node, PolyMap.class );
            return PolyDocument.ofDocument( value );
        }


    }

}
