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

package org.polypheny.db.type.entity.document;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SimpleSerializerDef;
import io.activej.serializer.annotations.Deserialize;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.Pair;

@Slf4j
@EqualsAndHashCode(callSuper = true)
public class PolyDocument extends PolyMap<PolyString, PolyValue> {


    public PolyDocument( @Deserialize("map") Map<PolyString, PolyValue> value ) {
        super( value, PolyType.DOCUMENT );
    }


    public PolyDocument( PolyString key, PolyValue value ) {
        this( new HashMap<>() {{
            put( key, value );
        }} );
    }


    public static PolyDocument ofDocument( Map<PolyString, PolyValue> value ) {
        return new PolyDocument( value );
    }


    @SafeVarargs
    public PolyDocument( Pair<PolyString, PolyValue>... value ) {
        this( Map.ofEntries( value ) );
    }


    public static PolyDocument parse( String string ) {
        log.warn( "todo wfwefcw" );
        return null;
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyDocument.class, super.asExpression() );
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
                    out.writeLong( item.size() );
                    for ( Entry<PolyString, PolyValue> entry : item.entrySet() ) {
                        out.writeUTF8( PolySerializable.serialize( serializer, entry.getKey() ) );
                        out.writeUTF8( PolySerializable.serialize( serializer, entry.getValue() ) );
                    }
                }


                @Override
                public PolyDocument decode( BinaryInput in ) throws CorruptedDataException {
                    Map<PolyString, PolyValue> map = new HashMap<>();
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


    @Override
    public String toJson() {
        return "{" + map.entrySet().stream().map( e -> String.format( (e.getValue() != null && e.getValue().isString() ? "%s:\"%s\"" : "%s:%s"), e.getKey().toJson(), e.getValue() == null ? JsonNull.INSTANCE.toString() : e.getValue().toJson() ) ).collect( Collectors.joining( "," ) ) + "}";
    }


    public static class PolyDocumentSerializer implements JsonSerializer<PolyDocument>, JsonDeserializer<PolyDocument> {

        @Override
        public PolyDocument deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            JsonObject object = json.getAsJsonObject();
            Map<PolyString, PolyValue> map = new HashMap<>();
            for ( Entry<String, JsonElement> entry : object.entrySet() ) {
                map.put( PolyString.of( entry.getKey() ), PolyValue.GSON.fromJson( entry.getValue(), PolyValue.class ) );
            }
            return new PolyDocument( map );
        }


        @Override
        public JsonElement serialize( PolyDocument src, Type typeOfSrc, JsonSerializationContext context ) {
            JsonObject object = new JsonObject();
            for ( Entry<PolyString, PolyValue> entry : src.map.entrySet() ) {
                object.add( entry.getKey().toString(), entry.getValue().isString() ? new JsonPrimitive( entry.getValue().asString().value ) : context.serialize( entry.getValue(), PolyValue.class ) );
            }
            return object;
        }

    }

}
