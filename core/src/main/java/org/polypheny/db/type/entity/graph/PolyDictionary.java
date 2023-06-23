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

package org.polypheny.db.type.entity.graph;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SimpleSerializerDef;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.BuiltInMethod;

public class PolyDictionary extends PolyMap<PolyString, PolyValue> {

    private static Gson gson = new GsonBuilder().serializeNulls().enableComplexMapKeySerialization().create();


    public PolyDictionary( Map<PolyString, PolyValue> map ) {
        super( map );
    }


    public PolyDictionary() {
        this( Map.of() );
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


    public static PolyDictionary fromString( String json ) {
        return gson.fromJson( json, PolyDictionary.class );
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


    public static class PolyDictionarySerializer implements JsonSerializer<PolyDictionary>, JsonDeserializer<PolyDictionary> {

        @Override
        public PolyDictionary deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            JsonObject object = json.getAsJsonObject();
            Map<PolyString, PolyValue> map = new HashMap<>();
            for ( Entry<String, JsonElement> entry : object.entrySet() ) {
                map.put( PolyString.of( entry.getKey() ), PolyValue.GSON.fromJson( entry.getValue(), PolyValue.class ) );
            }
            return new PolyDictionary( map );
        }


        @Override
        public JsonElement serialize( PolyDictionary src, Type typeOfSrc, JsonSerializationContext context ) {
            JsonObject object = new JsonObject();
            for ( Entry<PolyString, PolyValue> entry : src.map.entrySet() ) {
                object.add( entry.getKey().toString(), context.serialize( entry.getValue(), PolyValue.class ) );
            }
            return object;
        }

    }

}
