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

package org.polypheny.db.type.entity;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
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
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Delegate;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

@EqualsAndHashCode(callSuper = true)
@Value(staticConstructor = "copyOf")
public class PolyList<E extends PolyValue> extends PolyValue implements List<E> {


    @Delegate
    @Serialize
    public List<E> value;


    public PolyList( @Deserialize("value") List<E> value ) {
        super( PolyType.ARRAY );
        this.value = new ArrayList<>( value );
    }


    @SafeVarargs
    public PolyList( E... value ) {
        this( Arrays.asList( value ) );
    }


    public static <E extends PolyValue> PolyList<E> of( Collection<E> value ) {
        return new PolyList<>( new ArrayList<>( value ) );
    }


    @SafeVarargs
    public static <E extends PolyValue> PolyList<E> of( E... values ) {
        return new PolyList<>( values );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyList.class, value.stream().map( e -> e == null ? Expressions.constant( null ) : e.asExpression() ).collect( Collectors.toList() ) );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        if ( value.size() != o.asList().value.size() ) {
            return value.size() - o.asList().value.size();
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
    public String toJson() {
        return value == null ? "null" : value.stream().map( e -> e == null ? "null" : e.isString() ? String.format( "\"%s\"", e.asString().value ) : e.toJson() ).collect( Collectors.joining( ",", "[", "]" ) );
    }


    public static class PolyListSerializerDef extends SimpleSerializerDef<PolyList<?>> {

        @Override
        protected BinarySerializer<PolyList<?>> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyList<?> item ) {
                    out.writeLong( item.size() );
                    for ( PolyValue entry : item ) {
                        out.writeUTF8( PolySerializable.serialize( serializer, entry ) );
                    }
                }


                @Override
                public PolyList<?> decode( BinaryInput in ) throws CorruptedDataException {
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


    public static class PolyListSerializer<E extends PolyValue> implements JsonSerializer<PolyList<E>>, JsonDeserializer<PolyList<E>> {

        private static final String CLASSNAME = "className";
        private static final String INSTANCE = "instance";
        private static final String SIZE = "size";


        @Override
        public JsonElement serialize( PolyList<E> src, Type typeOfSrc, JsonSerializationContext context ) {
            if ( src == null ) {
                return JsonNull.INSTANCE;
            }

            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty( SIZE, src.size() );
            jsonObject.addProperty( CLASSNAME, src.isEmpty() || src.value.get( 0 ) == null ? PolyValue.class.getName() : src.value.get( 0 ).getClass().getName() );
            JsonArray jsonArray = new JsonArray();
            for ( PolyValue entry : src.value ) {
                if ( entry == null ) {
                    jsonArray.add( JsonNull.INSTANCE );
                    continue;
                }
                jsonArray.add( context.serialize( entry, entry.getClass() ) );
            }
            jsonObject.add( INSTANCE, jsonArray );
            return jsonObject;
        }


        @Override
        public PolyList<E> deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            String className = jsonObject.get( CLASSNAME ).getAsString();
            int size = jsonObject.get( SIZE ).getAsInt();
            List<E> list = new ArrayList<>();

            try {
                Type type = Class.forName( className );
                JsonArray jsonArray = jsonObject.get( INSTANCE ).getAsJsonArray();
                for ( JsonElement element : jsonArray ) {
                    list.add( context.deserialize( element, type ) );
                }
            } catch ( ClassNotFoundException e ) {
                throw new JsonParseException( "Invalid class name: " + className, e );
            }

            return PolyList.of( list );
        }

    }

}
