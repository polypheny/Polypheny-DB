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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;

@EqualsAndHashCode(callSuper = true)
public class PolySymbol extends PolyValue {

    public Enum<?> value;


    public PolySymbol( Enum<?> value ) {
        super( PolyType.SYMBOL );
        this.value = value;
    }


    public static PolySymbol of( Enum<?> value ) {
        return new PolySymbol( value );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        return ((Enum) value).compareTo( o.asSymbol().value );
    }


    @Override
    public Expression asExpression() {
        return Expressions.constant( value );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolySymbol.class );
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    public static class PolySymbolSerializer implements JsonSerializer<PolySymbol>, JsonDeserializer<PolySymbol> {


        @Override
        public JsonElement serialize( PolySymbol src, Type typeOfSrc, JsonSerializationContext context ) {
            JsonObject object = new JsonObject();
            object.addProperty( "type", src.value.getClass().getName() );
            object.addProperty( "data", src.value.name() );
            return object;
        }


        @Override
        public PolySymbol deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            JsonObject object = json.getAsJsonObject();
            String className = object.get( "type" ).getAsString();
            try {
                Class<Enum<?>> clazz = (Class<Enum<?>>) Class.forName( className );
                String name = object.get( "data" ).getAsString();
                return PolySymbol.of( Arrays.stream( clazz.getEnumConstants() ).filter( s -> s.name().equals( name ) ).findFirst().orElse( null ) );
            } catch ( ClassNotFoundException e ) {
                throw new JsonParseException( "Invalid class name: " + className, e );
            }
        }

    }

}
