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
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Expression;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;

@EqualsAndHashCode(callSuper = true)
@Value(staticConstructor = "of")
public class PolyBinary extends PolyValue {

    public ByteString value;


    public PolyBinary( ByteString value ) {
        super( PolyType.BINARY );
        this.value = value;
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        return 0;
    }


    @Override
    public Expression asExpression() {
        return null;
    }


    @Override
    public PolySerializable copy() {
        return null;
    }


    @Override
    public String toString() {
        return value.toBase64String();
    }


    public static class PolyBinarySerializer implements JsonSerializer<PolyBinary>, JsonDeserializer<PolyBinary> {

        @Override
        public JsonElement serialize( PolyBinary src, Type typeOfSrc, JsonSerializationContext context ) {
            return new JsonPrimitive( src.value.toBase64String() );
        }


        @Override
        public PolyBinary deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            return PolyBinary.of( ByteString.ofBase64( json.getAsString() ) );
        }

    }

}
