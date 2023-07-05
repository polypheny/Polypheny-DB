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
import org.polypheny.db.util.ConversionUtil;

@EqualsAndHashCode(callSuper = true)
@Value
public class PolyBinary extends PolyValue {

    public static final PolyBinary EMPTY = new PolyBinary( ByteString.EMPTY );
    public ByteString value;


    public PolyBinary( ByteString value ) {
        super( PolyType.BINARY );
        this.value = value;
    }


    public static PolyBinary of( ByteString value ) {
        return new PolyBinary( value );
    }


    public static PolyBinary of( byte[] value ) {
        return new PolyBinary( new ByteString( value ) );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        return 0;
    }


    /**
     * Converts this bit string to a hex string, such as "7AB".
     */
    public String toHexString() {
        byte[] bytes = value.getBytes();
        String s = ConversionUtil.toStringFromByteArray( bytes, 16 );
        switch ( value.getBytes().length % 8 ) {
            case 1: // B'1' -> X'1'
            case 2: // B'10' -> X'2'
            case 3: // B'100' -> X'4'
            case 4: // B'1000' -> X'8'
                return s.substring( 1 );
            case 5: // B'10000' -> X'10'
            case 6: // B'100000' -> X'20'
            case 7: // B'1000000' -> X'40'
            case 0: // B'10000000' -> X'80', and B'' -> X''
                return s;
        }
        if ( (value.getBytes().length % 8) == 4 ) {
            return s.substring( 1 );
        } else {
            return s;
        }
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


    public int getBitCount() {
        return value.getBytes().length;
    }


    @Override
    public @NotNull Long deriveByteSize() {
        return (long) (value == null ? 1 : value.getBytes().length);
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
