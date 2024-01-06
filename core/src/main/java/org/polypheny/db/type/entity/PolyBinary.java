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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BitString;
import org.polypheny.db.util.ConversionUtil;

@EqualsAndHashCode(callSuper = true)
@Value
public class PolyBinary extends PolyValue {

    public static final PolyBinary EMPTY = new PolyBinary( ByteString.EMPTY );

    @JsonProperty()
    @JsonSerialize(using = ByteStringSerializer.class)
    @JsonDeserialize(using = ByteStringDeserializer.class)
    public ByteString value;


    /**
     * Creates a PolyBinary.
     *
     * @param value The value of the PolyBinary
     */
    @JsonCreator
    public PolyBinary( @JsonProperty("value") ByteString value ) {
        super( PolyType.BINARY );
        this.value = value;
    }


    public static PolyBinary of( ByteString value ) {
        return new PolyBinary( value );
    }


    public static PolyBinary of( byte[] value ) {
        return new PolyBinary( new ByteString( value ) );
    }


    public static PolyBinary ofNullable( byte[] value ) {
        return value == null ? null : PolyBinary.of( value );
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
    public @NotNull PolyString asString() {
        return value == null ? PolyString.of( null ) : PolyString.of( toHexString() );
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( PolyBinary.class, "of", Expressions.constant( value.getBytes() ) );
    }


    @Override
    public PolySerializable copy() {
        return null;
    }


    @Override
    public String toString() {
        return value.toString();
    }


    public int getBitCount() {
        return BitString.createFromBytes( value.getBytes() ).getBitCount();
    }


    @Override
    public @NotNull Long deriveByteSize() {
        return (long) (value == null ? 1 : value.getBytes().length);
    }


    /**
     * Returns a byte-string padded with zero bytes to make it at least a given length,
     */
    public PolyBinary padRight( int length ) {
        if ( value == null ) {
            return PolyBinary.of( (ByteString) null );
        }
        if ( this.value.length() >= length ) {
            return this;
        }
        return PolyBinary.of( new ByteString( Arrays.copyOf( value.getBytes(), length ) ) );
    }


    public static class ByteStringSerializer extends StdSerializer<ByteString> {

        public ByteStringSerializer() {
            super( ByteString.class );
        }


        @Override
        public void serialize( ByteString value, JsonGenerator gen, SerializerProvider provider ) throws IOException {
            gen.writeBinary( value.getBytes() );
        }

    }


    public static class ByteStringDeserializer extends StdDeserializer<ByteString> {

        public ByteStringDeserializer() {
            super( ByteString.class );
        }


        @Override
        public ByteString deserialize( JsonParser p, DeserializationContext ctxt ) throws IOException, JacksonException {
            return new ByteString( p.getBinaryValue() );
        }

    }


}
