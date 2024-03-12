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
import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;
import lombok.Value;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BitString;

@Value
public class PolyBinary extends PolyValue {

    public static final PolyBinary EMPTY = new PolyBinary( new byte[0], null );

    @JsonProperty()
    @JsonSerialize(using = ByteStringSerializer.class)
    @JsonDeserialize(using = ByteStringDeserializer.class)
    public byte[] value;

    @JsonProperty()
    @Nullable
    public Integer count;


    /**
     * Creates a PolyBinary.
     *
     * @param value The value of the PolyBinary
     */
    @JsonCreator
    public PolyBinary( @JsonProperty("value") byte[] value, @JsonProperty("count") Integer count ) {
        super( PolyType.BINARY );
        this.value = value;
        this.count = count;
    }


    public static PolyBinary of( byte[] value, int count ) {
        return new PolyBinary( value, count );
    }


    public PolyBinary of( BitString value ) {
        return PolyBinary.of( value.getAsByteArray() );
    }


    public static PolyBinary of( ByteString value ) {
        return new PolyBinary( value.getBytes(), null );
    }


    public static PolyBinary of( byte[] value ) {
        return new PolyBinary( value, null );
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
        String s = BaseEncoding.base16().omitPadding().encode( value );
        BitSet set = BitSet.valueOf( value );
        String out = switch ( set.length() ) { // B'1' -> X'1'
            // B'10' -> X'2'
            // B'100' -> X'4'
            case 1, 2, 3, 4 -> // B'1000' -> X'8'
                    s.substring( 1 ); // B'10000' -> X'10'
            // B'100000' -> X'20'
            // B'1000000' -> X'40'
            case 5, 6, 7, 0 -> // B'10000000' -> X'80', and B'' -> X''
                    s;
            default -> s;
        };

        if ( count != null && count != out.length() ) {
            if ( count > out.length() ) {
                return StringUtils.leftPad( out, count, "0" );
            }
            return out.substring( out.length() - count );
        }
        return out;
    }


    @Override
    public @NotNull PolyString asString() {
        return value == null ? PolyString.of( null ) : PolyString.of( toHexString() );
    }


    @Override
    public Object toJava() {
        return value;
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( PolyBinary.class, "of", Expressions.constant( value ) );
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
        return BitString.createFromBytes( value ).getBitCount();
    }


    @Override
    public @NotNull Long deriveByteSize() {
        return (long) (value == null ? 1 : value.length);
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        if ( !super.equals( o ) ) {
            return false;
        }

        PolyBinary that = (PolyBinary) o;

        if ( Arrays.equals( value, that.value ) ) {
            return true;
        }

        return Objects.equals( new ByteString( value ).toBase64String().toUpperCase(), new ByteString( that.value ).toBase64String().toUpperCase() );
    }


    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }


    /**
     * Returns a byte-string padded with zero bytes to make it at least a given length,
     */
    public PolyBinary padRight( int length ) {
        if ( value == null ) {
            return PolyBinary.of( (byte[]) null );
        }
        if ( this.value.length >= length ) {
            return this;
        }
        return PolyBinary.of( new ByteString( Arrays.copyOf( value, length ) ) );
    }


    public int length() {
        return value.length;
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
