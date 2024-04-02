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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Charsets;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.def.SimpleSerializerDef;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Util;

@Value
public class PolyString extends PolyValue {

    public static final Charset DEFAULT_CHARSET = Charsets.UTF_8;

    @Serialize
    @JsonProperty
    public String value;

    public Charset charset;


    public PolyString( @JsonProperty("value") @Deserialize("value") String value ) {
        this( value, DEFAULT_CHARSET );
    }


    public PolyString( String value, Charset charset ) {
        super( PolyType.VARCHAR );
        this.value = value;
        this.charset = charset;
    }


    public static PolyString of( String value, @Nullable String charset ) {
        return new PolyString( value, charset == null ? DEFAULT_CHARSET : Charset.forName( charset ) );
    }


    public static PolyString of( String value, @Nullable Charset charset ) {
        return new PolyString( value, charset == null ? DEFAULT_CHARSET : charset );
    }


    public static PolyString of( String value ) {
        return new PolyString( value );
    }


    public static PolyString ofNullable( String value ) {
        return of( value );
    }


    @Override
    public @Nullable String toJson() {
        return value == null ? JsonToken.VALUE_NULL.asString() : value;
    }


    public @Nullable String toQuotedJson() {
        return value == null ? JsonToken.VALUE_NULL.asString() : "\"" + value + "\"";
    }


    public static PolyString concat( List<PolyString> strings ) {
        return PolyString.of( strings.stream().map( s -> s.value ).collect( Collectors.joining() ) );
    }


    public static PolyString convert( @Nullable PolyValue value ) {
        if ( value == null ) {
            return null;
        }

        if ( value.isString() ) {
            return value.asString();
        } else if ( value.isDocument() ) {
            return PolyString.of( value.asDocument().toJson() );
        } else if ( value.isNumber() ) {
            return PolyString.of( value.toJson() );
        } else if ( value.isBoolean() ) {
            return PolyString.of( value.asBoolean().value.toString() );
        }

        throw new GenericRuntimeException( getConvertError( value, PolyString.class ) );
    }


    public static PolyString convert( char value ) {
        return PolyString.of( String.valueOf( value ) );
    }


    public static PolyString join( String delimiter, List<PolyString> strings ) {
        return PolyString.of( strings.stream().map( s -> s.value ).collect( Collectors.joining( delimiter ) ) );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyString.class, Expressions.constant( value ) );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        return ObjectUtils.compare( value, o.asString().value );
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        PolyString that = (PolyString) o;
        return Objects.equals( value, that.value );
    }


    @Override
    public int hashCode() {
        return Objects.hash( super.hashCode(), value );
    }


    @Override
    public @NotNull Long deriveByteSize() {
        return (long) (value == null ? 1 : value.getBytes( charset ).length);
    }


    @Override
    public boolean isNull() {
        return value == null;
    }


    @Override
    public Object toJava() {
        return value;
    }


    @Override
    public PolySerializable copy() {
        return null;
    }


    public String asCharset( String charset ) {
        return asCharset( Charset.forName( charset ) );
    }


    public String asCharset( Charset charset ) {
        if ( this.charset.equals( charset ) ) {
            return value;
        }
        return new String( value.getBytes( this.charset ), charset );
    }


    public String toPrefixedString() {
        StringBuilder ret = new StringBuilder();
        if ( null != charset && charset != PolyString.DEFAULT_CHARSET ) {
            ret.append( "_" );
            ret.append( prettyCharset( charset ) );
        }
        ret.append( "'" );
        ret.append( Util.replace( getValue(), "'", "''" ) );
        ret.append( "'" );

        return ret.toString();
    }


    private String prettyCharset( Charset charset ) {
        if ( Charsets.UTF_8.equals( charset ) ) {
            return "UTF8";
        } else if ( Charsets.UTF_16.equals( charset ) ) {
            return "UTF16";
        }
        return charset.displayName();
    }


    public static class PolyStringSerializerDef extends SimpleSerializerDef<PolyString> {

        @Override
        protected BinarySerializer<PolyString> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyString item ) {
                    out.writeUTF8Nullable( item.value );
                }


                @Override
                public PolyString decode( BinaryInput in ) throws CorruptedDataException {
                    return PolyString.of( in.readUTF8Nullable() );
                }
            };
        }

    }


    public String toTypedString( boolean prefix ) {
        StringBuilder ret = new StringBuilder();
        if ( prefix && (null != charset && charset != PolyString.CHARSET) ) {
            ret.append( "_" );
            ret.append( charset.displayName() );
        }
        ret.append( "'" );
        ret.append( Util.replace( getValue(), "'", "''" ) );
        ret.append( "'" );
        return ret.toString();
    }


    @Override
    public String toString() {
        return value;
    }

}
