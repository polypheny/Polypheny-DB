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
import com.fasterxml.jackson.core.JsonToken;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.def.SimpleSerializerDef;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Objects;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;

@Value
public class PolyLong extends PolyNumber {

    @Serialize
    @JsonProperty
    @Nullable
    public Long value;


    /**
     * Creates a PolyLong, which is a wrapper for {@link Long}.
     *
     * @param value The value of the PolyLong
     */
    @JsonCreator
    public PolyLong( @JsonProperty("value") @Deserialize("value") @Nullable Long value ) {
        super( PolyType.BIGINT );
        this.value = value;
    }


    public static PolyLong of( long value ) {
        return new PolyLong( value );
    }


    public static PolyLong of( Long value ) {
        return new PolyLong( value );
    }


    public static PolyLong of( Number value ) {
        return new PolyLong( value.longValue() );
    }


    public static PolyLong ofNullable( Number value ) {
        return value == null ? null : of( value );
    }


    @Override
    public @Nullable String toJson() {
        return value == null ? JsonToken.VALUE_NULL.asString() : String.valueOf( value );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !o.isNumber() ) {
            return -1;
        }
        return ObjectUtils.compare( value, o.asNumber().LongValue() );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyLong.class, Expressions.constant( value ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyLong.class );
    }


    @Override
    public int intValue() {
        return Math.toIntExact( value );
    }


    @Override
    public long longValue() {
        return value;
    }


    @Override
    public float floatValue() {
        return value.floatValue();
    }


    @Override
    public double doubleValue() {
        return value;
    }


    @Override
    public BigDecimal bigDecimalValue() {
        return BigDecimal.valueOf( value );
    }


    @Override
    public PolyLong increment() {
        return PolyLong.of( value + 1 );
    }


    @Override
    public @NotNull PolyNumber divide( @NotNull PolyNumber other ) {
        return PolyBigDecimal.of( bigDecimalValue().divide( other.bigDecimalValue(), MathContext.DECIMAL64 ) );
    }


    @Override
    public @NotNull PolyNumber multiply( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().multiply( other.bigDecimalValue() ) ) : PolyLong.of( value * other.longValue() );
    }


    @Override
    public @NotNull PolyNumber plus( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().add( other.bigDecimalValue() ) ) : PolyLong.of( value + other.longValue() );
    }


    @Override
    public @NotNull PolyNumber subtract( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().subtract( other.bigDecimalValue() ) ) : PolyLong.of( value - other.longValue() );
    }


    @Override
    public PolyNumber negate() {
        return PolyLong.of( -value );
    }


    public static PolyLong convert( PolyValue value ) {
        if ( value == null ) {
            return null;
        }

        if ( value.isLong() ) {
            return PolyLong.of( value.asNumber().longValue() );
        } else if ( value.isTemporal() ) {
            return PolyLong.of( value.asTemporal().getMillisSinceEpoch() );
        } else if ( value.isString() ) {
            return PolyLong.of( Long.parseLong( value.asString().value ) );
        } else if ( value.isNumber() ) {
            return PolyLong.of( value.asNumber().LongValue() );
        }

        throw new GenericRuntimeException( getConvertError( value, PolyLong.class ) );
    }


    @Override
    public @NotNull Long deriveByteSize() {
        return 16L;
    }


    @Override
    public Object toJava() {
        return value;
    }


    @Override
    public int hashCode() {
        return Objects.hash( super.hashCode(), value );
    }


    @Override
    public String toString() {
        return value.toString();
    }


    public static class PolyLongSerializerDef extends SimpleSerializerDef<PolyLong> {

        @Override
        protected BinarySerializer<PolyLong> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyLong item ) {
                    if ( item.value == null ) {
                        out.writeBoolean( true );
                    } else {
                        out.writeBoolean( false );
                        out.writeLong( item.value );
                    }
                }


                @Override
                public PolyLong decode( BinaryInput in ) throws CorruptedDataException {
                    boolean isNull = in.readBoolean();
                    if ( isNull ) {
                        return null;
                    }
                    return PolyLong.of( in.readLong() );
                }
            };
        }

    }

}
