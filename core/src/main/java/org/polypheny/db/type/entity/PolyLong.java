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
import com.fasterxml.jackson.core.JsonToken;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Objects;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;

@Value
public class PolyLong extends PolyNumber {

    @Serialize
    @JsonProperty
    public Long value;


    /**
     * Creates a PolyLong, which is a wrapper for {@link Long}.
     *
     * @param value The value of the PolyLong
     */
    @JsonCreator
    public PolyLong( @JsonProperty("value") @Deserialize("value") Long value ) {
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


    public static PolyLong from( PolyValue value ) {
        if ( PolyType.NUMERIC_TYPES.contains( value.type ) ) {
            return PolyLong.of( value.asNumber().longValue() );
        }

        throw new GenericRuntimeException( String.format( "%s does not support conversion to %s.", value, value.type ) );
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


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null ) {
            return false;
        }

        if ( !(o instanceof PolyValue val) ) {
            return false;
        }

        if ( val.isNull() ) {
            return false;
        }

        if ( val.isNumber() ) {
            return PolyNumber.compareTo( this, val.asNumber() ) == 0;
        }

        return false;
    }


    public static PolyLong convert( Object value ) {
        if ( value == null ) {
            return null;
        }

        if ( value instanceof PolyValue ) {
            if ( ((PolyValue) value).isLong() ) {
                return PolyLong.of( ((PolyValue) value).asNumber().longValue() );
            } else if ( ((PolyValue) value).isTemporal() ) {
                return PolyLong.of( ((PolyValue) value).asTemporal().getMillisSinceEpoch() );
            } else if ( ((PolyValue) value).isString() ) {
                return PolyLong.of( Long.parseLong( ((PolyValue) value).asString().value ) );
            }
        }

        throw new NotImplementedException( "convert " + PolyTimestamp.class.getSimpleName() );
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


}
