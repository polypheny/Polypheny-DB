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

package org.polypheny.db.type.entity.numerical;

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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;

@Value
public class PolyInteger extends PolyNumber {

    public static final PolyInteger ZERO = PolyInteger.of( 0 );
    @Serialize
    @JsonProperty
    @Nullable
    public Integer value;


    public PolyInteger( @JsonProperty("value") @Deserialize("value") @Nullable Integer value ) {
        super( PolyType.INTEGER );
        this.value = value;
    }


    @Override
    public @Nullable String toJson() {
        return value == null ? JsonToken.VALUE_NULL.asString() : String.valueOf( value );
    }


    public static PolyInteger convert( @Nullable PolyValue object ) {
        if ( object == null ) {
            return null;
        }

        if ( object.isInteger() ) {
            return object.asInteger();
        } else if ( object.isNumber() ) {
            return PolyInteger.ofNullable( object.asNumber().NumberValue() );
        } else if ( object.isTemporal() ) {
            return PolyInteger.of( object.asTemporal().getMillisSinceEpoch() );
        } else if ( object.isString() ) {
            return PolyInteger.of( Integer.parseInt( object.asString().value ) );
        }

        throw new GenericRuntimeException( getConvertError( object, PolyInteger.class ) );
    }


    public static PolyInteger of( byte value ) {
        return new PolyInteger( (int) value );
    }


    public static PolyInteger of( short value ) {
        return new PolyInteger( (int) value );
    }


    public static PolyInteger of( int value ) {
        return new PolyInteger( value );
    }


    public static PolyInteger of( Integer value ) {
        return new PolyInteger( value );
    }


    public static PolyInteger of( Number value ) {
        return new PolyInteger( value.intValue() );
    }


    public static PolyInteger ofNullable( Number value ) {
        return value == null ? null : of( value );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyInteger.class, Expressions.constant( value ) );
    }


    @Override
    public int hashCode() {
        return Objects.hash( super.hashCode(), value );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !o.isNumber() ) {
            return -1;
        }

        return PolyNumber.compareTo( this, o.asNumber() );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyInteger.class );
    }


    @Override
    public @NotNull Long deriveByteSize() {
        return 4L;
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
    public int intValue() {
        return value;
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
        return value == null ? null : new BigDecimal( value );
    }


    @Override
    public PolyInteger increment() {
        return PolyInteger.of( value + 1 );
    }


    @Override
    public @NotNull PolyNumber divide( @NotNull PolyNumber other ) {
        return PolyBigDecimal.of( bigDecimalValue().divide( other.bigDecimalValue(), MathContext.DECIMAL64 ) );
    }


    @Override
    public @NotNull PolyNumber multiply( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().multiply( other.bigDecimalValue() ) ) : PolyInteger.of( value * other.intValue() );
    }


    @Override
    public @NotNull PolyNumber plus( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().add( other.bigDecimalValue() ) ) : PolyInteger.of( value + other.intValue() );
    }


    @Override
    public @NotNull PolyNumber subtract( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().subtract( other.bigDecimalValue() ) ) : PolyInteger.of( value - other.intValue() );
    }


    @Override
    public PolyNumber negate() {
        return PolyInteger.of( -value );
    }


    public static class PolyIntegerSerializerDef extends SimpleSerializerDef<PolyInteger> {

        @Override
        protected BinarySerializer<PolyInteger> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyInteger item ) {
                    if ( item == null ) {
                        out.writeBoolean( true );
                        return;
                    }
                    out.writeBoolean( false );

                    out.writeInt( item.value );
                }


                @Override
                public PolyInteger decode( BinaryInput in ) throws CorruptedDataException {
                    if ( in.readBoolean() ) {
                        return null;
                    }
                    return new PolyInteger( in.readInt() );
                }
            };
        }

    }


    @Override
    public String toString() {
        return value.toString();
    }

}
