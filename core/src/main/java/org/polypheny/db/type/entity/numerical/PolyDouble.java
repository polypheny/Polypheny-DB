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
import lombok.EqualsAndHashCode;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;

@EqualsAndHashCode(callSuper = true)
public class PolyDouble extends PolyNumber {

    @Serialize
    @JsonProperty
    public Double value;


    @JsonCreator
    public PolyDouble( @Deserialize("value") @JsonProperty("value") Double value ) {
        super( PolyType.DOUBLE );
        this.value = value;
    }


    public static PolyDouble of( Double value ) {
        return new PolyDouble( value );
    }


    public static PolyDouble of( Number value ) {
        return new PolyDouble( value.doubleValue() );
    }


    public static PolyDouble ofNullable( Number value ) {
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

        return ObjectUtils.compare( value, o.asNumber().DoubleValue() );
    }


    public static PolyDouble convert( @Nullable Object object ) {
        if ( object == null ) {
            return null;
        }

        if ( object instanceof PolyValue ) {
            if ( ((PolyValue) object).isDouble() ) {
                return ((PolyValue) object).asDouble();
            } else if ( ((PolyValue) object).isNumber() ) {
                return PolyDouble.of( ((PolyValue) object).asNumber().DoubleValue() );
            }
        }

        throw new GenericRuntimeException( "Could not convert Integer" );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyDouble.class, Expressions.constant( value ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyDouble.class );
    }


    @Override
    public int intValue() {
        return value.intValue();
    }


    @Override
    public long longValue() {
        return value.longValue();
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
    public PolyDouble increment() {
        return PolyDouble.of( value + 1 );
    }


    @Override
    public @NotNull PolyDouble divide( @NotNull PolyNumber other ) {
        return PolyDouble.of( value / other.doubleValue() );
    }


    @Override
    public @NotNull PolyDouble multiply( @NotNull PolyNumber other ) {
        return PolyDouble.of( value * other.doubleValue() );
    }


    @Override
    public @NotNull PolyNumber plus( @NotNull PolyNumber b1 ) {
        return PolyDouble.of( value + b1.doubleValue() );
    }


    @Override
    public @NotNull PolyNumber subtract( @NotNull PolyNumber b1 ) {
        return PolyDouble.of( value - b1.doubleValue() );
    }


    @Override
    public PolyNumber negate() {
        return PolyDouble.of( -value );
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return 8L;
    }


    @Override
    public Object toJava() {
        return value;
    }


    public static class PolyDoubleSerializerDef extends SimpleSerializerDef<PolyDouble> {

        @Override
        protected BinarySerializer<PolyDouble> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyDouble item ) {
                    out.writeUTF8Nullable( item.value == null ? null : item.value.toString() );
                }


                @Override
                public PolyDouble decode( BinaryInput in ) throws CorruptedDataException {
                    @Nullable String d = in.readUTF8Nullable();
                    return new PolyDouble( d == null ? null : Double.valueOf( d ) );
                }
            };
        }

    }


    @Override
    public String toString() {
        return value.toString();
    }

}
