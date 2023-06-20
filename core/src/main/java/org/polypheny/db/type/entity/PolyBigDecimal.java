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
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SimpleSerializerDef;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Objects;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.category.PolyNumber;

@Value
public class PolyBigDecimal extends PolyNumber {

    public BigDecimal value;


    public PolyBigDecimal( BigDecimal value ) {
        super( PolyType.DECIMAL );
        this.value = value;
    }


    public static PolyBigDecimal of( BigDecimal value ) {
        return new PolyBigDecimal( value );
    }


    public static PolyBigDecimal of( String value ) {
        return new PolyBigDecimal( new BigDecimal( value ) );
    }


    public static PolyBigDecimal of( long value ) {
        return new PolyBigDecimal( BigDecimal.valueOf( value ) );
    }


    public static PolyBigDecimal of( double value ) {
        return new PolyBigDecimal( BigDecimal.valueOf( value ) );
    }


    public static PolyBigDecimal of( Number value, int precision, int scale ) {
        return PolyBigDecimal.of( value.doubleValue() );
    }


    public static PolyBigDecimal convert( Object value ) {
        if ( value instanceof PolyNumber ) {
            return PolyBigDecimal.of( ((PolyNumber) value).bigDecimalValue() );
        } else if ( value instanceof PolyValue ) {
            return null;
        }
        return null;
    }


    public static PolyBigDecimal convert( PolyValue value ) {
        if ( value.isNumber() ) {
            return PolyBigDecimal.of( value.asNumber().bigDecimalValue() );
        } else if ( value.isTemporal() ) {
            return PolyBigDecimal.of( value.asTemporal().getSinceEpoch() );
        } else if ( value.isString() ) {
            return PolyBigDecimal.of( value.asString().value );
        }
        return null;
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
        return value.doubleValue();
    }


    @Override
    public BigDecimal bigDecimalValue() {
        return value;
    }


    @Override
    public PolyNumber increment() {
        return PolyBigDecimal.of( value.add( BigDecimal.ONE ) );
    }


    @Override
    public @NotNull PolyNumber divide( @NotNull PolyNumber other ) {
        return PolyBigDecimal.of( value.divide( other.bigDecimalValue(), MathContext.DECIMAL64 ) );
    }


    @Override
    public @NotNull PolyNumber multiply( @NotNull PolyNumber other ) {
        return PolyBigDecimal.of( value.multiply( other.bigDecimalValue() ) );
    }


    @Override
    public @NotNull PolyNumber plus( @NotNull PolyNumber b1 ) {
        return PolyBigDecimal.of( value.add( b1.bigDecimalValue() ) );
    }


    @Override
    public @NotNull PolyNumber subtract( @NotNull PolyNumber b1 ) {
        return PolyBigDecimal.of( value.subtract( b1.bigDecimalValue() ) );
    }


    @Override
    public PolyBigDecimal negate() {
        return PolyBigDecimal.of( value.negate() );
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        PolyBigDecimal that = (PolyBigDecimal) o;
        return Objects.equals( value.stripTrailingZeros(), that.value.stripTrailingZeros() );
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

        return ObjectUtils.compare( value, o.asNumber().BigDecimalValue() );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyBigDecimal.class, Expressions.constant( value ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyBigDecimal.class );
    }


    public static class PolyBigDecimalSerializerDef extends SimpleSerializerDef<PolyBigDecimal> {

        @Override
        protected BinarySerializer<PolyBigDecimal> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyBigDecimal item ) {
                    out.writeUTF8( item.value.toString() );
                }


                @Override
                public PolyBigDecimal decode( BinaryInput in ) throws CorruptedDataException {
                    return new PolyBigDecimal( new BigDecimal( in.readUTF8() ) );
                }
            };
        }

    }


    public static class PolyBigDecimalSerializer implements JsonDeserializer<PolyBigDecimal>, JsonSerializer<PolyBigDecimal> {


        @Override
        public PolyBigDecimal deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            return PolyBigDecimal.of( json.getAsBigDecimal() );
        }


        @Override
        public JsonElement serialize( PolyBigDecimal src, Type typeOfSrc, JsonSerializationContext context ) {
            return new JsonPrimitive( src.value );
        }

    }

}
