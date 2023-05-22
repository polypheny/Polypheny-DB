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

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SimpleSerializerDef;
import java.math.BigDecimal;
import java.math.RoundingMode;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.category.PolyNumber;

@EqualsAndHashCode(callSuper = true)
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


    public static PolyBigDecimal of( long value ) {
        return new PolyBigDecimal( BigDecimal.valueOf( value ) );
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
    public PolyNumber divide( PolyNumber other ) {
        return PolyBigDecimal.of( value.divide( other.bigDecimalValue(), RoundingMode.UNNECESSARY ) );
    }


    @Override
    public PolyNumber multiply( PolyNumber other ) {
        return PolyBigDecimal.of( value.multiply( other.bigDecimalValue() ) );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        return value.compareTo( o.asBigDecimal().value );
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

}
