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
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import io.activej.serializer.def.SimpleSerializerDef;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;

@EqualsAndHashCode(callSuper = true)
@Value
public class PolyBoolean extends PolyValue {

    public static final PolyBoolean TRUE = PolyBoolean.of( true );
    public static final PolyBoolean FALSE = PolyBoolean.of( false );

    @Serialize
    @JsonProperty
    @SerializeNullable
    public Boolean value;


    /**
     * Creates a PolyBoolean.
     *
     * @param value The value of the PolyBoolean
     */
    public PolyBoolean( @JsonProperty("value") @Deserialize("value") Boolean value ) {
        super( PolyType.BOOLEAN );
        this.value = value;
    }


    public static PolyBoolean of( Boolean value ) {
        return new PolyBoolean( value );
    }


    public static PolyBoolean ofNullable( Boolean value ) {
        return value == null ? null : of( value );
    }


    public static PolyBoolean of( boolean value ) {
        return new PolyBoolean( value );
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
    public @Nullable String toJson() {
        return value == null ? JsonToken.VALUE_NULL.asString() : String.valueOf( value );
    }


    public static PolyBoolean convert( @Nullable PolyValue value ) {
        if ( value == null ) {
            return null;
        }

        if ( value.isBoolean() ) {
            return value.asBoolean();
        } else if ( value.isNumber() ) {
            return value.asBoolean();
        }

        throw new GenericRuntimeException( getConvertError( value, PolyBoolean.class ) );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyBoolean.class, Expressions.constant( value ) );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( isSameType( o ) ) {
            return ObjectUtils.compare( value, o.asBoolean().value );
        }
        return -1;
    }


    @Override
    public @NotNull Long deriveByteSize() {
        return 1L;
    }


    public static class PolyBooleanSerializerDef extends SimpleSerializerDef<PolyBoolean> {

        @Override
        protected BinarySerializer<PolyBoolean> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyBoolean item ) {
                    out.writeBoolean( item == null );
                    if ( item == null ) {
                        return;
                    }
                    out.writeBoolean( item.value );
                }


                @Override
                public PolyBoolean decode( BinaryInput in ) throws CorruptedDataException {
                    boolean isNull = in.readBoolean();
                    if ( isNull ) {
                        return PolyBoolean.of( null );
                    }
                    return in.readBoolean() ? PolyBoolean.TRUE : PolyBoolean.FALSE;
                }
            };
        }

    }


    @Override
    public PolySerializable copy() {
        return null;
    }


    @Override
    public String toString() {
        return value.toString();
    }

}
