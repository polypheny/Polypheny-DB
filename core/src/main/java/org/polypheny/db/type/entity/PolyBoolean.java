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
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import java.lang.reflect.Type;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;

@EqualsAndHashCode(callSuper = true)
@Value
public class PolyBoolean extends PolyValue {

    public static final PolyBoolean TRUE = PolyBoolean.of( true );
    public static final PolyBoolean FALSE = PolyBoolean.of( false );

    @Serialize
    @SerializeNullable
    public Boolean value;


    public PolyBoolean( @Deserialize("value") Boolean value ) {
        super( PolyType.BOOLEAN );
        this.value = value;
    }


    public static PolyBoolean of( Boolean value ) {
        return new PolyBoolean( value );
    }


    public static PolyBoolean of( boolean value ) {
        return new PolyBoolean( value );
    }


    public static PolyBoolean convert( Object value ) {
        if ( value instanceof PolyValue ) {
            if ( ((PolyValue) value).isBoolean() ) {
                return ((PolyValue) value).asBoolean();
            }
        }
        throw new NotImplementedException( "convert value to boolean" );
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


    public static class PolyBooleanSerializerDef extends SimpleSerializerDef<PolyBoolean> {

        @Override
        protected BinarySerializer<PolyBoolean> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyBoolean item ) {
                    out.writeBoolean( item.value );
                }


                @Override
                public PolyBoolean decode( BinaryInput in ) throws CorruptedDataException {
                    return in.readBoolean() ? PolyBoolean.TRUE : PolyBoolean.FALSE;
                }
            };
        }

    }


    public static class PolyBooleanSerializer implements JsonSerializer<PolyBoolean>, JsonDeserializer<PolyBoolean> {

        @Override
        public PolyBoolean deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            return PolyBoolean.of( json.getAsBoolean() );
        }


        @Override
        public JsonElement serialize( PolyBoolean src, Type typeOfSrc, JsonSerializationContext context ) {
            return new JsonPrimitive( src.value );
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
