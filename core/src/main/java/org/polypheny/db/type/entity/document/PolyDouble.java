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

package org.polypheny.db.type.entity.document;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SimpleSerializerDef;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

@EqualsAndHashCode(callSuper = true)
@Value
public class PolyDouble extends PolyValue {

    @Serialize
    public Double value;


    public PolyDouble( @Deserialize("value") Double value ) {
        super( PolyType.DOUBLE, false );
        this.value = value;
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        return this.value.compareTo( o.asDouble().value );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyDouble.class, Expressions.constant( value ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyDouble.class );
    }


    public static class PolyDoubleSerializerDef extends SimpleSerializerDef<PolyDouble> {

        @Override
        protected BinarySerializer<PolyDouble> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyDouble item ) {
                    out.writeDouble( item.value );
                }


                @Override
                public PolyDouble decode( BinaryInput in ) throws CorruptedDataException {
                    return new PolyDouble( in.readDouble() );
                }
            };
        }

    }

}
