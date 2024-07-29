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
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;

@EqualsAndHashCode(callSuper = true)
@Value
public class PolySymbol extends PolyValue {

    @JsonProperty
    public Enum<?> value;
    @JsonProperty
    public Object object;


    @JsonCreator
    public PolySymbol( @JsonProperty("value") Enum<?> value, @JsonProperty("object") Object o ) {
        super( PolyType.SYMBOL );
        this.value = value;
        this.object = o;
    }


    public static PolySymbol of( Enum<?> value ) {
        return new PolySymbol( value, null );
    }


    public static PolySymbol of( Object value ) {
        return new PolySymbol( null, value );
    }


    public <T extends Enum<T>> T asEnum( Class<T> clazz ) {
        return clazz.cast( value );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        return ((Enum) value).compareTo( o.asSymbol().value );
    }


    @Override
    public Expression asExpression() {
        return Expressions.constant( value );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolySymbol.class );
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    @Override
    public Object toJava() {
        return value;
    }


    @Override
    public String toString() {
        return value.name();
    }

}
