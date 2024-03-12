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

import java.util.Map;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;

public class PolyUserDefinedValue extends PolyValue {

    private final Map<String, PolyType> template;
    private final Map<String, PolyValue> value;


    public PolyUserDefinedValue( Map<String, PolyType> template, Map<String, PolyValue> value ) {
        super( PolyType.USER_DEFINED_TYPE );
        this.template = template;
        this.value = value;
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        throw new NotImplementedException();
    }


    @Override
    public Expression asExpression() {
        return null;
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyUserDefinedValue.class );
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    @Override
    public Object toJava() {
        return value == null ? null : value.toString();
    }

}
