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

import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

@EqualsAndHashCode(callSuper = true)
@Value
public class PolyBoolean extends PolyValue<Boolean> {

    public PolyBoolean( Boolean value ) {
        super( PolyType.BOOLEAN, value );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyBoolean.class, Expressions.constant( value ) );
    }


    @Override
    public int compareTo( @NotNull PolyValue<Boolean> o ) {
        return value.compareTo( o.value );
    }

}
