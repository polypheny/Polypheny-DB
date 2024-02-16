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

package org.polypheny.db.adapter.neo4j.types;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.type.PolyType;

@Getter
@EqualsAndHashCode(callSuper = true)
@Value
public class NestedSingleType extends NestedPolyType {

    PolyType type;


    @Override
    public boolean isSingle() {
        return true;
    }


    @Override
    public NestedSingleType asSingle() {
        return this;
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( NestedSingleType.class, Expressions.constant( type, PolyType.class ) );
    }

}
