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

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.catalog.impl.Expressible;
import org.polypheny.db.type.PolyType;

@EqualsAndHashCode(callSuper = true)
@Value
public class NestedListType extends NestedPolyType {

    @Getter
    public PolyType type;

    public List<String> names;


    public List<NestedPolyType> types;


    public NestedListType( PolyType type, List<String> names, List<NestedPolyType> types ) {
        this.type = type;
        this.names = names;
        this.types = types;
    }


    public NestedListType( PolyType type, List<NestedPolyType> types ) {
        this.type = type;
        this.types = types;
        this.names = types.stream().map( t -> (String) null ).toList();
    }


    @Override
    public boolean isList() {
        return true;
    }


    @Override
    public NestedListType asList() {
        return this;
    }


    @Override
    public Expression asExpression() {
        Expression tExpression = EnumUtils.constantArrayList( types.stream().map( Expressible::asExpression ).toList(), NestedPolyType.class );
        Expression nExpression = EnumUtils.constantArrayList( names.stream().map( Expressions::constant ).toList(), String.class );
        return Expressions.new_( NestedListType.class, Expressions.constant( type ), nExpression, tExpression );
    }

}
