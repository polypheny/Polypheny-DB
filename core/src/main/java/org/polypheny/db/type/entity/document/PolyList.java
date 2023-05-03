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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Delegate;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

@EqualsAndHashCode(callSuper = true)
@Value
public class PolyList extends PolyValue<List<PolyValue<Object>>> implements List<PolyValue<Object>> {

    @Delegate
    public List<PolyValue<Object>> delegate;


    public PolyList( List<PolyValue<Object>> value ) {
        super( PolyType.ARRAY, value );
        delegate = ImmutableList.copyOf( value );
    }


    public PolyList( PolyValue<Object>... value ) {
        this( Arrays.asList( value ) );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyList.class, value.stream().map( Expressible::asExpression ).collect( Collectors.toList() ) );
    }


    @Override
    public int compareTo( @NotNull PolyValue<List<PolyValue<Object>>> o ) {
        if ( value.size() != o.value.size() ) {
            return value.size() - o.value.size();
        }

        return Pair.zip( value, o.value ).stream().allMatch( p -> p.getKey().isSameType( p.getValue() ) && p.getKey().compareTo( p.getValue() ) == 0 ) ? 0 : -1;

    }

}
