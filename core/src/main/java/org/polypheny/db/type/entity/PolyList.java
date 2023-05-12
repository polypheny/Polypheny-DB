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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Delegate;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

@EqualsAndHashCode(callSuper = true)
@Value(staticConstructor = "copyOf")
public class PolyList<E extends PolyValue> extends PolyValue implements List<E> {


    @Delegate
    @Serialize
    public List<E> value;


    public PolyList( @Deserialize("value") List<E> value ) {
        super( PolyType.ARRAY, false );
        this.value = ImmutableList.copyOf( value );
    }


    @SafeVarargs
    public PolyList( E... value ) {
        this( Arrays.asList( value ) );
    }


    public static <E extends PolyValue> PolyList<E> of( Collection<E> value ) {
        return new PolyList<>( List.copyOf( value ) );
    }


    @SafeVarargs
    public static <E extends PolyValue> PolyList<E> of( E... values ) {
        return new PolyList<>( values );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyList.class, value.stream().map( Expressible::asExpression ).collect( Collectors.toList() ) );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        if ( value.size() != o.asList().value.size() ) {
            return value.size() - o.asList().value.size();
        }

        for ( Pair<E, ?> pair : Pair.zip( value, o.asList().value ) ) {
            if ( pair.left.compareTo( (PolyValue) pair.right ) != 0 ) {
                return pair.left.compareTo( (PolyValue) pair.right );
            }
        }

        return 0;
    }


    @Override
    public PolySerializable copy() {
        return null;
    }

}
