/*
 * Copyright 2019-2025 The Polypheny Project
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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

@Slf4j
@EqualsAndHashCode(callSuper = false)
@Value
public class PolyListImpl<E extends PolyValue> extends PolyList<E> {

    @Serialize
    @JsonIgnore
    @Delegate
    public List<E> value;

    public static final PolyList<?> EMPTY_LIST = new PolyListImpl<>();

    /**
     * Creates a PolyListImpl, which is the default generic implementation of a PolyList,
     * where the List, as well as all the elements are comparable.
     *
     * @param value The value of the PolyList
     */
    @JsonCreator
    public PolyListImpl(@JsonProperty("value") @Deserialize("value") List<E> value ) {
        super( PolyType.ARRAY );
        this.value = new ArrayList<>( value );
    }


    @SafeVarargs
    public PolyListImpl( E... value ) {
        this( Arrays.asList( value ) );
    }


    @Override
    public @NotNull String toTypedJson() {
        try {
            return JSON_WRAPPER.writerFor( new TypeReference<PolyListImpl<PolyValue>>() {
            } ).writeValueAsString( this );
        } catch ( JsonProcessingException e ) {
            log.warn( "Error on serializing typed JSON." );
            return PolyNull.NULL.toTypedJson();
        }
    }


    @Override
    public String toString() {
        return value == null ? "null" : value.toString();
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( PolyList.class, "ofExpression", value.stream().map(e -> e == null ? Expressions.constant( null ) : e.asExpression() ).toList() );
    }


    @Override
    public @Nullable String toJson() {
        return value == null ? JsonToken.VALUE_NULL.asString() : "[" + value.stream().map( e -> {
            if ( e == null ) {
                return JsonToken.VALUE_NULL.asString();
            } else if ( e.isString() ) {
                return e.asString().toQuotedJson();
            } else if ( e.isDate() || e.isTime() || e.isTimestamp() ) {
                return "\"" + e.toJson() + "\"";
            } else {
                return e.toJson();
            }
        } ).collect( Collectors.joining( "," ) ) + "]";
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        PolyList<?> otherList = o.asList();
        if ( value.size() != otherList.size() ) {
            return Long.compare( value.size(), otherList.size() );
        }

        for ( Pair<E, ?> pair : Pair.zip( value, otherList ) ) {
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


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    @Override
    public Object toJava() {
        return value == null ? null : value.stream().map( PolyValue::toJava ).collect( Collectors.toList() );
    }

}
