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

package org.polypheny.db.type.entity.relational;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SimpleSerializerDef;
import io.activej.serializer.annotations.Serialize;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Delegate;
import lombok.experimental.NonFinal;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BuiltInMethod;

@EqualsAndHashCode(callSuper = true)
@Value(staticConstructor = "of")
@NonFinal
public class PolyMap<K extends PolyValue, V extends PolyValue> extends PolyValue implements Map<K, V> {

    @Delegate
    @Serialize
    public Map<K, V> map;


    public PolyMap( Map<K, V> map ) {
        this( map, PolyType.MAP );
    }


    public PolyMap( Map<K, V> map, PolyType type ) {
        super( type );
        this.map = new HashMap<>( map );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }
        Map<PolyValue, PolyValue> other = o.asMap();

        if ( map.size() != other.size() ) {

            return map.size() > other.size() ? 1 : -1;
        }

        for ( Entry<PolyValue, PolyValue> entry : other.entrySet() ) {
            if ( other.containsKey( entry.getKey() ) ) {
                int i = entry.getValue().compareTo( other.get( entry.getKey() ) );
                if ( i != 0 ) {
                    return i;
                }
            } else {
                return -1;
            }

        }

        return 0;
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyMap.class, Expressions.call(
                BuiltInMethod.MAP_OF_ENTRIES.method,
                EnumUtils.expressionList(
                        entrySet()
                                .stream()
                                .map( p -> Expressions.call(
                                        BuiltInMethod.PAIR_OF.method,
                                        p.getKey().asExpression(),
                                        p.getValue().asExpression() ) ).collect( Collectors.toList() ) ) ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyMap.class );
    }


    public static class PolyDocumentSerializerDef extends SimpleSerializerDef<PolyMap<?, ?>> {

        @Override
        protected BinarySerializer<PolyMap<? extends PolyValue, ? extends PolyValue>> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyMap<? extends PolyValue, ? extends PolyValue> item ) {
                    out.writeLong( item.size() );
                    for ( Entry<? extends PolyValue, ? extends PolyValue> entry : item.entrySet() ) {
                        out.writeUTF8( PolySerializable.serialize( serializer, entry.getKey() ) );
                        out.writeUTF8( PolySerializable.serialize( serializer, entry.getValue() ) );
                    }
                }


                @Override
                public PolyMap<? extends PolyValue, ? extends PolyValue> decode( BinaryInput in ) throws CorruptedDataException {
                    Map<PolyValue, PolyValue> map = new HashMap<>();
                    long size = in.readLong();
                    for ( long i = 0; i < size; i++ ) {
                        map.put(
                                PolySerializable.deserialize( in.readUTF8(), serializer ),
                                PolySerializable.deserialize( in.readUTF8(), serializer ) );
                    }
                    return PolyMap.of( map );
                }
            };
        }

    }

}
