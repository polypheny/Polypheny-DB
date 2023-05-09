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

package org.polypheny.db.type;

import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.type.entity.PolyValue;

public interface PolySerializable {

    Supplier<SerializerBuilder> builder = () -> SerializerBuilder.create( DefiningClassLoader.create( PolyPluginManager.getMainClassLoader() ) ).withAnnotationCompatibilityMode();


    Map<Class<?>, BinarySerializer<?>> cache = new HashMap<>();


    <T extends PolySerializable> BinarySerializer<T> getSerializer();

    default String serialize() {
        byte[] buffer = new byte[2000];
        int i = getSerializer().encode( buffer, 0, this );
        return new String( buffer, 0, i );
    }

    static <T> String serialize( BinarySerializer<T> serializer, T item ) {
        byte[] buffer = new byte[2000];
        int i = serializer.encode( buffer, 0, item );
        return new String( buffer, 0, i );
    }


    static <T extends PolySerializable> T deserialize( String serialized, Class<T> clazz ) {
        return PolySerializable.deserialize( serialized.getBytes( StandardCharsets.UTF_8 ), clazz );
    }

    static <T extends PolySerializable> T deserialize( String serialized, BinarySerializer<T> serializer ) {
        return serializer.decode( serialized.getBytes( StandardCharsets.UTF_8 ), 0 );
    }

    static <T extends PolySerializable> T deserialize( byte[] serialized, Class<T> clazz ) {
        return clazz.cast( getOrAdd( clazz ).decode( serialized, 0 ) );
    }

    static <T extends PolySerializable> String serialize( T document, Class<T> clazz ) {
        byte[] buffer = new byte[2000];
        int i = getOrAdd( clazz ).encode( buffer, 0, document );
        return new String( buffer, 0, i );
    }


    static <T> BinarySerializer<T> getOrAdd( Class<T> clazz ) {
        return (BinarySerializer<T>) cache.computeIfAbsent( clazz, k -> PolyValue.getAbstractBuilder().build( k ) );
    }


    PolySerializable copy();

}
