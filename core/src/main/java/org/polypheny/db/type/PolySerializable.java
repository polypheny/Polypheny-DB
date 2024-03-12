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

package org.polypheny.db.type;

import com.drew.lang.Charsets;
import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerFactory;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.util.Util;

public interface PolySerializable {


    DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create( PolyPluginManager.getMainClassLoader() == null ? PolySerializable.class.getClassLoader() : PolyPluginManager.getMainClassLoader() );

    // activej serializer requires this charset
    Charset SERIALIZAION_CHARSET = Charsets.ISO_8859_1;

    static <C extends Class<T>, T> BinarySerializer<T> buildSerializer( C clazz ) {
        return SerializerFactory.defaultInstance()
                .create( CLASS_LOADER, clazz );
    }

    Map<Class<?>, BinarySerializer<?>> cache = new HashMap<>();
    int BUFFER_SIZE = RuntimeConfig.SERIALIZATION_BUFFER_SIZE.getInteger();
    Charset CHARSET = Util.getDefaultCharset();


    <T extends PolySerializable> BinarySerializer<T> getSerializer();

    default String serialize() {
        byte[] buffer = new byte[BUFFER_SIZE];
        int i = getSerializer().encode( buffer, 0, this );
        return new String( buffer, 0, i, SERIALIZAION_CHARSET );
    }

    static <T> String serialize( BinarySerializer<T> serializer, T item ) {
        byte[] buffer = new byte[BUFFER_SIZE];
        int i = serializer.encode( buffer, 0, item );
        return new String( buffer, 0, i, SERIALIZAION_CHARSET );
    }


    static <T extends PolySerializable> T deserialize( String serialized, Class<T> clazz ) {
        try {
            return PolySerializable.deserialize( serialized.getBytes( SERIALIZAION_CHARSET ), clazz );
        } catch ( Throwable throwable ) {
            throw new GenericRuntimeException( throwable.getMessage() );
        }
    }

    static <T extends PolySerializable> T deserialize( String serialized, BinarySerializer<T> serializer ) {
        try {
            return serializer.decode( serialized.getBytes( SERIALIZAION_CHARSET ), 0 );
        } catch ( Throwable throwable ) {
            throw new GenericRuntimeException( throwable.getMessage() );
        }
    }

    static <T extends PolySerializable> T deserialize( byte[] serialized, Class<T> clazz ) {
        return clazz.cast( buildSerializer( clazz ).decode( serialized, 0 ) );
    }


    PolySerializable copy();

}
