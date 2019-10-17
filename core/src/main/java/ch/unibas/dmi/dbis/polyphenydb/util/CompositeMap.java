/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.util;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;


/**
 * Unmodifiable view onto multiple backing maps. An element occurs in the map if it occurs in any of the backing maps; the value is the value that occurs
 * in the first map that contains the key.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class CompositeMap<K, V> implements Map<K, V> {

    private final ImmutableList<Map<K, V>> maps;


    public CompositeMap( ImmutableList<Map<K, V>> maps ) {
        this.maps = maps;
    }


    /**
     * Creates a CompositeMap.
     */
    // Would like to use '@SafeVarargs' but JDK 1.6 doesn't support it.
    @SafeVarargs
    public static <K, V> CompositeMap<K, V> of( Map<K, V> map0, Map<K, V>... maps ) {
        return new CompositeMap<>( list( map0, maps ) );
    }


    private static <E> ImmutableList<E> list( E e, E[] es ) {
        ImmutableList.Builder<E> builder = ImmutableList.builder();
        builder.add( e );
        for ( E map : es ) {
            builder.add( map );
        }
        return builder.build();
    }


    @Override
    public int size() {
        return keySet().size();
    }


    @Override
    public boolean isEmpty() {
        // Empty iff all maps are empty.
        for ( Map<K, V> map : maps ) {
            if ( !map.isEmpty() ) {
                return false;
            }
        }
        return true;
    }


    @Override
    public boolean containsKey( Object key ) {
        for ( Map<K, V> map : maps ) {
            if ( map.containsKey( key ) ) {
                return true;
            }
        }
        return false;
    }


    @Override
    public boolean containsValue( Object value ) {
        for ( Map<K, V> map : maps ) {
            if ( map.containsValue( value ) ) {
                return true;
            }
        }
        return false;
    }


    @Override
    public V get( Object key ) {
        for ( Map<K, V> map : maps ) {
            //noinspection SuspiciousMethodCalls
            if ( map.containsKey( key ) ) {
                return map.get( key );
            }
        }
        return null;
    }


    @Override
    public V put( K key, V value ) {
        // we are an unmodifiable view on the maps
        throw new UnsupportedOperationException();
    }


    @Override
    public V remove( Object key ) {
        // we are an unmodifiable view on the maps
        throw new UnsupportedOperationException();
    }


    @Override
    public void putAll( Map<? extends K, ? extends V> m ) {
        // we are an unmodifiable view on the maps
        throw new UnsupportedOperationException();
    }


    @Override
    public void clear() {
        // we are an unmodifiable view on the maps
        throw new UnsupportedOperationException();
    }


    @Override
    public Set<K> keySet() {
        final Set<K> keys = new LinkedHashSet<>();
        for ( Map<K, V> map : maps ) {
            keys.addAll( map.keySet() );
        }
        return keys;
    }


    private Map<K, V> combinedMap() {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        final Set<K> keys = new LinkedHashSet<>();
        for ( Map<K, V> map : maps ) {
            for ( Entry<K, V> entry : map.entrySet() ) {
                if ( keys.add( entry.getKey() ) ) {
                    builder.put( entry );
                }
            }
        }
        return builder.build();
    }


    @Override
    public Collection<V> values() {
        return combinedMap().values();
    }


    @Override
    public Set<Entry<K, V>> entrySet() {
        return combinedMap().entrySet();
    }
}

