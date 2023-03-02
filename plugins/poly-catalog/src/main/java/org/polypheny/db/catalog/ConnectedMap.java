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

package org.polypheny.db.catalog;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.jetbrains.annotations.NotNull;

public class ConnectedMap<K, V> extends ConcurrentHashMap<K, V> {

    ConcurrentLinkedQueue<Consumer<ConnectedMap<K, V>>> onChange = new ConcurrentLinkedQueue<>();


    public void change() {
        onChange.forEach( p -> p.accept( this ) );
    }


    public ConnectedMap( Map<K, V> allocations ) {
        super( allocations );
        change();
    }


    @Override
    public V put( @NotNull K key, @NotNull V value ) {
        V v = super.put( key, value );
        change();
        return v;
    }


    @Override
    public void putAll( Map<? extends K, ? extends V> m ) {
        super.putAll( m );
        change();
    }


    @Override
    public void clear() {
        super.clear();
        change();
    }


    @Override
    public V putIfAbsent( K key, V value ) {
        V v = super.putIfAbsent( key, value );
        change();
        return v;
    }


    @Override
    public boolean remove( Object key, Object value ) {
        boolean b = super.remove( key, value );
        change();
        return b;
    }


    @Override
    public boolean replace( K key, V oldValue, V newValue ) {
        boolean b = super.replace( key, oldValue, newValue );
        change();
        return b;
    }


    @Override
    public V replace( K key, V value ) {
        V v = super.replace( key, value );
        change();
        return v;
    }


    @Override
    public void replaceAll( BiFunction<? super K, ? super V, ? extends V> function ) {
        super.replaceAll( function );
        change();
    }


    public void addConnection( Consumer<ConnectedMap<K, V>> onChange ) {
        this.onChange.add( onChange );
    }


    public <K2, V2> void addRowConnection( Map<K2, V2> target, BiFunction<K, V, K2> keyTransformer, BiFunction<K, V, V2> valueTransformer ) {
        addConnection( o -> {
            target.clear();
            this.forEach( ( key, value ) -> target.put( keyTransformer.apply( key, value ), valueTransformer.apply( key, value ) ) );
        } );
    }

}
