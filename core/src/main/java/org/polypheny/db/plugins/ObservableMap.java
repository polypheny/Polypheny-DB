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

package org.polypheny.db.plugins;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.HashMap;
import java.util.Map;

public class ObservableMap<K, V> extends HashMap<K, V> {

    protected final PropertyChangeSupport listeners = new PropertyChangeSupport( this );


    public void addListener( PropertyChangeListener listener ) {
        this.listeners.addPropertyChangeListener( listener );
    }


    public void removeListener( PropertyChangeListener listener ) {
        this.listeners.removePropertyChangeListener( listener );
    }


    @Override
    public V put( K key, V value ) {
        V val = super.put( key, value );
        listeners.firePropertyChange( new PropertyChangeEvent( this, "put", null, key ) );
        return val;
    }


    @Override
    public void clear() {
        super.clear();
        listeners.firePropertyChange( new PropertyChangeEvent( this, "clear", null, null ) );
    }


    @Override
    public void putAll( Map<? extends K, ? extends V> m ) {
        super.putAll( m );
        listeners.firePropertyChange( new PropertyChangeEvent( this, "putAll", null, m ) );
    }

}
