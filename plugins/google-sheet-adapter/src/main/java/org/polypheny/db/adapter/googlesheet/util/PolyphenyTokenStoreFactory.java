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

package org.polypheny.db.adapter.googlesheet.util;

import com.google.api.client.util.store.AbstractDataStore;
import com.google.api.client.util.store.DataStore;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;


/**
 * This class serves as a temporary store for the token of the GoogleSheet API.
 * Could be turned into a file store later on.
 * Default store leads to errors as plugin, as it uses IOUtils of Google, which forces default Classloader, which cannot find this plugin.
 */
public class PolyphenyTokenStoreFactory extends FileDataStoreFactory {

    /**
     * @param dataDirectory data directory
     */
    public PolyphenyTokenStoreFactory( File dataDirectory ) throws IOException {
        super( dataDirectory );
    }


    @Override
    protected <V extends Serializable> DataStore<V> createDataStore( String id ) throws IOException {
        return new PolyTokenStore<>( this, id );
    }


    public static class PolyTokenStore<V extends Serializable> extends AbstractDataStore<V> {

        private final HashMap<String, V> map;


        /**
         * @param dataStoreFactory data store factory
         * @param id data store ID
         */
        protected PolyTokenStore( DataStoreFactory dataStoreFactory, String id ) {
            super( dataStoreFactory, id );
            this.map = new HashMap<>();
        }


        @Override
        public Set<String> keySet() {
            return map.keySet();
        }


        @Override
        public Collection<V> values() {
            return map.values();
        }


        @Override
        public V get( String key ) {
            return map.get( key );
        }


        @Override
        public DataStore<V> set( String key, V value ) {
            map.put( key, value );
            return this;
        }


        @Override
        public DataStore<V> clear() {
            map.clear();
            return this;
        }


        @Override
        public DataStore<V> delete( String key ) {
            map.remove( key );
            return this;
        }

    }

}
