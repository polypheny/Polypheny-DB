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

package org.polypheny.db.adapter.MetadataObserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashCache {

    private static final HashCache INSTANCE = new HashCache();

    private final Map<String, String> cache = new ConcurrentHashMap<>();


    private HashCache() {
    }


    public static HashCache getInstance() {
        return INSTANCE;
    }


    public void put( String uniqueName, String hash ) {
        this.cache.put( uniqueName, hash );
    }


    public String getHash( String uniqueName ) {
        return this.cache.get( uniqueName );
    }


}
