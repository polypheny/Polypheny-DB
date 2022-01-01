/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class StatusService {

    private static AtomicInteger idBuilder = new AtomicInteger();

    private static final Map<Integer, Consumer<String>> subscribers = new HashMap<>();


    public static synchronized int addSubscriber( Consumer<String> printer ) {
        int id = idBuilder.getAndIncrement();
        subscribers.put( id, printer );
        return id;
    }


    public static void print( String status ) {
        subscribers.values().forEach( c -> c.accept( status ) );
    }


    public static void removeSubscriber( int id ) {
        subscribers.remove( id );
    }

}
