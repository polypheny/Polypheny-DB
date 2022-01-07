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
import java.util.function.BiConsumer;

public class StatusService {

    private static final AtomicInteger idBuilder = new AtomicInteger();

    private static final Map<Integer, BiConsumer<String, Object>> infoSubs = new HashMap<>();
    private static final Map<Integer, BiConsumer<String, Object>> errorSubs = new HashMap<>();


    public static synchronized int addInfoSubscriber( BiConsumer<String, Object> printer ) {
        return StatusService.addSubscriber( printer, StatusType.INFO );
    }


    public static synchronized int addSubscriber( BiConsumer<String, Object> printer, StatusType type ) {
        int id = idBuilder.getAndIncrement();
        if ( type == StatusType.INFO ) {
            infoSubs.put( id, printer );
        } else {
            errorSubs.put( id, printer );
        }

        return id;
    }


    public static void printInfo( String status, Object arg ) {
        fireOnSubs( infoSubs, status, arg );
    }


    public static void printInfo( String status ) {
        printInfo( status, null );
    }


    public static void printError( String status, Object arg ) {
        fireOnSubs( errorSubs, status, arg );
    }


    private static void fireOnSubs( Map<Integer, BiConsumer<String, Object>> subs, String status, Object arg ) {
        subs.values().forEach( c -> c.accept( status, arg ) );
    }


    public static void removeSubscriber( int id ) {
        infoSubs.remove( id );
        errorSubs.remove( id );
    }


    public enum StatusType {
        INFO,
        ERROR
    }

}
