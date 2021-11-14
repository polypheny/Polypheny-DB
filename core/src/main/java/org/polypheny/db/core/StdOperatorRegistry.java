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

package org.polypheny.db.core;

import java.util.HashMap;

public class StdOperatorRegistry {

    private static final HashMap<String, Operator> registry = new HashMap<>();


    public synchronized static boolean register( String key, Operator operator ) {
        boolean replaced = registry.containsKey( key );
        registry.put( key, operator );
        return replaced;
    }


    public static Operator get( String key ) {
        return registry.get( key );
    }


    public static <E extends Operator, T> E get( String key, Class<T> clazz ) {
        return (E) clazz.cast( get( key ) );
    }


    public static <T extends Operator & AggFunction> T getAgg( String key ) {
        return (T) get( key );
    }


    public static <T extends Operator & BinaryOperator> T getBinary( String key ) {
        return (T) get( key );
    }

}
