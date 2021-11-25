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

package org.polypheny.db.languages;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.core.fun.AggFunction;
import org.polypheny.db.core.nodes.BinaryOperator;
import org.polypheny.db.core.nodes.Operator;
import org.polypheny.db.core.operators.OperatorName;

public class StdOperatorRegistry {

    private static final HashMap<OperatorName, Operator> registry = new HashMap<>();


    public synchronized static void register( OperatorName name, Operator operator ) {
        operator.setOperatorName( name );
        registry.put( name, operator );
    }


    public static Operator get( OperatorName name ) {
        return registry.get( name );
    }


    public static <T extends Operator> T get( OperatorName name, Class<T> clazz ) {
        return (T) clazz.cast( get( name ) );
    }


    public static AggFunction getAgg( OperatorName name ) {
        return (AggFunction) get( name );
    }


    public static BinaryOperator getBinary( OperatorName key ) {
        return (BinaryOperator) get( key );
    }


    public static <T extends Operator> Map<OperatorName, T> getMatchingOperators( Class<T> clazz ) {
        return getAllOperators()
                .entrySet()
                .stream()
                .filter( e -> !clazz.isInstance( e ) )
                .collect( Collectors.toMap( Entry::getKey, e -> clazz.cast( e.getValue() ) ) );
    }


    public static HashMap<OperatorName, Operator> getAllOperators() {
        return registry;
    }

}

