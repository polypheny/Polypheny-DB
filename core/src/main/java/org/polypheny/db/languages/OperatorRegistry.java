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

package org.polypheny.db.languages;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.nodes.BinaryOperator;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.util.Pair;

/**
 * OperatorRegistry is used to provide access to all available functions/operators, which Polypheny is able to handle.
 * NOTE: While at the moment a lot of functions are categorized as general are initially SQL functions,
 * the idea is to transition to more general functions registered under <code>null</code>
 *
 * The functions are registered via the {@link OperatorName} enum to have a central place of adding new operators,
 * which should make it more performant and intuitive
 */
public class OperatorRegistry {

    private static final Map<QueryLanguage, Map<OperatorName, Operator>> registry = new HashMap<>();


    static {
        // we register a new map for each language per default
        // the general operators are registered for null
        registry.put( null, new HashMap<>() );
        for ( QueryLanguage language : LanguageManager.getLanguages() ) {
            registry.put( language, new HashMap<>() );
        }
    }


    /**
     * Registers a new {@link Operator} under the given {@link OperatorName},
     * as no {@link QueryLanguage}  is provided the Operator is registered as a general
     * under the key <code>null</code>.
     *
     * @param name is the name of the operator
     * @param operator the operator itself
     */
    public synchronized static void register( OperatorName name, Operator operator ) {
        register( null, name, operator );
    }


    /**
     * Registers a new {@link Operator} under the given {@link OperatorName},
     * for the provided {@link QueryLanguage}.
     *
     * @param language the specific query language for the Operator
     * @param name is the name of the operator
     * @param operator the operator itself
     */
    public synchronized static void register( QueryLanguage language, OperatorName name, Operator operator ) {
        operator.setOperatorName( name );
        if ( !registry.containsKey( language ) ) {
            registry.put( language, new HashMap<>() );
        }

        registry.get( language ).put( name, operator );
    }


    /**
     * Retrieves the general function for the given {@link OperatorName}.
     *
     * @param name is the used {@link OperatorName} to retrieve the general function associated with it
     * @return the requested {@link Operator}
     */
    public static Operator get( OperatorName name ) {
        return get( null, name );
    }


    /**
     * Retrieves the language-specific function for the given {@link OperatorName} and {@link QueryLanguage}.
     *
     * @param language for which the {@link Operator} is retrieved
     * @param name of the requested {@link Operator}
     * @return the requested {@link Operator}
     */
    public static Operator get( QueryLanguage language, OperatorName name ) {
        return registry.get( language ).get( name );
    }


    /**
     * Retrieves the general function for the given {@link OperatorName} and casts it to the specified {@link Class}.
     *
     * @param name of the requested {@link Operator}
     * @param clazz the {@link Class} into which the operator is cast
     * @return the requested {@link Operator}
     */
    public static <T extends Operator> T get( OperatorName name, Class<T> clazz ) {
        return get( null, name, clazz );
    }


    /**
     * Retrieves the language-specific function for the given {@link OperatorName} and {@link QueryLanguage}.
     * and casts it to the specified {@link Class}.
     *
     * @param language for which the {@link Operator} is retrieved
     * @param name of the requested {@link Operator}
     * @param clazz the {@link Class} into which the operator is cast
     * @return the requested {@link Operator}
     */
    public static <T extends Operator> T get( QueryLanguage language, OperatorName name, Class<T> clazz ) {
        return clazz.cast( get( language, name ) );
    }


    /**
     * {@link AggFunction} utility function of {@link #get(OperatorName, Class)}.
     *
     * @return the requested {@link Operator} as {@link AggFunction}
     */
    public static AggFunction getAgg( OperatorName name ) {
        return (AggFunction) get( name );
    }


    /**
     * {@link BinaryOperator} utility function of {@link #get(OperatorName, Class)}.
     *
     * @return the requested {@link Operator} as {@link BinaryOperator}
     */
    public static BinaryOperator getBinary( OperatorName key ) {
        return (BinaryOperator) get( key );
    }


    /**
     * Retrieves all {@link Operator}s for a given {@link QueryLanguage},
     * <code>null</code> is used for general functions.
     *
     * @param language for which all {@link Operator}s are retrieved
     * @return a map of all {@link Operator} with their respective {@link OperatorName}
     */
    public static Map<OperatorName, ? extends Operator> getMatchingOperators( QueryLanguage language ) {
        return registry.get( language );
    }


    /**
     * Retrieves all available operators sorted in a {@link Map}.
     */
    public static Map<Pair<QueryLanguage, OperatorName>, Operator> getAllOperators() {
        return registry
                .entrySet()
                .stream()
                .flatMap( e -> e.getValue().entrySet().stream().map( c -> Pair.of( e.getKey(), c ) ) )
                .collect( Collectors.toMap( t -> Pair.of( t.left, t.right.getKey() ), t -> t.right.getValue() ) );
    }


    public static void remove( QueryLanguage language ) {
        registry.remove( language );
    }

}

