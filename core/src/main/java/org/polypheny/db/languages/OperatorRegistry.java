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

package org.polypheny.db.languages;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
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

    // Maps an Operator to a unique name (either op.getName() or op.getOperatorName().name() in case of colliding names within a QueryLanguage.
    // Exception: the operator with the shortest OperatorName can use op.getName()
    private static Map<Operator, String> uniqueNames = null;
    private static Map<Pair<QueryLanguage, String>, Operator> nameLookup = null; // maps a query language and uniqueName to the operator


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


    /**
     * Returns the operator registered with a QueryLanguage of the given DataModel
     * and name equal the unique name registered in uniqueNames.
     * Calling this method the first time creates a lookup table based on the current state of the registry.
     * If new operators are registered after that point, they cannot be found.
     *
     * @param model the data model of the query language the operator was registered with
     * @param name the unique name of the operator (either op.getName() or op.getOperatorName().name() in case of collisions)
     * @return the specified operator or {@code null} if no such operator is registered.
     */
    public static Operator getFromUniqueName( DataModel model, String name ) {
        if ( nameLookup == null ) {
            buildNameLookup();
        }
        QueryLanguage ql = switch ( model ) {
            case RELATIONAL -> null;
            case DOCUMENT -> QueryLanguage.from( "mongo" );
            case GRAPH -> QueryLanguage.from( "cypher" );
        };
        return nameLookup.get( Pair.of( ql, name ) );
    }


    /**
     * Returns the unique name (within its query language) the given operator.
     * By default, the unique name is equal to op.getName(). In case multiple operators have the same name, all but one have to change their unique name to op.getOperatorName().name().
     * The operator that can keep using op.getName() is the one whose OperatorName is the shortest (and first in lexical order in case of same length).
     * Example: OperatorName.PLUS has unique name "+", while OperatorName.DATETIME_PLUS has unique name "DATETIME_PLUS" (since PLUS is shorter than DATETIME_PLUS).
     *
     * Calling this method the first time creates a lookup table based on the current state of the registry.
     * If new operators are registered after that point, they cannot be found.
     *
     * @param op the operator
     * @return the unique name of the specified operator (either equal to op.getName() or op.getOperatorName().name() in case of collisions)
     */
    public static String getUniqueName( Operator op ) {
        if ( uniqueNames == null ) {
            buildNameLookup();
        }
        return uniqueNames.get( op );
    }


    private static void buildNameLookup() {
        Map<Pair<QueryLanguage, String>, Operator> nameLookup = new HashMap<>();
        Map<Operator, String> uniqueNames = new HashMap<>();
        for ( Map.Entry<Pair<QueryLanguage, OperatorName>, Operator> entry : OperatorRegistry.getAllOperators().entrySet() ) {
            QueryLanguage ql = entry.getKey().left;
            String opName = entry.getKey().right.name(); // this is not the same as op.getName()!
            Operator op = entry.getValue();

            if ( op != null ) {
                String uniqueName = op.getName();
                if ( uniqueName.isEmpty() ) {
                    uniqueName = op.getOperatorName().name();
                }

                Operator prevOp = nameLookup.get( Pair.of( ql, uniqueName ) );
                if ( prevOp != null ) {
                    String prevOpName = prevOp.getOperatorName().name();
                    int d = prevOpName.length() - opName.length();
                    if ( d == 0 && prevOpName.compareTo( opName ) == 0 ) {
                        throw new GenericRuntimeException( "Cannot have two operators registered for the same query language and OperatorName." );
                    }

                    if ( d > 0 || (d == 0 && prevOpName.compareTo( opName ) > 0) ) {
                        // found shorter name -> new operator takes priority and can use op.getName() as uniqueName.
                        nameLookup.put( Pair.of( ql, prevOpName ), prevOp );
                        uniqueNames.put( prevOp, prevOpName );
                    } else {
                        uniqueName = opName;
                    }
                }
                nameLookup.put( Pair.of( ql, uniqueName ), op );
                uniqueNames.put( op, uniqueName );
            }
        }

        // prevent overwriting in case lookups are created concurrently
        if ( OperatorRegistry.nameLookup == null ) {
            OperatorRegistry.nameLookup = nameLookup;
        }
        if ( OperatorRegistry.uniqueNames == null ) {
            OperatorRegistry.uniqueNames = uniqueNames;
        }
    }

}
