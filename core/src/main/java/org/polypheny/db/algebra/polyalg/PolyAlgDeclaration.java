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

package org.polypheny.db.algebra.polyalg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.StringJoiner;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArg;


public class PolyAlgDeclaration {

    public final String opName;
    public final ImmutableList<String> opAliases;
    public final int numInputs; // -1 if arbitrary amount is allowed

    public final ImmutableList<Parameter> posParams;
    public final ImmutableList<Parameter> kwParams;
    private final ImmutableMap<String, Parameter> paramLookup;


    public PolyAlgDeclaration( String opName, ImmutableList<String> opAliases, int numInputs, ImmutableList<Parameter> params ) {
        this.opName = opName;
        this.opAliases = opAliases;
        this.numInputs = numInputs;

        ImmutableMap.Builder<String, Parameter> bMap = ImmutableMap.builder();
        ImmutableList.Builder<Parameter> bPos = ImmutableList.builder();
        ImmutableList.Builder<Parameter> bKey = ImmutableList.builder();
        for ( Parameter p : params ) {
            bMap.put( p.name, p );
            if ( p.isPositional() ) {
                bPos.add( p );
            } else {
                bKey.add( p );
            }
        }
        this.posParams = bPos.build();
        this.kwParams = bKey.build();
        this.paramLookup = bMap.build();
    }


    public PolyAlgDeclaration( String opName, int numInputs, ImmutableList<Parameter> params ) {
        this( opName, ImmutableList.of(), numInputs, params );
    }


    public PolyAlgDeclaration( String opName, int numInputs ) {
        this( opName, ImmutableList.of(), numInputs, ImmutableList.of() );
    }


    public PolyAlgDeclaration( String opName, int numInputs, Parameter param ) {
        this( opName, ImmutableList.of(), numInputs, ImmutableList.of( param ) );
    }


    public String serializeArguments( Map<Parameter, PolyAlgArg> preparedAttributes ) {
        StringJoiner joiner = new StringJoiner( ", ", "[", "]" );

        for ( Parameter p : posParams ) {
            assert preparedAttributes.containsKey( p );
            joiner.add( preparedAttributes.get( p ).toPolyAlg() );
        }
        for ( Parameter p : kwParams ) {
            if ( preparedAttributes.containsKey( p ) ) {
                String attribute = preparedAttributes.get( p ).toPolyAlg();
                if ( !p.defaultValue.equals( attribute ) ) {
                    joiner.add( p.name + "=" + attribute );
                }
            }
        }
        return joiner.toString();
    }


    public Parameter getPos( int i ) {
        return posParams.get( i );
    }


    public Parameter getParam( String name ) {
        return paramLookup.get( name );
    }


    /**
     * Depending on whether a defaultValue is specified, a Parameter can result in two types of corresponding arguments:
     * <ul>
     *     <li>Positional arguments:
     *     An argument for a positional Parameter always has to be included in the proper position. It does not have a default value</li>
     *     <li>Keyword arguments: Arguments are preceded by the name of the parameter. Keyword arguments can be omitted, in which case the defaultValue is used.</li>
     * </ul>
     *
     * @param name The name of this Parameter
     * @param type The type of this Parameter
     * @param isMultiValued Boolean indicating whether an argument consisting of multiple values can be specified
     * @param defaultValue The default value for this Parameter or null if it has no default value (if it should have a default value of null, use an empty string instead)
     */
    public record Parameter(String name, ParamType type, boolean isMultiValued, String defaultValue) {

        public Parameter( String name ) {
            this( name, ParamType.ANY );
        }


        public Parameter( String name, ParamType type ) {
            this( name, type, false, null );
        }


        public Parameter( String name, ParamType type, boolean isMultiValued ) {
            this( name, type, isMultiValued, null );
        }


        public boolean isPositional() {
            return defaultValue == null;
        }

    }


    public enum ParamType {
        /**
         * The default type. Should only be used if no other type fits better.
         */
        ANY,
        INTEGER,
        STRING,

        /**
         * A boolean flag, either "true" or "false".
         */
        BOOLEAN,

        /**
         * A serialized RexNode
         */
        SIMPLE_REX,
        AGGREGATE,

        /**
         * A REX that evaluates to a boolean.
         */
        BOOLEAN_REX,
        ENTITY,

        /**
         * A specific field (= column in the relational data model).
         */
        FIELD,

        /**
         * A list with no elements.
         */
        EMPTY_LIST,

        /**
         *
         */
        COLLATION,

        /**
         * Correlation ID
         */
        CORR_ID
    }

}
