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
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;


public class PolyAlgDeclaration {

    public final String opName;
    public final ImmutableList<String> opAliases;
    public final int numInputs; // -1 if arbitrary amount is allowed
    public final ImmutableList<OperatorTag> opTags;

    public final ImmutableList<Parameter> posParams;
    public final ImmutableList<Parameter> kwParams;
    private final ImmutableMap<String, Parameter> paramLookup;


    @Builder
    public PolyAlgDeclaration( @NonNull String opName, @Singular ImmutableList<String> opAliases, @Singular ImmutableList<OperatorTag> opTags, int numInputs, @Singular ImmutableList<Parameter> params ) {
        this.opName = opName;
        this.opAliases = (opAliases != null) ? opAliases : ImmutableList.of();
        this.numInputs = numInputs;
        this.opTags = (opTags != null) ? opTags : ImmutableList.of();
        params = (params != null) ? params : ImmutableList.of();

        assert PolyAlgDeclaration.hasUniqueNames( params );

        ImmutableMap.Builder<String, Parameter> bMap = ImmutableMap.builder();
        ImmutableList.Builder<Parameter> bPos = ImmutableList.builder();
        ImmutableList.Builder<Parameter> bKey = ImmutableList.builder();
        for ( Parameter p : params ) {
            bMap.put( p.name, p );
            bMap.putAll( p.aliases.stream().collect( Collectors.toMap( a -> a, a -> p ) ) );

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


    public Parameter getPos( int i ) {
        return posParams.get( i );
    }


    public Parameter getParam( String name ) {
        return paramLookup.get( name );
    }


    private static boolean hasUniqueNames( ImmutableList<Parameter> params ) {
        Set<String> names = new HashSet<>();
        for ( Parameter p : params ) {
            if ( names.contains( p.name ) ) {
                return false;
            }
            names.add( p.name );
            for ( String alias : p.aliases ) {
                if ( names.contains( alias ) ) {
                    return false;
                }
                names.add( alias );
            }
        }
        return true;
    }


    /**
     * Checks whether this operator has exactly one positional parameter, which in addition must be multiValued.
     * If this is the case, it is safe for the multiValued parameter to omit the brackets, as it is possible to
     * infer that any positional argument must belong to the multiValued argument.
     *
     * @return whether it is safe to unpack the values of the (only) positional argument
     */
    public boolean canUnpackValues() {
        return posParams.size() == 1 && getPos( 0 ).isMultiValued;
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
     * @param aliases List of alias names that can additionally be used to identify this parameter.
     * @param tags List of ParameterTags associated with this Parameter
     * @param type The type of this Parameter
     * @param isMultiValued Boolean indicating whether an argument consisting of multiple values can be specified
     * @param defaultValue The default value for this Parameter or null if it has no default value (if it should have a default value of null, use an empty string instead)
     */
    @Builder
    public record Parameter(@NonNull String name, @Singular ImmutableList<String> aliases, @Singular ImmutableList<ParameterTag> tags, @NonNull ParamType type, boolean isMultiValued, String defaultValue) {

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

        JOIN_TYPE_ENUM( true ),

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
        CORR_ID;

        private final boolean isEnum;


        ParamType() {
            this.isEnum = false;
        }


        ParamType( boolean isEnum ) {
            this.isEnum = isEnum;
        }


        public boolean isEnum() {
            return isEnum;
        }

    }


    public enum OperatorTag {
        REL,
        DOC,
        LPG,
        LOGICAL,
        PHYSICAL,
        ALLOCATION,

        /**
         * Operator is irrelevant for the average user.
         */
        ADVANCED
    }


    public enum ParameterTag {

        /**
         * Only show parameter in advanced mode.
         */
        ADVANCED
    }

}
