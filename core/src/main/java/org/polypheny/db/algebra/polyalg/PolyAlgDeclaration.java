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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import org.apache.commons.lang3.function.TriFunction;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.polyalg.arguments.AggArg;
import org.polypheny.db.algebra.polyalg.arguments.AnyArg;
import org.polypheny.db.algebra.polyalg.arguments.BooleanArg;
import org.polypheny.db.algebra.polyalg.arguments.CollationArg;
import org.polypheny.db.algebra.polyalg.arguments.CorrelationArg;
import org.polypheny.db.algebra.polyalg.arguments.EntityArg;
import org.polypheny.db.algebra.polyalg.arguments.EnumArg;
import org.polypheny.db.algebra.polyalg.arguments.FieldArg;
import org.polypheny.db.algebra.polyalg.arguments.IntArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;


public class PolyAlgDeclaration {

    public final String opName;
    public final ImmutableList<String> opAliases;
    public final DataModel model;
    private final int numInputs; // -1 if arbitrary amount is allowed
    public final ImmutableList<OperatorTag> opTags;

    public final ImmutableList<Parameter> posParams;
    public final ImmutableList<Parameter> kwParams;
    private final ImmutableMap<String, Parameter> paramLookup;

    private final TriFunction<PolyAlgArgs, List<AlgNode>, AlgCluster, AlgNode> creator;


    @Builder
    public PolyAlgDeclaration(
            @NonNull String opName,
            @Singular ImmutableList<String> opAliases,
            @NonNull DataModel model,
            TriFunction<PolyAlgArgs, List<AlgNode>, AlgCluster, AlgNode> creator,
            @Singular ImmutableList<OperatorTag> opTags,
            int numInputs,
            @Singular ImmutableList<Parameter> params ) {
        this.opName = opName;
        this.opAliases = (opAliases != null) ? opAliases : ImmutableList.of();
        this.model = model;
        this.creator = creator;
        this.numInputs = numInputs;
        this.opTags = (opTags != null) ? opTags : ImmutableList.of();
        params = (params != null) ? params : ImmutableList.of();

        assert PolyAlgDeclaration.hasUniqueNames( params );

        ImmutableMap.Builder<String, Parameter> bMap = ImmutableMap.builder();
        ImmutableList.Builder<Parameter> bPos = ImmutableList.builder();
        ImmutableList.Builder<Parameter> bKey = ImmutableList.builder();
        for ( Parameter p : params ) {
            assert p.hasValidDefault();

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


    public AlgNode createNode( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        return creator.apply( args, children, cluster );
    }


    /**
     * Retrieves the positional parameter at the specified position.
     *
     * @param i The position of the parameter to retrieve.
     * @return The parameter at the specified position, or {@code null} if the position is out of bounds.
     */
    public Parameter getPos( int i ) {
        if ( i < 0 || i >= posParams.size() ) {
            return null;
        }
        return posParams.get( i );
    }


    /**
     * Retrieves the parameter (positional or keyword) associated with the specified name.
     * It is also possible to specify an alias name.
     *
     * @param name The name of the parameter to retrieve.
     * @return The parameter associated with the specified name, or {@code null} if no parameter is found.
     */
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


    public boolean containsParam( Parameter p ) {
        return posParams.contains( p ) || kwParams.contains( p );
    }


    public boolean supportsNumberOfChildren( int n ) {
        return numInputs == -1 || numInputs == n;
    }


    /**
     * Depending on whether a defaultValue is specified, a Parameter can result in two types of corresponding arguments:
     * <ul>
     *     <li>Positional arguments:
     *     An argument for a positional Parameter always has to be included in the proper position. It does not have a default value</li>
     *     <li>Keyword arguments: Arguments are preceded by the name of the parameter. Keyword arguments can be omitted, in which case the defaultValue is used.</li>
     * </ul>
     *
     * The boolean isMultiValued indicates whether an argument can consist of a list of values of the specified type.
     */
    @Builder
    @Data
    public static class Parameter {

        @NonNull
        private final String name;
        @Singular
        private final ImmutableList<String> aliases;
        @Singular
        private final ImmutableList<ParameterTag> tags;
        @NonNull
        private final ParamType type;
        private final boolean isMultiValued;
        private final PolyAlgArg defaultValue;


        public boolean isPositional() {
            return defaultValue == null;
        }


        public boolean isCompatible( ParamType type ) {
            return this.type == type || (isMultiValued && type == ParamType.EMPTY_LIST);
        }


        /**
         * Checks if the parameter has a valid default value.
         * This can either be no default value at all, or it is a {@link PolyAlgArg} of a compatible type.
         *
         * @return true if the default value is valid
         */
        public boolean hasValidDefault() {
            return isPositional() || isCompatible( defaultValue.getType() );
        }


        public String getDefaultAsPolyAlg( AlgNode context, List<String> inputFieldNames ) {
            if ( isPositional() ) {
                return null;
            }
            return defaultValue.toPolyAlg( context, inputFieldNames );
        }

    }


    @Getter
    public enum ParamType {
        /**
         * The default type. Should only be used if no other type fits better.
         */
        ANY( AnyArg.class ),
        INTEGER( IntArg.class ),

        /**
         * A boolean flag, either "true" or "false".
         */
        BOOLEAN( BooleanArg.class ),

        /**
         * A serialized RexNode
         */
        SIMPLE_REX( RexArg.class ),
        AGGREGATE( AggArg.class ),
        ENTITY( EntityArg.class ),

        JOIN_TYPE_ENUM( EnumArg.class, true ),

        /**
         * A specific field (= column in the relational data model).
         */
        FIELD( FieldArg.class ),

        /**
         * A list with no elements.
         */
        EMPTY_LIST( ListArg.class ),

        /**
         *
         */
        COLLATION( CollationArg.class ),

        /**
         * Correlation ID
         */
        CORR_ID( CorrelationArg.class );

        private final Class<? extends PolyAlgArg> argClass;
        private final boolean isEnum;


        ParamType( Class<? extends PolyAlgArg> argClass ) {
            this( argClass, false );
        }


        ParamType( Class<? extends PolyAlgArg> argClass, boolean isEnum ) {
            this.argClass = argClass;
            this.isEnum = isEnum;
        }

    }


    public enum OperatorTag {
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
