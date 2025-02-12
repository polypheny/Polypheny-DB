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

package org.polypheny.db.algebra.polyalg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
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
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.SemiJoinType;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.polyalg.arguments.AggArg;
import org.polypheny.db.algebra.polyalg.arguments.AnyArg;
import org.polypheny.db.algebra.polyalg.arguments.BooleanArg;
import org.polypheny.db.algebra.polyalg.arguments.CollationArg;
import org.polypheny.db.algebra.polyalg.arguments.CorrelationArg;
import org.polypheny.db.algebra.polyalg.arguments.DoubleArg;
import org.polypheny.db.algebra.polyalg.arguments.EntityArg;
import org.polypheny.db.algebra.polyalg.arguments.EnumArg;
import org.polypheny.db.algebra.polyalg.arguments.FieldArg;
import org.polypheny.db.algebra.polyalg.arguments.IntArg;
import org.polypheny.db.algebra.polyalg.arguments.LaxAggArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.algebra.polyalg.arguments.WindowGroupArg;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.type.PolyType;


public class PolyAlgDeclaration {

    public final String opName;
    public final ImmutableSet<String> opAliases;
    public final DataModel model; // null: common (can be used with nodes of any datamodel)
    public final Convention convention; // null: no convention (i.e. not a physical operator)
    private final int numInputs; // -1 if arbitrary amount is allowed
    public final ImmutableSet<OperatorTag> opTags;
    private final boolean isNotFullyImplemented; //

    public final ImmutableList<Parameter> posParams;
    public final ImmutableList<Parameter> kwParams;
    private final ImmutableMap<String, Parameter> paramLookup;

    private final TriFunction<PolyAlgArgs, List<AlgNode>, AlgCluster, AlgNode> creator;


    @Builder
    public PolyAlgDeclaration(
            @NonNull String opName,
            @Singular ImmutableSet<String> opAliases,
            DataModel model,
            Convention convention,
            TriFunction<PolyAlgArgs, List<AlgNode>, AlgCluster, AlgNode> creator,
            @Singular ImmutableSet<OperatorTag> opTags,
            int numInputs,
            boolean isNotFullyImplemented,
            @Singular ImmutableList<Parameter> params ) {
        this.opName = opName;
        this.opAliases = (opAliases != null) ? opAliases : ImmutableSet.of();
        this.model = model;
        this.convention = convention;
        this.creator = creator;
        this.numInputs = numInputs;
        this.isNotFullyImplemented = isNotFullyImplemented;
        this.opTags = (opTags != null) ? opTags : ImmutableSet.of();
        params = (params != null) ? params : ImmutableList.of();

        assert PolyAlgDeclaration.hasUniqueNames( params );
        assert PolyAlgDeclaration.hasRequiredTags( params );
        assert convention == null || this.opTags.contains( OperatorTag.PHYSICAL );

        ImmutableMap.Builder<String, Parameter> bMap = ImmutableMap.builder();
        ImmutableList.Builder<Parameter> bPos = ImmutableList.builder();
        ImmutableList.Builder<Parameter> bKey = ImmutableList.builder();
        for ( Parameter p : params ) {
            assert p.hasCompatibleSimpleType();
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


    private static boolean hasRequiredTags( ImmutableList<Parameter> params ) {
        for ( Parameter p : params ) {
            if ( p.requiresAlias && !p.tags.contains( ParamTag.ALIAS ) ) {
                return false;
            }
            if ( p.tags.contains( ParamTag.HIDE_TRIVIAL ) && !p.tags.contains( ParamTag.ALIAS ) ) {
                return false;
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
        return posParams.size() == 1 && getPos( 0 ).isMultiValued();
    }


    public boolean containsParam( Parameter p ) {
        return posParams.contains( p ) || kwParams.contains( p );
    }


    public boolean hasParams() {
        return !paramLookup.isEmpty();
    }


    public boolean supportsNumberOfChildren( int n ) {
        return numInputs == -1 || numInputs == n;
    }


    public boolean mightRequireAuxiliaryProject() {
        return (numInputs == -1 || numInputs > 1) &&
                !opTags.contains( OperatorTag.PHYSICAL ) &&
                (model == DataModel.RELATIONAL || model == null);
    }


    public ObjectNode serialize( ObjectMapper mapper ) {
        ObjectNode node = mapper.createObjectNode();
        node.put( "name", opName );

        ArrayNode aliases = mapper.createArrayNode();
        for ( String alias : opAliases ) {
            aliases.add( alias );
        }
        node.set( "aliases", aliases );

        if ( model == null ) {
            node.put( "model", "COMMON" );
        } else {
            node.put( "model", model.name() );
        }

        if ( convention != null ) {
            node.put( "convention", convention.getName() );
        }
        node.put( "numInputs", numInputs );

        ArrayNode tags = mapper.createArrayNode();
        for ( OperatorTag tag : opTags ) {
            tags.add( tag.name() );
        }
        node.set( "tags", tags );

        ArrayNode posArr = mapper.createArrayNode();
        for ( Parameter p : posParams ) {
            posArr.add( p.serialize( mapper ) );
        }
        if ( !posArr.isEmpty() && canUnpackValues() ) {
            ((ObjectNode) posArr.get( 0 )).put( "canUnpackValues", true );
        }
        node.set( "posParams", posArr );

        ArrayNode kwArr = mapper.createArrayNode();
        for ( Parameter p : kwParams ) {
            kwArr.add( p.serialize( mapper ) );
        }
        node.set( "kwParams", kwArr );

        if ( isNotFullyImplemented ) {
            node.put( "notRegistered", true ); // disables editing for this node in the UI
        }

        return node;
    }


    /**
     * Depending on whether a defaultValue is specified, a Parameter can result in two types of corresponding arguments:
     * <ul>
     *     <li>Positional arguments:
     *     An argument for a positional Parameter always has to be included in the proper position. It does not have a default value</li>
     *     <li>Keyword arguments: Arguments are preceded by the name of the parameter. Keyword arguments can be omitted, in which case the defaultValue is used.</li>
     * </ul>
     *
     * The int multiValued indicates that the argument is wrapped within multiValued number of nested lists. (0: arg, 1: [arg0, arg1, ...], 2: [[arg00, arg01, ...], ...])
     * The boolean requiresAlias can be useful if a key-value pair is expected. This alias corresponds to the "AS" clause
     * and should not be confused with parameter name aliases.
     */
    @Builder
    @Data
    public static class Parameter {

        @NonNull
        private final String name;
        @Singular
        private final ImmutableSet<String> aliases;
        @Singular
        private final ImmutableSet<ParamTag> tags;
        private final SimpleType simpleType;
        @NonNull
        private final ParamType type;
        private final int multiValued; // 0: not multivalued (default). otherwise: nesting depth of lists
        public final boolean requiresAlias;
        private final PolyAlgArg defaultValue; // for multiValued parameters the default value should be a ListArg representing the outermost element


        public boolean isPositional() {
            return defaultValue == null;
        }


        public boolean isCompatible( ParamType type ) {
            return this.type == type || (isMultiValued() && type == ParamType.LIST);
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


        public boolean hasCompatibleSimpleType() {
            return this.simpleType == null ||
                    (this.simpleType.isCompatible( type ) &&
                            !(this.isPositional() && this.simpleType == SimpleType.HIDDEN));
        }


        public boolean isMultiValued() {
            return multiValued > 0;
        }


        public String getDefaultAsPolyAlg( AlgNode context, List<String> inputFieldNames ) {
            if ( isPositional() ) {
                return null;
            }
            return defaultValue.toPolyAlg( context, inputFieldNames );
        }


        public ObjectNode serialize( ObjectMapper mapper ) {
            ObjectNode node = mapper.createObjectNode();
            node.put( "name", name );

            ArrayNode aliasesArr = mapper.createArrayNode();
            for ( String alias : aliases ) {
                aliasesArr.add( alias );
            }
            node.set( "aliases", aliasesArr );

            ArrayNode tagsArr = mapper.createArrayNode();
            for ( ParamTag tag : tags ) {
                tagsArr.add( tag.name() );
            }
            node.set( "tags", tagsArr );

            node.put( "type", type.name() );
            if ( simpleType != null ) {
                node.put( "simpleType", simpleType.name() );
            }
            node.put( "multiValued", multiValued );
            node.put( "requiresAlias", requiresAlias );
            if ( !isPositional() ) {
                node.set( "defaultValue", defaultValue.serializeWrapped( null, List.of(), mapper ) );
                node.put( "defaultPolyAlg", defaultValue.toPolyAlg( null, List.of() ) );
            }
            node.put( "isEnum", type.isEnum() );

            return node;
        }

    }


    /**
     * When creating a new ParamType, you also need to
     * <ul>
     *     <li>Create a corresponding {@link PolyAlgArg}</li>
     *     <li>Write code for parsing the argument in {@link org.polypheny.db.algebra.polyalg.parser.PolyAlgToAlgConverter}</li>
     *     <li>Add support for your new type to Polypheny-UI</li>
     * </ul>
     */
    @Getter
    public enum ParamType {
        /**
         * The default type. Should only be used if no other type fits better.
         */
        ANY( AnyArg.class ),
        INTEGER( IntArg.class ),
        DOUBLE( DoubleArg.class ),
        STRING( StringArg.class ),

        /**
         * A boolean flag, either "true" or "false".
         */
        BOOLEAN( BooleanArg.class ),

        /**
         * A serialized RexNode
         */
        REX( RexArg.class ),
        AGGREGATE( AggArg.class ),
        LAX_AGGREGATE( LaxAggArg.class ),
        ENTITY( EntityArg.class ),

        // Every new enum also needs to be added to the PolyAlgToAlgConverter like any other new ParamType
        JOIN_TYPE_ENUM( EnumArg.class, JoinAlgType.class ),
        SEMI_JOIN_TYPE_ENUM( EnumArg.class, SemiJoinType.class ),
        MODIFY_OP_ENUM( EnumArg.class, Modify.Operation.class ),
        DISTRIBUTION_TYPE_ENUM( EnumArg.class, AlgDistribution.Type.class ),
        DATAMODEL_ENUM( EnumArg.class, DataModel.class ),
        POLY_TYPE_ENUM( EnumArg.class, PolyType.class ),

        /**
         * A specific field (= column in the relational data model).
         */
        FIELD( FieldArg.class ),

        /**
         * The type of ListArg itself (should only return this value as type if it has no elements).
         */
        LIST( ListArg.class ),

        /**
         *
         */
        COLLATION( CollationArg.class ),

        /**
         * Correlation ID
         */
        CORR_ID( CorrelationArg.class ),

        /**
         * Window.Group
         */
        WINDOW_GROUP( WindowGroupArg.class );

        private final Class<? extends PolyAlgArg> argClass;
        private final Class<? extends Enum<?>> enumClass;


        ParamType( Class<? extends PolyAlgArg> argClass ) {
            this( argClass, null );
        }


        ParamType( Class<? extends PolyAlgArg> argClass, Class<? extends Enum<?>> enumClass ) {
            this.argClass = argClass;
            this.enumClass = enumClass;
        }


        public boolean isEnum() {
            return this.argClass == EnumArg.class;
        }


        public static List<ParamType> getEnumParamTypes() {
            return Arrays.stream( ParamType.values() ).filter( ParamType::isEnum ).toList();
        }

    }


    public enum OperatorTag {
        LOGICAL,
        PHYSICAL,
        ALLOCATION,

        /**
         * Operator should be hidden in simple mode.
         */
        ADVANCED
    }


    public enum ParamTag {

        /**
         * Parameter allows for a (possibly optional) alias (not to be confused with a parameter name alias)
         */
        ALIAS,

        /**
         * Indicates that negative values are not permitted (typically together with IntArg)
         */
        NON_NEGATIVE,

        /**
         * For projects and some other operators it is useful to let the user hide any trivial arguments in the UI
         */
        HIDE_TRIVIAL,

        /**
         * Indicates that this parameter should be treated as a PolyNode
         */
        POLY_NODE,

        /**
         * Indicates that this parameter should be treated as a PolyPath
         */
        POLY_PATH
    }


    public enum SimpleType {
        HIDDEN, // do not show parameter in simple mode and use default value instead
        REX_PREDICATE( ParamType.REX ),
        REX_UINT( ParamType.REX ), // integer >= 0
        SIMPLE_COLLATION( ParamType.COLLATION ),
        SIMPLE_AGG( ParamType.AGGREGATE );

        private final Set<ParamType> compatible; // empty: compatible with all


        SimpleType() {
            this( Set.of() );
        }


        SimpleType( ParamType compatible ) {
            this( Set.of( compatible ) );
        }


        SimpleType( Set<ParamType> compatible ) {
            this.compatible = compatible;
        }


        public boolean isCompatible( ParamType type ) {
            return compatible.isEmpty() || compatible.contains( type );
        }
    }

}
