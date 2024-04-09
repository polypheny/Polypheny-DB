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

package org.polypheny.db.algebra.polyalg.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.NonNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.JoinType;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.Parameter;
import org.polypheny.db.algebra.polyalg.PolyAlgRegistry;
import org.polypheny.db.algebra.polyalg.arguments.AnyArg;
import org.polypheny.db.algebra.polyalg.arguments.BooleanArg;
import org.polypheny.db.algebra.polyalg.arguments.CorrelationArg;
import org.polypheny.db.algebra.polyalg.arguments.EntityArg;
import org.polypheny.db.algebra.polyalg.arguments.EnumArg;
import org.polypheny.db.algebra.polyalg.arguments.IntArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgAliasedArgument;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgExpression;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgNamedArgument;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgNode;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgNodeList;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgOperator;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexNode;

/**
 * Converter class, which transforms PolyAlg in its PolyAlgNode form to an equal AlgNode
 */
public class PolyAlgToAlgConverter {

    private final AlgCluster cluster;


    public PolyAlgToAlgConverter( @NonNull AlgCluster cluster ) {
        this.cluster = cluster;
    }


    public AlgRoot convert( PolyAlgNode root ) {
        AlgNode node = buildNode( (PolyAlgOperator) root );
        return null;
    }


    public static AlgNode buildNode( PolyAlgOperator operator ) {
        PolyAlgDeclaration declaration = Objects.requireNonNull( PolyAlgRegistry.getDeclaration( operator.getOpName() ) );
        System.out.println( declaration.opName + "[ " + operator.getChildren() + " ](" );

        List<AlgNode> children = operator.getChildren().stream()
                .map( PolyAlgToAlgConverter::buildNode )
                .toList();

        return declaration.createNode( buildArgs( declaration, operator.getArguments() ), children );
    }


    private static PolyAlgArgs buildArgs( PolyAlgDeclaration decl, List<PolyAlgNamedArgument> namedArgs ) {
        PolyAlgArgs converted = new PolyAlgArgs( decl );
        Map<Parameter, PolyAlgAliasedArgument> args = new HashMap<>();
        List<PolyAlgAliasedArgument> keyArgs = new ArrayList<>();

        boolean noMorePosArgs = false;
        Set<Parameter> usedParams = new HashSet<>();

        boolean canUnpack = decl.canUnpackValues();
        List<PolyAlgAliasedArgument> argsToCombine = new ArrayList<>(); // only used if(canUnpack) to temporarily store posArgs

        Parameter p;
        for ( int i = 0; i < namedArgs.size(); i++ ) {
            PolyAlgNamedArgument namedArg = namedArgs.get( i );
            String name = namedArg.getName();
            PolyAlgAliasedArgument aliasedArg = namedArg.getAliasedArg();

            if ( namedArg.isPositionalArg() ) {
                if ( noMorePosArgs ) {
                    throw new GenericRuntimeException( "Positional argument after keyword argument" );
                }
                if ( canUnpack ) {
                    argsToCombine.add( aliasedArg );
                    continue;
                } else {
                    p = decl.getPos( i ); // TODO: handle invalid arguments
                }
            } else {
                noMorePosArgs = true;

                p = decl.getParam( name );
            }
            converted.put( p, buildArg( p, aliasedArg ) );
            usedParams.add( p );
        }

        if ( !argsToCombine.isEmpty() ) {
            p = decl.getPos( 0 );
            PolyAlgAliasedArgument firstArg = argsToCombine.get( 0 );
            if ( argsToCombine.size() == 1 ) {
                converted.put( p, buildArg( p, firstArg ) );
            } else {
                PolyAlgNodeList listArg = new PolyAlgNodeList( argsToCombine, firstArg.getPos() );
                converted.put( p, buildArg( p, listArg, null ) );
            }
        }

        if ( !converted.validate( true ) ) {
            throw new GenericRuntimeException( "Missing positional argument" );
        }

        return converted;
    }


    private static PolyAlgArg buildArg( Parameter p, PolyAlgAliasedArgument aliasedArg ) {
        PolyAlgNode arg = aliasedArg.getArg();
        String alias = aliasedArg.getAlias();

        if ( p.isMultiValued() ) {
            if ( arg instanceof PolyAlgNodeList ) {
                return buildArg( p, (PolyAlgNodeList) arg, alias );
            }
            return buildArg( p, new PolyAlgNodeList( List.of( aliasedArg ), arg.getPos() ), alias );
        }

        return convertArg( p.getType(), aliasedArg );
    }


    private static PolyAlgArg buildArg( Parameter p, PolyAlgNodeList listArg, String alias ) {
        List<PolyAlgArg> args = new ArrayList<>();
        for ( PolyAlgNode node : listArg.getPolyAlgList() ) {
            PolyAlgAliasedArgument aliasedArg = (PolyAlgAliasedArgument) node;
            args.add( convertArg( p.getType(), aliasedArg ) );
        }
        // We do not specify aliases for the entire list. Instead, this should happen on an element level (if necessary).
        return new ListArg<>( args );
    }


    private static PolyAlgArg convertArg( ParamType type, PolyAlgAliasedArgument aliasedArg ) {
        if ( aliasedArg.getArg() instanceof PolyAlgExpression ) {
            return convertExpression( type, (PolyAlgExpression) aliasedArg.getArg(), aliasedArg.getAlias() );
        } else {
            throw new GenericRuntimeException( "Nested PolyAlgNodeLists are currently not supported" );
        }
    }


    private static PolyAlgArg convertExpression( ParamType type, PolyAlgExpression exp, String alias ) {

        switch ( type ) {
            case ANY -> {
                // Actions for ParamType.ANY
                return new AnyArg( exp.toString() );
            }
            case INTEGER -> {
                return new IntArg( exp.toInt() );
            }
            case BOOLEAN -> {
                return new BooleanArg( exp.toBoolean() );
            }
            case SIMPLE_REX -> {
                return new RexArg( convertRexNode( exp ), alias );
            }
            case AGGREGATE -> {
                // AggregateCall agg = new AggregateCall();
                // return new AggArg( agg );
            }
            case ENTITY -> {
                return new EntityArg( convertEntity( exp ) );
            }
            case JOIN_TYPE_ENUM -> {
                return new EnumArg<>( exp.toEnum( JoinType.class ), ParamType.JOIN_TYPE_ENUM );
            }
            case FIELD -> {
                // Actions for ParamType.FIELD
            }
            case EMPTY_LIST -> {
                return ListArg.EMPTY;
            }
            case COLLATION -> {
                // Actions for ParamType.COLLATION
            }
            case CORR_ID -> {
                return new CorrelationArg( new CorrelationId( exp.toString() ) );
            }
            default -> throw new IllegalStateException( "Unexpected value: " + type );
        }
        return null;
    }


    private static RexNode convertRexNode( PolyAlgExpression exp ) {
        return null;
    }


    private static Entity convertEntity( PolyAlgExpression exp ) {
        List<String> literals = exp.getLiteralsAsStrings();
        if ( literals.size() == 3 && literals.get( 1 ).equals( "." ) ) {
            String namespaceName = literals.get( 0 );
            String entityName = literals.get( 2 );

            return null;
        } else if ( literals.size() == 1 ) {
            String entityName = literals.get( 0 );

            return null;
        }
        throw new GenericRuntimeException( "Invalid entity name: " + String.join( "", literals ) );
    }

}
