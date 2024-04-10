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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.JoinType;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.Parameter;
import org.polypheny.db.algebra.polyalg.PolyAlgRegistry;
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
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgAliasedArgument;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgExpression;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgLiteral;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgNamedArgument;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgNode;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgNodeList;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgOperator;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.Pair;

/**
 * Converter class, which transforms PolyAlg in its PolyAlgNode form to an equal AlgNode
 */
public class PolyAlgToAlgConverter {

    private final Snapshot snapshot;
    private final AlgCluster cluster;
    private final RexBuilder builder;


    public PolyAlgToAlgConverter( Snapshot snapshot, AlgCluster cluster ) {
        this.snapshot = snapshot;
        this.cluster = cluster;
        this.builder = cluster.getRexBuilder();
    }


    public AlgRoot convert( PolyAlgNode root ) {
        AlgNode node = buildNode( (PolyAlgOperator) root );

        // Wrap {@link AlgNode} into a RelRoot
        final AlgDataType tupleType = node.getTupleType();
        final List<Pair<Integer, String>> fields = Pair.zip( IntStream.range( 0, tupleType.getFieldCount() ).boxed().toList(), tupleType.getFieldNames() );
        final AlgCollation collation =
                node instanceof Sort
                        ? ((Sort) node).collation
                        : AlgCollations.EMPTY;
        return new AlgRoot( node, tupleType, Kind.SELECT, fields, collation );
    }


    private AlgNode buildNode( PolyAlgOperator operator ) {
        PolyAlgDeclaration decl = Objects.requireNonNull( PolyAlgRegistry.getDeclaration( operator.getOpName() ) );

        List<AlgNode> children = operator.getChildren().stream()
                .map( this::buildNode )
                .toList();
        if ( !decl.supportsNumberOfChildren( children.size() ) ) {
            throw new GenericRuntimeException( "Invalid number of children for '" + decl.opName + "'" );
        }

        PolyAlgArgs args = buildArgs( decl, operator.getArguments(), new Context( children, decl.model ) );
        return decl.createNode( args, children, cluster );
    }


    private PolyAlgArgs buildArgs( PolyAlgDeclaration decl, List<PolyAlgNamedArgument> namedArgs, Context ctx ) {
        PolyAlgArgs converted = new PolyAlgArgs( decl );

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

            if ( usedParams.contains( p ) ) {
                throw new GenericRuntimeException( "Argument " + p.getName() + " was used more than once" );
            }
            usedParams.add( p );
            converted.put( p, buildArg( p, aliasedArg, ctx ) );
        }

        if ( !argsToCombine.isEmpty() ) {
            p = decl.getPos( 0 );
            PolyAlgAliasedArgument firstArg = argsToCombine.get( 0 );
            if ( argsToCombine.size() == 1 ) {
                converted.put( p, buildArg( p, firstArg, ctx ) );
            } else {
                PolyAlgNodeList listArg = new PolyAlgNodeList( argsToCombine, firstArg.getPos() );
                converted.put( p, buildList( p, listArg, null, ctx ) );
            }
        }

        if ( !converted.validate( true ) ) {
            throw new GenericRuntimeException( "Missing positional argument" );
        }

        return converted;
    }


    private PolyAlgArg buildArg( Parameter p, PolyAlgAliasedArgument aliasedArg, Context ctx ) {
        PolyAlgNode arg = aliasedArg.getArg();
        String alias = aliasedArg.getAlias();

        if ( p.isMultiValued() ) {
            if ( arg instanceof PolyAlgNodeList ) {
                return buildList( p, (PolyAlgNodeList) arg, alias, ctx );
            }
            return buildList( p, new PolyAlgNodeList( List.of( aliasedArg ), arg.getPos() ), alias, ctx );
        }

        return convertArg( p, aliasedArg, ctx );
    }


    private PolyAlgArg buildList( Parameter p, PolyAlgNodeList listArg, String alias, Context ctx ) {
        List<PolyAlgArg> args = new ArrayList<>();
        for ( PolyAlgNode node : listArg.getPolyAlgList() ) {
            PolyAlgAliasedArgument aliasedArg = (PolyAlgAliasedArgument) node;
            args.add( convertArg( p, aliasedArg, ctx ) );
        }
        // We do not specify aliases for the entire list. Instead, this should happen on an element level (if necessary).
        return new ListArg<>( args );
    }


    private PolyAlgArg convertArg( Parameter p, PolyAlgAliasedArgument aliasedArg, Context ctx ) {
        if ( aliasedArg.getArg() instanceof PolyAlgExpression ) {
            return convertExpression( p, (PolyAlgExpression) aliasedArg.getArg(), aliasedArg.getAlias(), ctx );
        } else {
            throw new GenericRuntimeException( "Nested PolyAlgNodeLists are currently not supported" );
        }
    }


    private PolyAlgArg convertExpression( Parameter p, PolyAlgExpression exp, String alias, Context ctx ) {
        //System.out.println( "PolyAlgExpression: " + exp.toString() + " AS " + alias);
        return switch ( p.getType() ) {
            case ANY -> new AnyArg( exp.toString() );
            case INTEGER -> new IntArg( exp.toInt() );
            case BOOLEAN -> new BooleanArg( exp.toBoolean() );
            case SIMPLE_REX -> {
                RexNode node = convertRexNode( exp, ctx );
                yield new RexArg( node, alias == null ? exp.toString() : alias );
            }
            case AGGREGATE -> {
                // AggregateCall agg = new AggregateCall();
                // return new AggArg( agg );
                throw new GenericRuntimeException( "Aggregate argument not yet implemented" );
            }
            case ENTITY -> new EntityArg( convertEntity( exp, ctx.dataModel ) );
            case JOIN_TYPE_ENUM -> new EnumArg<>( exp.toEnum( JoinType.class ), ParamType.JOIN_TYPE_ENUM );
            case FIELD -> new FieldArg( ctx.getFieldOrdinal( exp.toIdentifier() ) );
            case EMPTY_LIST -> ListArg.EMPTY;
            case COLLATION -> new CollationArg( convertCollation( exp, ctx ) );
            case CORR_ID -> new CorrelationArg( new CorrelationId( exp.toString() ) );
            default -> throw new IllegalStateException( "Unexpected value: " + p.getType() );
        };
    }


    private RexNode convertRexNode( PolyAlgExpression exp, Context ctx ) {
        if ( exp.isCall() ) {
            Operator operator = exp.getOperator();
            List<RexNode> operands = exp.getChildExps().stream().map( e -> convertRexNode( e, ctx ) ).toList();
            return builder.makeCall( operator, operands );
        } else if ( exp.isSingleLiteral() ) {
            PolyAlgLiteral literal = exp.getLiterals().get( 0 );

            // TODO: handle all cases of non-call RexNodes
            if ( literal.isQuoted() ) {
                return builder.makeLiteral( literal.toUnquotedString() );
            } else if ( literal.isNumber() ) {
                return AlgBuilder.literal( literal.toNumber(), builder );
            } else if ( literal.isBoolean() ) {
                return AlgBuilder.literal( literal.toBoolean(), builder );
            } else {
                String str = literal.toString();
                int idx = ctx.getFieldOrdinal( str );
                return RexIndexRef.of( idx, ctx.fields );
            }

        }
        throw new GenericRuntimeException( "Invalid RexNode: " + exp );

    }


    private Entity convertEntity( PolyAlgExpression exp, DataModel dataModel ) {
        String[] names = exp.toIdentifier().split( "\\.", 3 );
        GenericRuntimeException exception = new GenericRuntimeException( "Invalid entity name: " + String.join( ".", names ) );
        String namespaceName;
        String entityName;
        if ( names.length == 2 ) {
            namespaceName = names[0];
            entityName = names[1];
        } else if ( names.length == 1 ) {
            namespaceName = Catalog.DEFAULT_NAMESPACE_NAME;
            entityName = names[0];
        } else {
            throw exception;
        }

        return switch ( dataModel ) {
            case RELATIONAL -> snapshot.rel().getTable( namespaceName, entityName ).orElseThrow( () -> exception );
            case DOCUMENT -> snapshot.doc().getCollection(
                            snapshot.getNamespace( namespaceName ).orElseThrow( () -> exception ).id,
                            entityName )
                    .orElseThrow( () -> exception );
            case GRAPH -> snapshot.graph().getGraph(
                            snapshot.getNamespace( namespaceName ).orElseThrow( () -> exception ).id )
                    .orElseThrow( () -> exception );
        };
    }


    private AlgFieldCollation convertCollation( PolyAlgExpression exp, Context ctx ) {
        List<PolyAlgLiteral> literals = exp.getLiterals();
        int size = literals.size();
        int fieldIndex = ctx.getFieldOrdinal( literals.get( 0 ).toString() );
        return switch ( size ) {
            case 1 -> new AlgFieldCollation( fieldIndex );
            case 2 -> new AlgFieldCollation( fieldIndex, literals.get( 1 ).toDirection() );
            case 3 -> new AlgFieldCollation(
                    fieldIndex,
                    literals.get( 1 ).toDirection(),
                    literals.get( 2 ).toNullDirection() );
            default -> throw new GenericRuntimeException( "Too many values for AlgFieldCollation" );
        };
    }


    private static final class Context {

        private final List<AlgNode> children;
        private final List<String> fieldNames;
        private final List<AlgDataTypeField> fields;
        private final DataModel dataModel;


        private Context( List<AlgNode> children, DataModel dataModel ) {
            this.children = children;
            this.fieldNames = children.stream()
                    .flatMap( node -> node.getTupleType().getFieldNames().stream() )
                    .toList();
            this.fields = children.stream()
                    .flatMap( node -> node.getTupleType().getFields().stream() )
                    .toList();
            this.dataModel = dataModel;
        }


        private AlgNode getChildFromFieldOrdinal( int ord ) {
            int offset = 0;
            for ( AlgNode child : children ) {
                int count = child.getTupleType().getFieldCount();
                if ( ord < offset + count ) {
                    return child;
                }
                offset += count;
            }
            throw new GenericRuntimeException( "Invalid field index" );
        }


        private int getFieldOrdinal( String fieldName ) {
            int idx = fieldNames.indexOf( fieldName );
            if ( idx < 0 ) {
                throw new GenericRuntimeException( "Invalid field name" );
            }
            return idx;
        }

    }

}
