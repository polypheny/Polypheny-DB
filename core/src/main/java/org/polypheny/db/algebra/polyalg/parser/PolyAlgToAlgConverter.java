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

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
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
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.Parameter;
import org.polypheny.db.algebra.polyalg.PolyAlgRegistry;
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
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgAliasedArgument;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgExpression;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgExpressionExtension;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgExpressionExtension.ExtensionType;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgLiteral;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgLiteral.LiteralType;
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
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;

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
        PolyAlgDeclaration decl = Objects.requireNonNull( PolyAlgRegistry.getDeclaration( operator.getOpName() ), "'" + operator.getOpName() + "' is not a registered PolyAlg Operator." );

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
        } else if ( aliasedArg.getArg() instanceof PolyAlgNodeList ) {
            return buildList( p, (PolyAlgNodeList) aliasedArg.getArg(), aliasedArg.getAlias(), ctx );
            //throw new GenericRuntimeException( "Nested PolyAlgNodeLists are currently not supported" );
        } else {
            throw new GenericRuntimeException( "This PolyAlgNode type is currently not supported" );
        }
    }


    private PolyAlgArg convertExpression( Parameter p, PolyAlgExpression exp, String alias, Context ctx ) {
        //System.out.println( "PolyAlgExpression: " + exp.toString() + " AS " + alias);
        ParamType pType = p.getType();
        return switch ( pType ) {
            case ANY -> new AnyArg( exp.toString() );
            case INTEGER -> new IntArg( exp.toInt() );
            case BOOLEAN -> new BooleanArg( exp.toBoolean() );
            case STRING -> new StringArg( exp.toString() );
            case SIMPLE_REX -> {
                RexNode node = convertRexNode( exp, ctx );
                yield new RexArg( node, alias == null ? exp.getDefaultAlias() : alias );
            }
            case AGGREGATE -> new AggArg( convertAggCall( exp, alias, ctx ) );
            case ENTITY -> new EntityArg( convertEntity( exp, ctx.dataModel ) );
            case JOIN_TYPE_ENUM -> new EnumArg<>( exp.toEnum( JoinAlgType.class ), ParamType.JOIN_TYPE_ENUM );
            case MODIFY_OP_ENUM -> new EnumArg<>( exp.toEnum( Modify.Operation.class ), ParamType.MODIFY_OP_ENUM );
            case FIELD -> new FieldArg( ctx.getFieldOrdinal( exp.toIdentifier() ) );
            case EMPTY_LIST -> ListArg.EMPTY;
            case COLLATION -> new CollationArg( convertCollation( exp, ctx ) );
            case CORR_ID -> new CorrelationArg( new CorrelationId( exp.toString() ) );
            default -> throw new IllegalStateException( "Unexpected value: " + p.getType() );
        };
    }


    private RexNode convertRexNode( PolyAlgExpression exp, Context ctx ) {
        if ( exp.isCall() ) {
            return convertRexCall( exp, ctx );
        } else if ( exp.isSingleLiteral() ) {
            PolyAlgLiteral literal = exp.getLiterals().get( 0 );
            return convertRexLiteral( literal, exp.getAlgDataType(), ctx );
        }
        throw new GenericRuntimeException( "Invalid RexNode: " + exp );
    }


    private RexNode convertRexCall( PolyAlgExpression exp, Context ctx ) {
        Operator operator = exp.getOperator();
        if ( operator.getOperatorName() == OperatorName.CAST ) {
            RexNode child = convertRexNode( exp.getOnlyChild(), ctx );
            return new RexCall( exp.getAlgDataTypeForCast(), operator, ImmutableList.of( child ) );
        }
        // TODO: handle other special kinds of calls (Kind.NEW_SPECIFICATION can also specify cast type...)
        List<RexNode> operands = exp.getChildExps().stream().map( e -> convertRexNode( e, ctx ) ).toList();

        return builder.makeCall( operator, operands );
    }


    /**
     * Converts a PolyAlgLiteral into an appropriate RexNode.
     * This does not have to be a RexLiteral, but can also be a RexIndexRef or RexDynamicParam.
     *
     * @param literal the PolyAlgLiteral to be converted
     * @param type the AlgDataType specified in the PolyAlgebra
     * @param ctx Context
     * @return A RexNode representing the specified PolyAlgLiteral
     */
    private RexNode convertRexLiteral( PolyAlgLiteral literal, AlgDataType type, Context ctx ) {
        // TODO: handle all cases of non-call RexNodes

        // first handle cases where explicit type doesn't matter
        if ( literal.getType() == LiteralType.CORRELATION_VAR ) {
            Pair<CorrelationId, String> pair = literal.toCorrelationFieldAccess();
            RexNode corr = builder.makeCorrel( ctx.getChildTupleType( 0 ), pair.left );
            return builder.makeFieldAccess( corr, pair.right, true );
        }

        if ( type == null ) {
            // no explicit type information, so we can only guess which one from the LiteralType the parser detected:
            return switch ( literal.getType() ) {
                case QUOTED -> builder.makeLiteral( literal.toUnquotedString() );
                case NUMBER -> AlgBuilder.literal( literal.toNumber(), builder );
                case BOOLEAN -> AlgBuilder.literal( literal.toBoolean(), builder );
                case NULL -> AlgBuilder.literal( null, builder );
                case STRING -> {
                    String str = literal.toString();
                    int idx = ctx.getFieldOrdinal( str );
                    yield RexIndexRef.of( idx, ctx.fields );
                }
                default -> throw new GenericRuntimeException( "Invalid Literal: '" + literal + "'" );
            };
        } else {
            if ( literal.getType() == LiteralType.DYNAMIC_PARAM ) {
                return builder.makeDynamicParam( type, literal.toDynamicParam() );
            }
            if ( literal.getType() == LiteralType.NULL ) {
                return builder.makeNullLiteral( type );
            }
            String str = literal.toUnquotedString();
            return switch ( type.getPolyType() ) {
                case BOOLEAN -> builder.makeLiteral( literal.toBoolean() );
                case TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL -> builder.makeExactLiteral( new BigDecimal( str ), type );
                case FLOAT, REAL, DOUBLE -> builder.makeApproxLiteral( new BigDecimal( str ), type );
                case DATE -> builder.makeDateLiteral( new DateString( str ) );
                case TIME -> builder.makeTimeLiteral( new TimeString( str ), type.getPrecision() );
                case TIMESTAMP -> builder.makeTimestampLiteral( new TimestampString( str ), type.getPrecision() );
                case CHAR, VARCHAR -> builder.makeLiteral( PolyString.of( str ), type, type.getPolyType() );
                case NULL -> builder.constantNull();
                default -> throw new GenericRuntimeException( "Unsupported type: " + type.getFullTypeString() );
            };
        }

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


    private AggregateCall convertAggCall( PolyAlgExpression exp, String name, Context ctx ) {
        AggFunction f = exp.getAggFunction();
        List<Integer> args = new ArrayList<>();
        AlgDataType type = null;
        boolean isDistinct = false;
        for ( PolyAlgExpression child : exp.getChildExps() ) {
            String fieldName = child.getLastLiteral().toString();
            if ( child.getLiterals().size() == 2 && child.getLiterals().get( 0 ).toString().equals( "DISTINCT" ) ) {
                isDistinct = true;
            }
            args.add( ctx.getFieldOrdinal( fieldName ) );
            if ( type == null ) {
                type = ctx.getDataTypeFromFieldName( fieldName );
            }
        }

        int filter = -1;
        PolyAlgExpressionExtension extension = exp.getExtension( ExtensionType.FILTER );
        if ( extension != null ) {
            PolyAlgLiteral filterLiteral = extension.getLiterals().get( 0 );
            filter = ctx.getFieldOrdinal( filterLiteral.toString() );
        }
        boolean isApproximate = exp.getExtension( ExtensionType.APPROXIMATE ) != null;
        return AggregateCall.create( f, isDistinct, isApproximate, args, filter, AlgCollations.EMPTY, // TODO: parse WITHIN for Collation
                0, ctx.children.get( 0 ), type, name ); // type can be null with this create method
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
                throw new GenericRuntimeException( "Invalid field name: '" + fieldName + "'" );
            }
            return idx;
        }


        private AlgDataType getDataTypeFromFieldName( String fieldName ) {
            int ord = getFieldOrdinal( fieldName );
            int offset = 0;
            for ( AlgNode child : children ) {
                int count = child.getTupleType().getFieldCount();
                if ( ord < offset + count ) {
                    return child.getTupleType().getFields().get( ord - offset ).getType();
                }
                offset += count;
            }
            throw new GenericRuntimeException( "Invalid field index" );
        }


        public AlgDataType getChildTupleType( int idx ) {
            return children.get( idx ).getTupleType();
        }

    }

}
