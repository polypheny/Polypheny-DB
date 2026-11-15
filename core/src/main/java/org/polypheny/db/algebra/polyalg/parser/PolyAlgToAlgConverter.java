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

package org.polypheny.db.algebra.polyalg.parser;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.SemiJoinType;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.LaxAggregateCall;
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
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph.SubstitutionGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.information.InformationPolyAlg.PlanType;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexElementRef;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.PolyType;
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
    private final PlanType planType;


    public PolyAlgToAlgConverter( PlanType planType, Snapshot snapshot, AlgCluster cluster ) {
        this.snapshot = snapshot;
        this.cluster = cluster;
        this.builder = cluster.getRexBuilder();
        this.planType = planType;
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
                    throw new GenericRuntimeException( "Positional argument after keyword argument encountered for " + decl.opName );
                }
                if ( canUnpack ) {
                    argsToCombine.add( aliasedArg );
                    continue;
                } else {
                    p = decl.getPos( i );
                    if ( p == null ) {
                        throw new GenericRuntimeException( "Too many positional arguments were given for " + decl.opName );
                    }
                }
            } else {
                noMorePosArgs = true;

                p = decl.getParam( name );
                if ( p == null ) {
                    throw new GenericRuntimeException( "Unexpected keyword argument '" + name + "' for " + decl.opName );
                }
            }

            if ( usedParams.contains( p ) ) {
                throw new GenericRuntimeException( "Argument " + p.getName() + " was used more than once for " + decl.opName );
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
                converted.put( p, buildList( p, listArg, null, ctx, 0 ) );
            }
        }
        if ( !converted.validate( true ) ) {
            throw new GenericRuntimeException( "Missing positional argument for " + decl.opName );
        }

        return converted;
    }


    private PolyAlgArg buildArg( Parameter p, PolyAlgAliasedArgument aliasedArg, Context ctx ) {
        PolyAlgNode arg = aliasedArg.getArg();
        String alias = aliasedArg.getAlias();

        if ( p.isMultiValued() ) {
            if ( arg instanceof PolyAlgNodeList ) {
                return buildList( p, (PolyAlgNodeList) arg, alias, ctx, 0 );
            }
            return buildList( p, new PolyAlgNodeList( List.of( aliasedArg ), arg.getPos() ), alias, ctx, 0 );
        }

        return convertArg( p, aliasedArg, ctx, 0 );
    }


    private PolyAlgArg buildList( Parameter p, PolyAlgNodeList listArg, String alias, Context ctx, int depth ) {
        List<PolyAlgArg> args = new ArrayList<>();
        for ( PolyAlgNode node : listArg.getPolyAlgList() ) {
            PolyAlgAliasedArgument aliasedArg = (PolyAlgAliasedArgument) node;
            args.add( convertArg( p, aliasedArg, ctx, depth + 1 ) ); // aliasedArg is within a list, so we increase its depth by 1
        }
        // We do not specify aliases for the entire list. Instead, this should happen on an element level (if necessary).
        return new ListArg<>( args );
    }


    private PolyAlgArg convertArg( Parameter p, PolyAlgAliasedArgument aliasedArg, Context ctx, int depth ) {
        if ( aliasedArg.getArg() instanceof PolyAlgExpression ) {
            // no more nested args
            if ( depth != p.getMultiValued() ) {
                throw new GenericRuntimeException( "Invalid depth for list argument " + p.getName() );
            }
            return convertExpression( p, (PolyAlgExpression) aliasedArg.getArg(), aliasedArg.getAlias(), ctx );
        } else if ( aliasedArg.getArg() instanceof PolyAlgNodeList ) {
            return buildList( p, (PolyAlgNodeList) aliasedArg.getArg(), aliasedArg.getAlias(), ctx, depth );
        } else {
            throw new GenericRuntimeException( "This PolyAlgNode type is currently not supported" );
        }
    }


    /**
     * Convertes the PolyAlgExpression for the specified parameter to a PolyAlgArg of the same parameter type.
     * The logic for converting a new parameter type should be added here.
     *
     * @param p the target parameter
     * @param exp the expression to convert
     * @param alias an optional alias (null if not specified)
     * @param ctx any additional context
     * @return The converted PolyAlgArg instance whose getType() method must be compatible with p.
     */
    private PolyAlgArg convertExpression( Parameter p, PolyAlgExpression exp, String alias, Context ctx ) {
        if ( p.requiresAlias && (alias == null || alias.isEmpty()) ) {
            throw new GenericRuntimeException( "Missing <AS> for " + p.getName() );
        }
        ParamType pType = p.getType();
        return switch ( pType ) {
            case ANY -> new AnyArg( exp.toString() );
            case INTEGER -> new IntArg( exp.toInt( p.getTags() ) );
            case DOUBLE -> new DoubleArg( exp.toDouble( p.getTags() ) );
            case BOOLEAN -> new BooleanArg( exp.toBoolean() );
            case STRING -> new StringArg( exp.toString() );
            case REX -> {
                RexNode node = convertRexNode( exp, ctx );
                yield new RexArg( node, alias == null ? exp.getDefaultAlias() : alias );
            }
            case AGGREGATE -> new AggArg( convertAggCall( exp, alias, ctx ) );
            case LAX_AGGREGATE -> new LaxAggArg( convertLaxAggCall( exp, alias, ctx ) );
            case ENTITY -> new EntityArg( convertEntity( exp, ctx ), snapshot, ctx.getNonNullDataModel() );
            case JOIN_TYPE_ENUM -> new EnumArg<>( exp.toEnum( JoinAlgType.class ), pType );
            case SEMI_JOIN_TYPE_ENUM -> new EnumArg<>( exp.toEnum( SemiJoinType.class ), pType );
            case MODIFY_OP_ENUM -> new EnumArg<>( exp.toEnum( Modify.Operation.class ), pType );
            case DISTRIBUTION_TYPE_ENUM -> new EnumArg<>( exp.toEnum( AlgDistribution.Type.class ), pType );
            case DATAMODEL_ENUM -> new EnumArg<>( exp.toEnum( DataModel.class ), pType );
            case POLY_TYPE_ENUM -> new EnumArg<>( exp.toEnum( PolyType.class ), pType );
            case FIELD -> new FieldArg( ctx.getFieldOrdinalOrThrow( exp.toIdentifier() ) );
            case LIST -> ListArg.EMPTY;
            case COLLATION -> new CollationArg( convertCollation( exp, ctx ) );
            case CORR_ID -> new CorrelationArg( new CorrelationId( exp.toString() ) );
            case WINDOW_GROUP -> throw new NotImplementedException( "Parsing of WindowGroup arguments is not yet supported." );
            default -> throw new IllegalStateException( "Unexpected value: " + p.getType() );
        };
    }


    private RexNode convertRexNode( PolyAlgExpression exp, Context ctx ) {
        if ( exp.isElementRef() ) {
            RexNode collectionRef = convertRexNode( exp.getOnlyChild(), ctx );
            return new RexElementRef( collectionRef, DocumentType.ofDoc() );
        } else if ( exp.isCall() ) {
            return convertRexCall( exp, ctx );
        } else if ( exp.isSingleLiteral() ) {
            PolyAlgLiteral literal = exp.getLiterals().get( 0 );
            return convertRexLiteral( literal, exp.getAlgDataType(), ctx );
        }
        throw new GenericRuntimeException( "Invalid RexNode: " + exp );
    }


    private RexNode convertRexCall( PolyAlgExpression exp, Context ctx ) {
        Operator operator = exp.getOperator( ctx.getNonNullDataModel() );
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
     * This does not have to be a RexLiteral, but can also be a RexIndexRef or RexDynamicParam or ...
     *
     * @param literal the PolyAlgLiteral to be converted
     * @param type the AlgDataType specified in the PolyAlgebra
     * @param ctx Context
     * @return A RexNode representing the specified PolyAlgLiteral
     */
    private RexNode convertRexLiteral( PolyAlgLiteral literal, AlgDataType type, Context ctx ) {

        // first handle cases where explicit type doesn't matter
        if ( literal.getType() == LiteralType.CORRELATION_VAR ) {
            Pair<CorrelationId, String> pair = literal.toCorrelationFieldAccess();
            RexNode corr = builder.makeCorrel( ctx.getChildTupleType( 0 ), pair.left );
            return builder.makeFieldAccess( corr, pair.right, true );
        }

        if ( type == null ) {
            // no explicit type information, so we can only guess which one from the LiteralType the parser detected:
            return switch ( literal.getType() ) {
                case QUOTED -> {
                    String str = literal.toUnquotedString();
                    int idx = ctx.getFieldOrdinal( str );
                    // limitation: cannot have string literal in double quotes called the same as a field, as we would always pick the field
                    yield idx >= 0 && literal.isDoubleQuoted() ? RexIndexRef.of( idx, ctx.fields ) : builder.makeLiteral( str );
                }
                case NUMBER -> AlgBuilder.literal( literal.toNumber(), builder );
                case BOOLEAN -> AlgBuilder.literal( literal.toBoolean(), builder );
                case NULL -> AlgBuilder.literal( null, builder );
                case POLY_VALUE -> builder.makeLiteral( literal.toPolyValue() );
                case STRING -> {
                    String str = literal.toString();
                    DataModel dataModel = ctx.getNonNullDataModel();
                    if ( dataModel == DataModel.DOCUMENT || (dataModel == DataModel.GRAPH && str.contains( "@" )) ) {
                        String[] idxSplit = str.split( "@", 2 );
                        Integer idx = null;
                        if ( idxSplit.length == 2 ) {
                            idx = Integer.parseInt( idxSplit[1] );
                        }
                        yield RexNameRef.create( List.of( idxSplit[0].split( "\\." ) ), idx, ctx.children.get( 0 ).getTupleType() );
                    } else {
                        // indexRef
                        int idx = ctx.getFieldOrdinalOrThrow( str );
                        yield RexIndexRef.of( idx, ctx.fields );
                    }
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
            if ( literal.getType() == LiteralType.LOCAL_REF ) {
                return new RexLocalRef( literal.toLocalRef(), type );
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
                case NODE -> builder.makeLiteral( literal.toPolyValue(), type );
                case DOCUMENT -> builder.makeDocumentLiteral( switch ( literal.getType() ) {
                    case NUMBER -> ((RexLiteral) AlgBuilder.literal( literal.toNumber(), builder )).value;
                    case BOOLEAN -> ((RexLiteral) AlgBuilder.literal( literal.toBoolean(), builder )).value;
                    default -> PolyString.of( str );
                } );
                default -> throw new GenericRuntimeException( "Unsupported type: " + type.getFullTypeString() );
            };
        }

    }


    private Entity convertEntity( PolyAlgExpression exp, Context ctx ) {
        if ( planType == PlanType.PHYSICAL ) {
            PhysicalEntity e = getPhysicalEntity( exp.toIdentifier() );
            ctx.updateFieldNamesIfEmpty( e );
            return e;
        }
        String[] atSplit = exp.toIdentifier().split( "@", 2 );

        String[] names = atSplit[0].split( "\\.", 2 );
        GenericRuntimeException exception = new GenericRuntimeException( "Invalid entity name: " + String.join( ".", names ) );

        String namespaceName;
        String rest = null; // contains everything after the first "."
        if ( names.length == 2 ) {
            namespaceName = names[0];
            rest = names[1];
        } else if ( names.length == 1 ) {
            namespaceName = names[0];
        } else {
            throw exception; // names.length == 0
        }

        LogicalNamespace ns = snapshot.getNamespace( namespaceName ).orElseThrow( () -> new GenericRuntimeException( "no namespace named " + namespaceName ) );

        if ( planType == PlanType.ALLOCATION ) {
            AllocationEntity e = getAllocationEntity( atSplit, ns, rest );
            if ( ns.dataModel == DataModel.GRAPH && rest != null ) {
                return e.withName( rest );
            }
            return e;
        }

        return switch ( ns.dataModel ) {
            case RELATIONAL -> {
                if ( rest == null ) {
                    yield new SubstitutionGraph( ns.id, "sub", false, ns.caseSensitive, List.of() );
                } else if ( ctx.getNonNullDataModel() == DataModel.GRAPH ) {
                    yield getSubGraph( ns, rest );
                }
                yield snapshot.rel().getTable( ns.id, rest ).orElseThrow( () -> exception );
            }
            case DOCUMENT -> {
                if ( rest == null ) {
                    yield new SubstitutionGraph( ns.id, "sub", false, ns.caseSensitive, List.of() );
                } else if ( ctx.getNonNullDataModel() == DataModel.GRAPH ) {
                    yield getSubGraph( ns, rest );
                }
                yield snapshot.doc().getCollection( ns.id, rest ).orElseThrow( () -> exception );
            }
            case GRAPH -> {
                if ( rest == null ) {
                    yield snapshot.graph().getGraph( ns.id ).orElseThrow( () -> new GenericRuntimeException( "no graph with id " + ns.id ) );
                } else {
                    yield getSubGraph( ns, rest );
                }
            }
        };
    }


    private SubstitutionGraph getSubGraph( LogicalNamespace ns, String namesStr ) {
        List<String> subNames = Arrays.asList( namesStr.split( "\\." ) );
        String name = subNames.size() == 1 ? subNames.get( 0 ) : "sub";
        return new SubstitutionGraph( ns.id, name, false, ns.caseSensitive, subNames.stream().map( PolyString::of ).toList() );
    }


    private AllocationEntity getAllocationEntity( String[] atSplit, LogicalNamespace ns, String entityName ) {
        // atSplit has structure [ns.entity, adapterName.partition]
        GenericRuntimeException exception = new GenericRuntimeException( "Invalid AllocationEntity: " + String.join( "@", atSplit ) );
        if ( atSplit.length != 2 ) {
            throw exception;
        }
        LogicalEntity logicalEntity = getLogicalEntity( entityName, ns );
        String[] apSplit = atSplit[1].split( "\\.", 2 ); // [adapterName, partition]
        LogicalAdapter adapter = snapshot.getAdapter( apSplit[0] ).orElseThrow( () -> exception );
        AllocationPlacement placement = snapshot.alloc().getPlacement( adapter.id, logicalEntity.id ).orElseThrow( () -> exception );

        if ( apSplit.length == 1 ) {
            List<AllocationEntity> entities = snapshot.alloc().getAllocsOfPlacement( placement.id );
            if ( entities.isEmpty() ) {
                throw exception;
            }
            return entities.get( 0 );
        }
        try {
            return snapshot.alloc().getAlloc( placement.id, Long.parseLong( apSplit[1] ) ).orElseThrow( () -> exception );
        } catch ( NumberFormatException e ) {
            long partitionId = snapshot.alloc().getPartitionFromName( logicalEntity.id, apSplit[1] ).orElseThrow( () -> exception ).id;
            return snapshot.alloc().getAlloc( placement.id, partitionId ).orElseThrow( () -> exception );
        }

    }


    private LogicalEntity getLogicalEntity( String entityName, LogicalNamespace ns ) {
        if ( entityName == null ) {
            if ( ns.dataModel == DataModel.GRAPH ) {
                return snapshot.graph().getGraph( ns.id ).orElseThrow(
                        () -> new GenericRuntimeException( "Cannot find entity: " + ns.name ) );
            } else {
                throw new GenericRuntimeException( "Entity name must not be null for a non-graph namespace." );
            }
        }
        return snapshot.getLogicalEntity( ns.id, entityName ).orElseThrow(
                () -> new GenericRuntimeException( "Cannot find entity: " + ns.name + "." + entityName ) );
    }


    private PhysicalEntity getPhysicalEntity( String s ) {
        GenericRuntimeException exception = new GenericRuntimeException( "Invalid PhysicalEntity: " + s );

        String[] apSplit = s.split( "\\.", 2 ); // [adapterName, physicalId]
        LogicalAdapter adapter = snapshot.getAdapter( apSplit[0] ).orElseThrow( () -> exception );
        long physicalId = Long.parseLong( apSplit[1] );
        return Catalog.getInstance().getAdapterCatalog( adapter.id ).orElseThrow( () -> exception ).getPhysical( physicalId );
    }


    private AlgFieldCollation convertCollation( PolyAlgExpression exp, Context ctx ) {
        List<PolyAlgLiteral> literals = exp.getLiterals();
        int size = literals.size();
        int fieldIndex = ctx.getFieldOrdinalOrThrow( literals.get( 0 ).toString() );
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
        boolean isDistinct = false;
        for ( PolyAlgExpression child : exp.getChildExps() ) {
            String fieldName = child.getLastLiteral().toString();
            if ( child.getLiterals().size() == 2 && child.getLiterals().get( 0 ).toString().equals( "DISTINCT" ) ) {
                isDistinct = true;
            }
            args.add( ctx.getFieldOrdinalOrThrow( fieldName ) );
        }

        int filter = -1;
        PolyAlgExpressionExtension extension = exp.getExtension( ExtensionType.FILTER );
        if ( extension != null ) {
            PolyAlgLiteral filterLiteral = extension.getLiterals().get( 0 );
            filter = ctx.getFieldOrdinalOrThrow( filterLiteral.toString() );
        }
        boolean isApproximate = exp.getExtension( ExtensionType.APPROXIMATE ) != null;
        // TODO: parse WITHIN clause for Collation (low priority, since not supported by practically all AggFunctions)
        return AggregateCall.create( f, isDistinct, isApproximate, args, filter, AlgCollations.EMPTY,
                0, ctx.children.get( 0 ), null, name ); // type can be null with this create method
    }


    private LaxAggregateCall convertLaxAggCall( PolyAlgExpression exp, String name, Context ctx ) {
        RexNode input = null;
        if ( !exp.getChildExps().isEmpty() ) {
            input = convertRexNode( exp.getOnlyChild(), ctx );
        }
        if ( name == null ) {
            name = exp.getDefaultAlias();
        }
        return LaxAggregateCall.create( name, exp.getAggFunction(), input );
    }


    private static final class Context {

        private final List<AlgNode> children;
        private List<String> fieldNames;
        private List<AlgDataTypeField> fields;
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


        private int getFieldOrdinalOrThrow( String fieldName ) {
            int idx = getFieldOrdinal( fieldName );
            if ( idx < 0 ) {
                throw new GenericRuntimeException( "Invalid field name: '" + fieldName + "'" );
            }
            return idx;
        }


        private int getFieldOrdinal( String fieldName ) {
            return fieldNames.indexOf( fieldName );
        }


        private AlgDataType getDataTypeFromFieldName( String fieldName ) {
            int ord = getFieldOrdinalOrThrow( fieldName );
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


        /**
         * The data model could be null for common AlgNodes.
         * In this case this method returns the default DataModel.
         *
         * @return the DataModel of this context or the default DataModel if it is null
         */
        public DataModel getNonNullDataModel() {
            return Objects.requireNonNullElse( dataModel, DataModel.getDefault() );
        }


        /**
         * In the case of a leaf node (e.g. a SCAN operation), fieldNames are empty.
         * We can manually add the fieldNames given the scanned entity to allow other arguments to use a field name instead of an index.
         *
         * @param e The entity that defines the field names for this node
         */
        public void updateFieldNamesIfEmpty( PhysicalEntity e ) {
            if ( fieldNames.isEmpty() ) {
                fieldNames = e.getTupleType().getFieldNames();
                fields = e.getTupleType().getFields();
            }
        }

    }

}
