/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.cypher.expression;

import static org.polypheny.db.algebra.operators.OperatorName.CYPHER_POINT;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.cypher2alg.CypherSyntaxException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.pattern.CypherEveryPathPattern;
import org.polypheny.db.functions.CypherScalarFunction;
import org.polypheny.db.functions.CypherTemporalFunction;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.Pair;

@Getter
public class CypherFunctionInvocation extends CypherExpression {

    private static final List<String> operatorNames = Arrays.stream( OperatorName.values() ).map( Enum::name ).toList();
    private final ParserPos namePos;
    private final List<String> namespace;
    private final boolean distinct;
    private final List<CypherExpression> arguments;
    private final OperatorName op;
    private final @Nullable CypherScalarFunction scalarFunction;
    private final @Nullable CypherTemporalFunction temporalFunction;


    public CypherFunctionInvocation( ParserPos beforePos, ParserPos namePos, List<String> namespace, String image, boolean distinct, List<CypherExpression> arguments ) {
        super( beforePos );
        this.namePos = namePos;
        this.namespace = namespace;
        this.scalarFunction = namespace.isEmpty() ? CypherScalarFunction.named( image ) : null;
        String qualifiedName = namespace.isEmpty() ? image : String.join( ".", namespace ) + "." + image;
        this.temporalFunction = namespace.size() <= 1 && !image.contains( "." )
                && (namespace.isEmpty() || !namespace.get( 0 ).contains( "." ))
                ? CypherTemporalFunction.named( qualifiedName )
                : null;
        if ( temporalFunction != null ) {
            String arityError = temporalFunction.arityError( arguments.size() );
            if ( arityError != null ) {
                throw new CypherSyntaxException( arityError );
            }
            this.op = OperatorName.CYPHER_TEMPORAL_FUNCTION;
        } else if ( !namespace.isEmpty() ) {
            this.op = switch ( qualifiedName.toLowerCase( Locale.ROOT ) ) {
                case "point.distance" -> OperatorName.DISTANCE;
                case "point.withinbbox" -> OperatorName.CYPHER_WITHIN_BBOX;
                case "point.withingeometry" -> OperatorName.CYPHER_WITHIN_GEOMETRY;
                default -> throw new CypherSyntaxException( "UnknownFunction: " + qualifiedName );
            };
        } else if ( scalarFunction != null ) {
            String arityError = scalarFunction.arityError( arguments.size() );
            if ( arityError != null ) {
                throw new CypherSyntaxException( arityError );
            }
            this.op = scalarFunction == CypherScalarFunction.RAND ? OperatorName.CYPHER_RAND : OperatorName.CYPHER_SCALAR_FUNCTION;
        } else if ( image.equalsIgnoreCase( "rels" ) ) {
            this.op = OperatorName.CYPHER_RELATIONSHIPS;
        } else if ( operatorNames.contains( image.toUpperCase( Locale.ROOT ) ) ) {
            this.op = OperatorName.valueOf( image.toUpperCase( Locale.ROOT ) );
        } else if ( operatorNames.contains( "CYPHER_" + image.toUpperCase( Locale.ROOT ) ) ) {
            this.op = OperatorName.valueOf( "CYPHER_" + image.toUpperCase( Locale.ROOT ) );
        } else if ( image.equals( "withinBBox" ) ) {
            this.op = OperatorName.CYPHER_WITHIN_BBOX;
        } else if ( image.equals( "withinGeometry" ) ) {
            this.op = OperatorName.CYPHER_WITHIN_GEOMETRY;
        } else if ( image.equalsIgnoreCase( "startNode" ) ) {
            this.op = OperatorName.CYPHER_START_NODE;
        } else if ( image.equalsIgnoreCase( "endNode" ) ) {
            this.op = OperatorName.CYPHER_END_NODE;
        } else {
            throw new GenericRuntimeException( "Used function is not supported!" );
        }
        this.distinct = distinct;
        this.arguments = arguments;
    }


    private RexNode getVectorDistanceRex( CypherContext context, RexType type ) {
        if ( arguments.size() != 3 ) {
            throw new GenericRuntimeException( "vector_distance requires exactly 3 arguments" );
        }

        RexNode v1 = arguments.get( 0 ).getRex( context, type ).right;
        RexNode v2 = arguments.get( 1 ).getRex( context, type ).right;

        RexNode metricRex = arguments.get( 2 ).getRex( context, type ).right;
        if ( !(metricRex instanceof RexLiteral metricLit) ) {
            throw new GenericRuntimeException( "vector_distance metric must be a string literal" );
        }
        String metric = metricLit.value.asString().value.toUpperCase( Locale.ROOT );

        OperatorName namedOp = switch ( metric ) {
            case "L1" -> OperatorName.L1_DISTANCE;
            case "L2" -> OperatorName.L2_DISTANCE;
            case "COSINE" -> OperatorName.COSINE_DISTANCE;
            case "HAMMING" -> OperatorName.HAMMING_DISTANCE;
            case "JACCARD" -> OperatorName.JACCARD_DISTANCE;
            case "INNER_PRODUCT" -> OperatorName.INNER_PRODUCT_DISTANCE;
            // parameterized version
            case "CHISQUARED", "L2SQUARED" -> OperatorName.DISTANCE;
            default -> throw new GenericRuntimeException( "Unknown distance metric: ", metric );
        };
        Operator operator = OperatorRegistry.get( namedOp );

        if ( namedOp == OperatorName.DISTANCE ) {
            return context.rexBuilder.makeCall( operator, List.of( v1, v2, metricRex ) );
        }
        return context.rexBuilder.makeCall( operator, List.of( v1, v2 ) );
    }


    public ImmutableList<CypherExpression> getArguments() {
        return ImmutableList.copyOf( arguments );
    }


    public OperatorName getOperatorName() {
        return op;
    }


    @Override
    protected Pair<PolyString, RexNode> toRex( CypherContext context, RexType type ) {
        return Pair.of( null, getRexCall( context, type ) );
    }


    public RexNode getRexCall( CypherContext context, RexType type ) {
        if ( distinct ) {
            throw new CypherSyntaxException( "DISTINCT requires an aggregating function." );
        }
        if ( temporalFunction != null ) {
            List<RexNode> operands = new ArrayList<>( arguments.size() + 2 );
            operands.add( context.rexBuilder.makeLiteral( temporalFunction.getName() ) );
            operands.add( context.rexBuilder.makeLiteral( temporalFunction.isRealtime() ? namePos.getLineNum() + ":" + namePos.getColumnNum() : "" ) );
            for ( CypherExpression argument : arguments ) {
                operands.add( argument.getRex( context, type ).right );
            }
            AlgDataType result = context.typeFactory.createPolyType( temporalFunction.returnType() );
            return nullableCall( context, result, operands );
        }
        if ( scalarFunction == CypherScalarFunction.RAND ) {
            RexNode callSite = context.rexBuilder.makeLiteral( namePos.getLineNum() + ":" + namePos.getColumnNum() );
            return context.rexBuilder.makeCall( context.typeFactory.createPolyType( PolyType.DOUBLE ),
                    OperatorRegistry.get( QueryLanguage.from( "cypher" ), op ), List.of( callSite ) );
        }
        if ( scalarFunction != null ) {
            List<RexNode> operands = new ArrayList<>( arguments.size() + 1 );
            operands.add( context.rexBuilder.makeLiteral( scalarFunction.name() ) );
            for ( CypherExpression argument : arguments ) {
                operands.add( argument.getRex( context, type ).right );
            }
            if ( scalarFunction == CypherScalarFunction.HEAD || scalarFunction == CypherScalarFunction.TAIL ) {
                requireListOperand( operands.get( 1 ), scalarFunction.name().toLowerCase( Locale.ROOT ) + "() requires a list." );
            }
            AlgDataType resultType = scalarFunction.resultType( context.typeFactory, operands.subList( 1, operands.size() ).stream().map( RexNode::getType ).toList() );
            return nullableCall( context, resultType, operands );
        }
        switch ( getOperatorName() ) {
            case CYPHER_POINT: {
                // VERY UGLY, but it works for now. This could be improved by using the function MAP_OF_ENTRIES,
                // but I am not sure how to call it.
                CypherLiteral mapExpression = (CypherLiteral) getArguments().get( 0 );
                List<RexNode> arguments = new ArrayList<>();
                mapExpression.getMapValue().forEach( ( key, value ) -> {
                    Pair<PolyString, RexNode> pair = value.getRex( context, RexType.PROJECT );
                    arguments.add( context.rexBuilder.makeLiteral( key ) );
                    arguments.add( pair.right );
                } );
                // Fill with NULL to make sure we have the correct amount of arguments.
                // 3 coordinates + 3 names + srid + crs = up to 8 possible
                while ( arguments.size() < 10 ) {
                    arguments.add( context.rexBuilder.makeNullLiteral( context.typeFactory.createUnknownType() ) );
                }
                return new RexCall(
                        context.geometryType,
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), CYPHER_POINT ),
                        arguments );
            }
            case DISTANCE: {
                return new RexCall(
                        context.typeFactory.createPolyType( PolyType.DOUBLE ),
                        // If the third argument for the point.distance function is the string 'neo4j', then
                        // use the spherical distance approximation of Neo4j. This uses a different earth radius.
                        // Otherwise, the default behavior is to use the same distance measurement as the other
                        // models.
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), arguments.size() == 3
                                && arguments.get( 2 ) instanceof CypherLiteral neo4jFlag
                                && neo4jFlag.getValue().toString().equals( "neo4j" )
                                ? OperatorName.DISTANCE_NEO4J
                                : OperatorName.DISTANCE
                        ),
                        List.of(
                                arguments.get( 0 ).getRex( context, RexType.PROJECT ).getRight(),
                                arguments.get( 1 ).getRex( context, RexType.PROJECT ).getRight()
                        ) );
            }
            case DISTANCE_NEO4J: {
                return new RexCall(
                        context.typeFactory.createPolyType( PolyType.DOUBLE ),
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.DISTANCE_NEO4J ),
                        List.of(
                                arguments.get( 0 ).getRex( context, RexType.PROJECT ).getRight(),
                                arguments.get( 1 ).getRex( context, RexType.PROJECT ).getRight()
                        ) );
            }
            case CYPHER_WITHIN_BBOX:
                return new RexCall(
                        context.booleanType,
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_WITHIN_BBOX ),
                        List.of(
                                arguments.get( 0 ).getRex( context, RexType.PROJECT ).getRight(),
                                // CypherFunctionInvocation.getRex -> throw
                                // Because create function logic is implemented in
                                arguments.get( 1 ).getRex( context, RexType.PROJECT ).getRight(),
                                arguments.get( 2 ).getRex( context, RexType.PROJECT ).getRight()
                        ) );
            case CYPHER_WITHIN_GEOMETRY:
                return new RexCall(
                        context.booleanType,
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_WITHIN_GEOMETRY ),
                        List.of(
                                arguments.get( 0 ).getRex( context, RexType.PROJECT ).getRight(),
                                // CypherFunctionInvocation.getRex -> throw
                                // Because create function logic is implemented in
                                arguments.get( 1 ).getRex( context, RexType.PROJECT ).getRight()
                        ) );
            case EXISTS:
                return exists( context, type );
            case VECTOR_DISTANCE:
                return getVectorDistanceRex( context, type );
            case COALESCE, CYPHER_COALESCE: {
                if ( arguments.isEmpty() ) {
                    throw new CypherSyntaxException( "coalesce() requires at least one argument." );
                }
                List<RexNode> operands = arguments.stream().map( argument -> argument.getRex( context, type ).right ).toList();
                AlgDataType result = PolyTypeUtil.commonDynamicType( context.typeFactory, operands.stream().map( RexNode::getType ).toList() );
                return context.rexBuilder.makeCall( result, OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_COALESCE ), operands );
            }
            case LAST, CYPHER_LAST: {
                if ( arguments.size() != 1 ) {
                    throw new CypherSyntaxException( "last() requires exactly one argument." );
                }
                RexNode list = arguments.get( 0 ).getRex( context, type ).right;
                requireListOperand( list, "last() requires a list." );
                AlgDataType component = list.getType().getComponentType();
                AlgDataType result = component == null ? context.typeFactory.createPolyType( PolyType.ANY ) : component;
                return context.rexBuilder.makeCall( context.typeFactory.createTypeWithNullability( result, true ),
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_LAST ), List.of( list ) );
            }
            case CYPHER_NODES, CYPHER_RELATIONSHIPS, CYPHER_LENGTH:
                return getPathFunctionRex( context, type );
            case CYPHER_SIZE:
                return unaryFunction( context, type, List.of( PolyType.ARRAY, PolyType.CHAR, PolyType.VARCHAR, PolyType.TEXT ),
                        context.typeFactory.createPolyType( PolyType.BIGINT ), "a list or string" );
            case CYPHER_RANGE:
                return rangeFunction( context, type );
            case CYPHER_TYPE:
                return unaryFunction( context, type, List.of( PolyType.EDGE ),
                        context.typeFactory.createPolyType( PolyType.VARCHAR ), "a relationship" );
            case CYPHER_ID:
                return unaryFunction( context, type, List.of( PolyType.NODE, PolyType.EDGE ),
                        context.typeFactory.createPolyType( PolyType.VARCHAR ), "a node or relationship" );
            case CYPHER_LABELS:
                return unaryFunction( context, type, List.of( PolyType.NODE ),
                        context.typeFactory.createArrayType( context.typeFactory.createPolyType( PolyType.VARCHAR ), -1, 1 ), "a node" );
            case CYPHER_KEYS, CYPHER_PROPERTIES:
                AlgDataType stringType = context.typeFactory.createPolyType( PolyType.VARCHAR );
                AlgDataType result = op == OperatorName.CYPHER_KEYS
                        ? context.typeFactory.createArrayType( stringType, -1, 1 )
                        : context.typeFactory.createMapType( stringType, context.typeFactory.createPolyType( PolyType.ANY ) );
                return unaryFunction( context, type, List.of( PolyType.NODE, PolyType.EDGE, PolyType.MAP, PolyType.DOCUMENT ),
                        result, "a node, relationship, or map" );
            case CYPHER_START_NODE, CYPHER_END_NODE:
                return relationshipEndpoint( context, type );
            default:
                throw new NotImplementedException( "Cypher Function to alg conversion missing: " + getOperatorName() );
        }
    }


    @Override
    public PolyValue getComparable() {
        if ( scalarFunction == null ) {
            return super.getComparable();
        }
        if ( distinct ) {
            throw new CypherSyntaxException( "DISTINCT requires an aggregating function." );
        }
        return scalarFunction.apply( arguments.stream().map( CypherExpression::getComparable ).toArray( PolyValue[]::new ) );
    }


    private RexNode exists( CypherContext context, RexType type ) {
        if ( !namespace.isEmpty() || arguments.size() != 1 ) {
            throw new CypherSyntaxException( "exists() requires exactly one property or path pattern argument." );
        }
        CypherExpression argument = arguments.get( 0 );
        if ( argument instanceof CypherProperty property ) {
            RexNode owner = property.getSubject().getRex( context, type ).right;
            switch ( owner.getType().getPolyType() ) {
                case MAP, NODE, EDGE, ANY, NULL -> {
                    return context.rexBuilder.makeCall( context.booleanType,
                            OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_PROPERTY_EXISTS ),
                            List.of( owner, context.rexBuilder.makeLiteral( property.getPropertyKey() ) ) );
                }
                default -> throw new CypherSyntaxException( "exists() requires a property of a map, node, or relationship." );
            }
        }
        if ( argument.getType() == ExpressionType.PATTERN && argument.getPattern() instanceof CypherEveryPathPattern pattern ) {
            return CypherPatternExpression.exists( pattern, context, type );
        }
        throw new CypherSyntaxException( "exists() requires a property or path pattern argument." );
    }


    private RexNode getPathFunctionRex( CypherContext context, RexType type ) {
        String function = op.name().substring( "CYPHER_".length() ).toLowerCase( Locale.ROOT );
        if ( arguments.size() != 1 ) {
            throw new CypherSyntaxException( function + "() requires exactly one argument." );
        }
        RexNode path = arguments.get( 0 ).getRex( context, type ).right;
        PolyType pathType = path.getType().getPolyType();
        if ( pathType != PolyType.PATH && pathType != PolyType.NULL && pathType != PolyType.ANY ) {
            throw new CypherSyntaxException( "InvalidArgumentType: " + function + "() requires a path." );
        }
        AlgDataType result = op == OperatorName.CYPHER_LENGTH
                ? context.typeFactory.createPolyType( PolyType.BIGINT )
                : context.typeFactory.createArrayType( context.typeFactory.createPolyType(
                        op == OperatorName.CYPHER_NODES ? PolyType.NODE : PolyType.EDGE ), -1, 1 );
        return context.rexBuilder.makeCall( context.typeFactory.createTypeWithNullability( result, true ),
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), op ), List.of( path ) );
    }


    private RexNode unaryFunction( CypherContext context, RexType type, List<PolyType> inputTypes, AlgDataType result, String expected ) {
        String function = op.name().substring( "CYPHER_".length() ).toLowerCase( Locale.ROOT ) + "()";
        if ( arguments.size() != 1 ) {
            throw new CypherSyntaxException( function + " requires exactly one argument." );
        }
        RexNode operand = arguments.get( 0 ).getRex( context, type ).right;
        requireOperand( operand, inputTypes, function + " requires " + expected + "." );
        return nullableCall( context, result, List.of( operand ) );
    }


    private RexNode relationshipEndpoint( CypherContext context, RexType type ) {
        String function = op == OperatorName.CYPHER_START_NODE ? "startNode()" : "endNode()";
        if ( arguments.size() != 1 ) {
            throw new CypherSyntaxException( function + " requires exactly one argument." );
        }
        RexNode relationship = arguments.get( 0 ).getRex( context, type ).right;
        requireOperand( relationship, List.of( PolyType.EDGE ), function + " requires a relationship." );
        return nullableCall( context, context.nodeType, List.of( context.graphValue(), relationship ) );
    }


    private RexNode rangeFunction( CypherContext context, RexType type ) {
        if ( arguments.size() < 2 || arguments.size() > 3 ) {
            throw new CypherSyntaxException( "range() requires two or three arguments." );
        }
        List<RexNode> operands = new ArrayList<>( 3 );
        for ( CypherExpression argument : arguments ) {
            operands.add( argument.getRex( context, type ).right );
        }
        if ( operands.size() == 2 ) {
            operands.add( context.rexBuilder.makeLiteral( PolyInteger.of( 1 ) ) );
        }
        return nullableCall( context, context.typeFactory.createArrayType( context.typeFactory.createPolyType( PolyType.BIGINT ), -1, 1 ), operands );
    }


    private void requireOperand( RexNode operand, List<PolyType> allowed, String message ) {
        PolyType type = operand.getType().getPolyType();
        if ( type != PolyType.ANY && type != PolyType.NULL && !allowed.contains( type ) ) {
            throw new CypherSyntaxException( "InvalidArgumentType: " + message );
        }
    }


    private RexNode nullableCall( CypherContext context, AlgDataType result, List<RexNode> operands ) {
        return context.rexBuilder.makeCall( context.typeFactory.createTypeWithNullability( result, true ),
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), op ), operands );
    }

}
