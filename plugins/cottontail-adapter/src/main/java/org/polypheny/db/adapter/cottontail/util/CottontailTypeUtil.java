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

package org.polypheny.db.adapter.cottontail.util;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.fun.SqlArrayValueConstructor;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.util.BuiltInMethod;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.BoolVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Date;
import org.vitrivr.cottontail.grpc.CottontailGrpc.DoubleVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.EntityName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.FloatVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.From;
import org.vitrivr.cottontail.grpc.CottontailGrpc.IntVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Literal;
import org.vitrivr.cottontail.grpc.CottontailGrpc.LongVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Projection;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Projection.ProjectionElement;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Scan;
import org.vitrivr.cottontail.grpc.CottontailGrpc.SchemaName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Type;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Vector;


@Slf4j
public class CottontailTypeUtil {

    public static final Method COTTONTAIL_SIMPLE_CONSTANT_TO_DATA_METHOD = Types.lookupMethod(
            CottontailTypeUtil.class,
            "toData",
            PolyValue.class, PolyType.class, PolyType.class );

    public static final Method COTTONTAIL_KNN_BUILDER_METHOD = Types.lookupMethod(
            Linq4JFixer.class,
            "generateKnn",
            String.class, Vector.class, PolyValue.class, String.class );


    /**
     * Maps the given map of attributes and (optional) aliases to a {@link Projection.Builder}.
     *
     * @param map Map of projection clauses.
     * @param proj The {@link Projection.Builder} to append to.
     */
    public static void mapToProjection( Map<String, Object> map, Projection.Builder proj ) {
        for ( Entry<String, Object> p : map.entrySet() ) {
            final ProjectionElement.Builder e = proj.addElementsBuilder();
            e.setColumn( ColumnName.newBuilder().setName( p.getKey() ) );
            if ( p.getValue() != null ) {
                e.setAlias( ColumnName.newBuilder().setName( (String) p.getValue() ) );
            }
            proj.addElements( e );
        }
    }


    public static CottontailGrpc.Type getPhysicalTypeRepresentation( AlgDataType algDataType ) {
        PolyType type = algDataType.getPolyType();
        PolyType componentType = algDataType.getComponentType().getPolyType();

        if ( componentType == null ) {
            return getPhysicalTypeRepresentation( type, componentType, 0 );
        } else {
            // TODO js(ct): Verify this call in regards to dimension
            return getPhysicalTypeRepresentation( componentType, type, 0 );
        }
    }


    public static CottontailGrpc.Type getPhysicalTypeRepresentation( PolyType logicalType, PolyType collectionType, int dimension ) {
        if ( collectionType == PolyType.ARRAY ) {
            if ( dimension != 1 ) {
                // Dimension isn't 1, thus we have to serialise the array.
                return Type.STRING;
            }

            return switch ( logicalType ) {
                case TINYINT, SMALLINT, INTEGER -> Type.INT_VEC;
                case BIGINT -> Type.LONG_VEC;
                case FLOAT, REAL -> Type.FLOAT_VEC;
                case DOUBLE, DECIMAL -> Type.DOUBLE_VEC;
                case BOOLEAN -> Type.BOOL_VEC;
                default -> Type.STRING;
            };
        } else if ( collectionType == null ) {

            switch ( logicalType ) {
                // Natively supported types
                case BOOLEAN:
                    return Type.BOOLEAN;
                case INTEGER:
                    return Type.INTEGER;
                case BIGINT:
                    return Type.LONG;
                case DOUBLE:
                    return Type.DOUBLE;
                case REAL:
                case FLOAT:
                    return Type.FLOAT;
                case VARCHAR:
                case CHAR:
                case JSON:
                    return Type.STRING;
                // Types that require special treatment.
                case TINYINT:
                case SMALLINT:
                case DATE:
                case TIME:
                    return Type.INTEGER;
                case TIMESTAMP:
                    return Type.DATE;
                case DECIMAL:
                case VARBINARY:
                case BINARY:
                case TEXT:
                    return Type.STRING;
                case FILE:
                case IMAGE:
                case AUDIO:
                case VIDEO:
                    return Type.STRING;
            }
        }

        throw new GenericRuntimeException( "Type " + logicalType + " is not supported by the Cottontail DB adapter." );
    }


    public static Expression rexDynamicParamToDataExpression( RexDynamicParam dynamicParam, ParameterExpression dynamicParameterMap_, PolyType actualType ) {
        return Expressions.call(
                COTTONTAIL_SIMPLE_CONSTANT_TO_DATA_METHOD,
                Expressions.convert_( Expressions.call(
                        dynamicParameterMap_,
                        BuiltInMethod.MAP_GET.method,
                        Expressions.constant( dynamicParam.getIndex() ) ), PolyValue.class ),
                Expressions.constant( actualType ),
                Expressions.constant( dynamicParam.getType() != null ?
                        dynamicParam.getType().getComponentType() != null ?
                                dynamicParam.getType().getComponentType().getPolyType()
                                : null
                        : null ) );
    }


    public static Expression rexLiteralToDataExpression( RexLiteral rexLiteral, PolyType actualType ) {
        Expression constantExpression;
        if ( rexLiteral.isNull() ) {
            constantExpression = Expressions.constant( null );
        } else {
            constantExpression = switch ( actualType ) {
                case BOOLEAN, INTEGER, BIGINT, DOUBLE, REAL, FLOAT, VARCHAR, CHAR, TEXT, JSON, DATE, TIME, TIMESTAMP, TINYINT, SMALLINT, DECIMAL, VARBINARY, BINARY, FILE, AUDIO, IMAGE, VIDEO -> rexLiteral.getValue().asExpression();
                default -> throw new GenericRuntimeException( "Type " + rexLiteral.type + " is not supported by the cottontail adapter." );
            };
        }

        return Expressions.call( COTTONTAIL_SIMPLE_CONSTANT_TO_DATA_METHOD, constantExpression, Expressions.constant( actualType ), Expressions.constant( null ) );
    }


    public static Expression rexArrayConstructorToExpression( RexCall rexCall, PolyType innerType ) {
        Expression constantExpression = arrayListToExpression( rexCall.getOperands(), innerType );
        return Expressions.call( COTTONTAIL_SIMPLE_CONSTANT_TO_DATA_METHOD, constantExpression, Expressions.constant( innerType ), Expressions.constant( null ) );
    }


    public static CottontailGrpc.Literal toData( PolyValue value, PolyType actualType, PolyType parameterComponentType ) {
        final CottontailGrpc.Literal.Builder builder = Literal.newBuilder();
        if ( value == null || value.isNull() ) {
            return builder.build();
        }

        log.trace( "Attempting to data value: {}, type: {}", value.getClass().getCanonicalName(), actualType );

        if ( value.isList() ) {
            log.trace( "Attempting to convert an array to data." );
            // TODO js(ct): add list.size() == 0 handling
            // Check whether the decimal array should be converted to a double array (i.e. when we are not comparing to a column of
            // type decimal (which is encoded as string since cottontail does not support the data type decimal))
            if ( parameterComponentType == PolyType.DECIMAL && actualType != PolyType.DECIMAL && actualType != PolyType.ARRAY && value.asList().get( 0 ).isBigDecimal() ) {
                List<PolyValue> numbers = new ArrayList<>( value.asList().size() );
                ((List<PolyValue>) value.asList()).forEach( e -> numbers.add( PolyDouble.of( e.asNumber().doubleValue() ) ) );
                value = PolyList.of( numbers );
            }
            final Vector vector = toVectorData( value.asList(), parameterComponentType );
            if ( vector != null ) {
                return builder.setVectorData( vector ).build();
            } else {
                /* TODO (RG): BigDecimals are currently handled by this branch, which excludes them from being usable for native NNS. */
                return builder.setStringData( value.toTypedJson() ).build();
            }
        }

        switch ( actualType ) {
            case BOOLEAN: {
                if ( value.isBoolean() ) {
                    return builder.setBooleanData( value.asBoolean().value ).build();
                }
                break;
            }
            case BIGINT: {
                if ( value.isNumber() ) {
                    return builder.setLongData( value.asNumber().longValue() ).build();
                }
                break;
            }
            case INTEGER:
            case TINYINT:
            case SMALLINT: {
                if ( value.isNumber() ) {
                    return builder.setIntData( value.asNumber().intValue() ).build();
                }
                break;
            }
            case DOUBLE: {
                if ( value.isNumber() ) {
                    return builder.setDoubleData( value.asNumber().doubleValue() ).build();
                }
                break;
            }
            case FLOAT:
            case REAL: {
                if ( value.isNumber() ) {
                    return builder.setFloatData( value.asNumber().floatValue() ).build();
                }
                break;
            }
            case JSON:
            case TEXT:
            case VARCHAR: {
                if ( value.isString() ) {
                    return builder.setStringData( value.asString().value ).build();
                }
                break;
            }
            case DECIMAL: {
                if ( value.isNumber() ) {
                    return builder.setStringData( value.asNumber().BigDecimalValue().toString() ).build();
                }
                break;
            }
            case TIME: {
                if ( value.isTemporal() ) {
                    return builder.setIntData( Math.toIntExact( value.asTemporal().getMillisOfDay() ) ).build();
                }
                break;
            }
            case DATE: {
                if ( value.isTemporal() ) {
                    return builder.setIntData( Math.toIntExact( value.asTemporal().getDaysSinceEpoch() ) ).build();
                }
                break;
            }
            case TIMESTAMP: {
                if ( value.isTemporal() ) {
                    return builder.setDateData( Date.newBuilder().setUtcTimestamp( value.asTemporal().getMillisSinceEpoch() ) ).build();
                }
                break;
            }
            case FILE:
            case IMAGE:
            case AUDIO:
            case VIDEO:
                if ( value.isBlob() ) {
                    return builder.setStringData( value.asBlob().as64String() ).build();
                }
        }

        log.error( "Conversion not possible! value: {}, type: {}", value.getClass().getCanonicalName(), actualType );
        throw new GenericRuntimeException( "Cottontail data type error: Type not handled." );
    }


    /**
     * Converts the provided vectorObject to a {@link Vector} respecting the provided {@link PolyType}
     * of the destination format. Used for NNS.
     *
     * @param vectorObject The vectorObject that should be converted.
     * @param dstElementType The {@link PolyType} of the destination element.
     * @return {@link Vector}
     */
    public static Vector toVectorCallData( PolyValue vectorObject, PolyType dstElementType ) {
        if ( vectorObject == null || vectorObject.isNull() ) {
            return Vector.newBuilder().build();
        }
        if ( !vectorObject.isList() ) {
            throw new GenericRuntimeException( "VectorObject is not a list." );
        }

        final Vector.Builder builder = Vector.newBuilder();
        return switch ( dstElementType ) {
            case TINYINT, SMALLINT, INTEGER -> {
                for ( PolyValue o : vectorObject.asList() ) {
                    builder.getIntVectorBuilder().addVector( o.asNumber().intValue() );
                }
                yield builder.build();
            }
            case BIGINT -> {
                for ( PolyValue o : vectorObject.asList() ) {
                    builder.getLongVectorBuilder().addVector( o.asNumber().longValue() );
                }
                yield builder.build();
            }
            case DECIMAL, DOUBLE -> {
                for ( PolyValue o : vectorObject.asList() ) {
                    builder.getDoubleVectorBuilder().addVector( o.asNumber().doubleValue() );
                }
                yield builder.build();
            }
            case FLOAT, REAL -> {
                for ( PolyValue o : vectorObject.asList() ) {
                    builder.getFloatVectorBuilder().addVector( o.asNumber().floatValue() );
                }
                yield builder.build();
            }
            default -> throw new GenericRuntimeException( "Unsupported type: " + dstElementType.getName() );
        };
    }


    /**
     * Converts list of primitive data types (i.e. {@link Double}, {@link Float}, {@link Long}, {@link Integer} or
     * {@link Boolean}) to a {@link Vector} usable by Cottontail DB.
     *
     * @param vectorObject List of {@link Object}s that need to be converted.
     * @return Converted object or null if conversion is not possible.
     */
    public static Vector toVectorData( List<PolyValue> vectorObject, PolyType parameterComponentType ) {
        final Vector.Builder vectorBuilder = Vector.newBuilder();
        // TODO js(ct): add list.size() == 0 handling

        return switch ( parameterComponentType ) {
            case INTEGER, SMALLINT, TINYINT -> vectorBuilder.setIntVector( IntVector.newBuilder().addAllVector(
                    vectorObject.stream().map( PolyValue::asNumber ).map( PolyNumber::intValue ).toList() ).build() ).build();
            case DOUBLE, DECIMAL -> vectorBuilder.setDoubleVector( DoubleVector.newBuilder().addAllVector(
                    vectorObject.stream().map( PolyValue::asNumber ).map( PolyNumber::doubleValue ).toList() ).build() ).build();
            case BIGINT -> vectorBuilder.setLongVector( LongVector.newBuilder().addAllVector(
                    vectorObject.stream().map( PolyValue::asNumber ).map( PolyNumber::longValue ).toList() ).build() ).build();
            case FLOAT, REAL -> vectorBuilder.setFloatVector( FloatVector.newBuilder().addAllVector(
                    vectorObject.stream().map( PolyValue::asNumber ).map( PolyNumber::floatValue ).toList() ).build() ).build();
            case BOOLEAN -> vectorBuilder.setBoolVector( BoolVector.newBuilder().addAllVector(
                    vectorObject.stream().map( PolyValue::asBoolean ).map( PolyBoolean::getValue ).toList() ).build() ).build();
            default -> null;
        };
    }


    private static Expression arrayListToExpression( List<RexNode> operands, PolyType innerType ) {
        List<PolyValue> list = arrayCallToList( operands, innerType ).asList();

        return switch ( innerType ) {
            /*case DECIMAL -> {
                List<PolyValue> stringEncoded = convertBigDecimalArray( list );
                yield Expressions.call(
                        Types.lookupMethod( Linq4JFixer.class, "fixBigDecimalArray", List.class ),
                        Expressions.constant( stringEncoded ) );
            }*/
            default -> Expressions.constant( list );
        };
    }


    private static List<PolyValue> convertBigDecimalArray( List<PolyValue> bigDecimalArray ) {
        List<PolyValue> fixedList = new ArrayList<>( bigDecimalArray.size() );
        for ( PolyValue o : bigDecimalArray ) {
            /*if ( o instanceof BigDecimal ) {
                fixedList.add( o.toString() );
            } else {
                fixedList.add( convertBigDecimalArray( (List) o ) );
            }*/
            fixedList.add( o );
        }
        return fixedList;
    }


    private static PolyValue arrayCallToList( List<RexNode> operands, PolyType innerType ) {
        List<PolyValue> list = new ArrayList<>( operands.size() );
        for ( RexNode node : operands ) {
            if ( node instanceof RexLiteral ) {
                list.add( rexLiteralToJavaClass( (RexLiteral) node, innerType ) );
            } else if ( node instanceof RexCall ) {
                list.add( arrayCallToList( ((RexCall) node).operands, innerType ) );
            } else {
                throw new GenericRuntimeException( "Invalid array." );
            }
        }

        return PolyList.copyOf( list );
    }


    private static PolyValue rexLiteralToJavaClass( RexLiteral rexLiteral, PolyType actualType ) {
        return switch ( actualType ) {
            case BOOLEAN, DOUBLE, BIGINT, REAL, FLOAT, INTEGER, VARCHAR, CHAR, JSON, TIMESTAMP, DATE, TIME, DECIMAL, VARBINARY, BINARY, TINYINT, SMALLINT -> rexLiteral.getValue();
            default -> throw new GenericRuntimeException( "Type " + actualType + " is not supported by the cottontail adapter." );
        };
    }


    public static From fromFromTableAndSchema( String table, String schema ) {
        return From.newBuilder().setScan( Scan.newBuilder().setEntity( EntityName.newBuilder().setName( table ).setSchema( SchemaName.newBuilder().setName( schema ) ) ) ).build();
    }


    /**
     * Converts the given {@link RexCall} to an {@link Expression} for the distance function invocation.
     *
     * @param knnCall {@link RexCall} to convert.
     * @param physicalColumnNames List of physical column names
     * @param alias The alias used to name the resulting field.
     * @return {@link Expression}
     */
    public static Expression knnCallToFunctionExpression( RexCall knnCall, List<String> physicalColumnNames, String alias ) {
        BlockBuilder inner = new BlockBuilder();
        ParameterExpression dynamicParameterMap_ = Expressions.parameter( Modifier.FINAL, Map.class, inner.newName( "dynamicParameters" ) );
        final Expression probingArgument = knnCallTargetColumn( knnCall.getOperands().get( 0 ), physicalColumnNames, dynamicParameterMap_ );
        final Expression queryArgument = knnCallVector( knnCall.getOperands().get( 1 ), dynamicParameterMap_, knnCall.getOperands().get( 0 ).getType().getComponentType().getPolyType() );
        final Expression distance = Expressions.convert_( knnCallDistance( knnCall.getOperands().get( 2 ), dynamicParameterMap_ ), PolyValue.class );
        return Expressions.lambda( Expressions.block( Expressions.return_( null, Expressions.call( COTTONTAIL_KNN_BUILDER_METHOD, probingArgument, queryArgument, distance, Expressions.constant( alias ) ) ) ), dynamicParameterMap_ );
    }


    /**
     * Converts the given {@link RexNode} to an {@link Expression} for the target column for a distance function invocation.
     *
     * @param node {@link RexNode} to convert
     * @param physicalColumnNames List of physical column names
     * @return {@link Expression}
     */
    private static Expression knnCallTargetColumn( RexNode node, List<String> physicalColumnNames, ParameterExpression dynamicParamMap ) {
        if ( node instanceof RexIndexRef inputRef ) {
            return Expressions.constant( physicalColumnNames.get( inputRef.getIndex() ) );
        } else if ( node instanceof RexDynamicParam dynamicParam ) {
            return Expressions.call( dynamicParamMap, BuiltInMethod.MAP_GET.method, Expressions.constant( dynamicParam.getIndex() ) );
        }

        throw new GenericRuntimeException( "First argument is neither an input ref nor a dynamic parameter" );
    }


    /**
     * Converts the given {@link RexNode} to an {@link Expression} for the query vector for a distance function invocation.
     *
     * @param node {@link RexNode} to convert
     * @param actualType The {@link PolyType} of the array elements. Required for proper conversion!
     * @return {@link Expression}
     */
    private static Expression knnCallVector( RexNode node, ParameterExpression dynamicParamMap, PolyType actualType ) {
        if ( (node instanceof RexCall) && (((RexCall) node).getOperator() instanceof SqlArrayValueConstructor) ) {
            final Expression arrayList = arrayListToExpression( ((RexCall) node).getOperands(), actualType );
            return Expressions.call( CottontailTypeUtil.class, "toVectorCallData", arrayList, Expressions.constant( actualType ) );
        } else if ( node instanceof RexDynamicParam dynamicParam ) {
            final Expression listExpression = Expressions.convert_( Expressions.call( dynamicParamMap, BuiltInMethod.MAP_GET.method, Expressions.constant( dynamicParam.getIndex() ) ), PolyValue.class );
            return Expressions.call( CottontailTypeUtil.class, "toVectorCallData", listExpression, Expressions.constant( actualType ) );
        }

        throw new GenericRuntimeException( "Argument is neither an array call nor a dynamic parameter" );
    }


    /**
     * Converts the given {@link RexNode} to an {@link Expression} for the name of the distance function.
     *
     * @param node {@link RexNode} to convert
     * @return {@link Expression}
     */
    private static Expression knnCallDistance( RexNode node, ParameterExpression dynamicParamMap ) {
        if ( node instanceof RexLiteral ) {
            return Expressions.constant( ((RexLiteral) node).getValue() );
        } else if ( node instanceof RexDynamicParam dynamicParam ) {
            return Expressions.call( dynamicParamMap, BuiltInMethod.MAP_GET.method,
                    Expressions.constant( dynamicParam.getIndex() ) );
        }

        throw new GenericRuntimeException( "Argument is neither an array call nor a dynamic parameter" );
    }

}