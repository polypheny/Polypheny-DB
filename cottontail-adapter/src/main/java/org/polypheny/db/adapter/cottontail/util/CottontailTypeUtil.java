/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.sql.SqlLiteral;
import org.polypheny.db.sql.fun.SqlArrayValueConstructor;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.BoolVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Data;
import org.vitrivr.cottontail.grpc.CottontailGrpc.DoubleVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Entity;
import org.vitrivr.cottontail.grpc.CottontailGrpc.FloatVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.From;
import org.vitrivr.cottontail.grpc.CottontailGrpc.IntVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.LongVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Null;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Schema;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Type;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Vector;


public class CottontailTypeUtil {


    public static final Method COTTONTAIL_SIMPLE_CONSTANT_TO_DATA_METHOD = Types.lookupMethod(
            CottontailTypeUtil.class,
            "toData",
            Object.class );

    public static final Method COTTONTAIL_SIMPLE_LIST_TO_VECTOR_METHOD = Types.lookupMethod(
            CottontailTypeUtil.class,
            "toVectorData",
            Object.class );


    public static final Method COTTONTAIL_KNN_BUILDER_METHOD = Types.lookupMethod(
            Linq4JFixer.class,
            "generateKnn",
            Object.class, Object.class, Object.class, Object.class, Object.class );
//            String.class, Integer.class, String.class, Object.class, Object.class );


    public static CottontailGrpc.Type getPhysicalTypeRepresentation( RelDataType relDataType ) {

        PolyType type = relDataType.getPolyType();
        PolyType componentType = relDataType.getComponentType().getPolyType();

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

            switch ( logicalType ) {
                case INTEGER:
                    return Type.INT_VEC;
                case DOUBLE:
                    return Type.DOUBLE_VEC;
                case BIGINT:
                    return Type.LONG_VEC;
                case FLOAT:
                case REAL:
                    return Type.FLOAT_VEC;
                case BOOLEAN:
                    return Type.BOOL_VEC;
                default:
                    return Type.STRING;
            }
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
                    return Type.STRING;
                // Types that require special treatment.
                case TINYINT:
                case SMALLINT:
                    return Type.INTEGER;
                case DATE:
                case TIME:
                    return Type.INTEGER;
                case TIMESTAMP:
                    return Type.LONG;
                case DECIMAL:
                case VARBINARY:
                case BINARY:
                    return Type.STRING;
            }
        }

        throw new RuntimeException( "Type " + logicalType + " is not supported by the cottontail adapter." );
    }


    public static Expression rexDynamicParamToDataExpression( RexDynamicParam dynamicParam, ParameterExpression dynamicParameterMap_ ) {
//        CompoundBooleanPredicate.newBuilder().set
        return Expressions.call( COTTONTAIL_SIMPLE_CONSTANT_TO_DATA_METHOD,
                Expressions.call( dynamicParameterMap_, BuiltInMethod.MAP_GET.method,
                        Expressions.constant( dynamicParam.getIndex() ) ) );
    }


    public static Expression rexLiteralToDataExpression( RexLiteral rexLiteral, PolyType actualType ) {
        ConstantExpression constantExpression;
        switch ( actualType ) {
            case BOOLEAN:
                constantExpression = Expressions.constant( rexLiteral.getValueAs( Boolean.class ) );
                break;
            case INTEGER:
                constantExpression = Expressions.constant( rexLiteral.getValueAs( Integer.class ) );
                break;
            case BIGINT:
                constantExpression = Expressions.constant( rexLiteral.getValueAs( Long.class ) );
                break;
            case DOUBLE:
                constantExpression = Expressions.constant( rexLiteral.getValueAs( Double.class ) );
                break;
            case REAL:
            case FLOAT:
                constantExpression = Expressions.constant( rexLiteral.getValueAs( Float.class ) );
                break;
            case VARCHAR:
            case CHAR:
                constantExpression = Expressions.constant( rexLiteral.getValueAs( String.class ) );
                break;
            case TIMESTAMP:
                constantExpression = Expressions.constant( rexLiteral.getValueAs( Long.class ) );
                break;
            case DATE:
            case TIME:
                constantExpression = Expressions.constant( rexLiteral.getValueAs( Integer.class ) );
                break;
            case TINYINT:
                constantExpression = Expressions.constant( rexLiteral.getValueAs( Byte.class ) );
                break;
            case SMALLINT:
                constantExpression = Expressions.constant( rexLiteral.getValueAs( Short.class ) );
                break;
            case DECIMAL:
//                constantExpression = Expressions.constant( org.polypheny.db.adapter.cottontail.util.CottontailSerialisation.GSON.toJson( rexLiteral.getValueAs( BigDecimal.class ) ) );
                BigDecimal bigDecimal = rexLiteral.getValueAs( BigDecimal.class );
                constantExpression = Expressions.constant( (bigDecimal != null) ? bigDecimal.toString() : null );
                break;
            case VARBINARY:
            case BINARY:
                constantExpression = Expressions.constant( rexLiteral.getValueAs( ByteString.class ).toBase64String() );
                break;
            default:
                throw new RuntimeException( "Type " + rexLiteral.getTypeName() + " is not supported by the cottontail adapter." );
        }

        return Expressions.call( COTTONTAIL_SIMPLE_CONSTANT_TO_DATA_METHOD, constantExpression );
    }


    public static Expression rexArrayConstructorToExpression( RexCall rexCall, PolyType innerType ) {
        Expression constantExpression;
//        List<Object> objectList = arrayCallToList( rexCall.getOperands(), innerType );

//        constantExpression = Expressions.constant( objectList );
        constantExpression = arrayListToExpression( rexCall.getOperands(), innerType );

        return Expressions.call( COTTONTAIL_SIMPLE_CONSTANT_TO_DATA_METHOD, constantExpression );
    }


    public static CottontailGrpc.Data toData( Object value ) {
        CottontailGrpc.Data.Builder builder = Data.newBuilder();
        if ( value == null ) {
            return builder.setNullData( Null.newBuilder().build() ).build();
        }

        if ( value instanceof Boolean ) {
            return builder.setBooleanData( (Boolean) value ).build();
        } else if ( value instanceof Integer ) {
            return builder.setIntData( (Integer) value ).build();
        } else if ( value instanceof Long ) {
            return builder.setLongData( (Long) value ).build();
        } else if ( value instanceof Byte ) {
            return builder.setIntData( ((Byte) value).intValue() ).build();
        } else if ( value instanceof Short ) {
            return builder.setIntData( ((Short) value).intValue() ).build();
        } else if ( value instanceof Double ) {
            return builder.setDoubleData( (Double) value ).build();
        } else if ( value instanceof Float ) {
            return builder.setFloatData( (Float) value ).build();
        } else if ( value instanceof String ) {
            return builder.setStringData( (String) value ).build();
        } else if ( value instanceof BigDecimal ) {
            return builder.setStringData( value.toString() ).build();
        } else if ( value instanceof TimeString ) {
            return builder.setIntData( ((TimeString) value).getMillisOfDay() ).build();
        } else if ( value instanceof java.sql.Time ) {
            java.sql.Time time = (java.sql.Time) value;
            TimeString timeString = new TimeString( time.toString() );
            return builder.setIntData( timeString.getMillisOfDay() ).build();
        } else if ( value instanceof DateString ) {
            return builder.setIntData( ((DateString) value).getDaysSinceEpoch() ).build();
        } else if ( value instanceof java.sql.Date ) {
            DateString dateString = new DateString( value.toString() );
            return builder.setIntData( dateString.getDaysSinceEpoch() ).build();
        } else if ( value instanceof TimestampString ) {
            return builder.setLongData( ((TimestampString) value).getMillisSinceEpoch() ).build();
        } else if ( value instanceof java.sql.Timestamp ) {
            String timeStampString = value.toString();
            if ( timeStampString.endsWith( ".0" ) ) {
                timeStampString = timeStampString.substring( 0, timeStampString.length() - 2 );
            }
            TimestampString tsString = new TimestampString( timeStampString );
            return builder.setLongData( tsString.getMillisSinceEpoch() ).build();
        } else if ( value instanceof Calendar ) {
            TimestampString timestampString = TimestampString.fromCalendarFields( (Calendar) value );
            return builder.setLongData( timestampString.getMillisSinceEpoch() ).build();
        } else if ( value instanceof List ) {
            Vector.Builder vectorBuilder = Vector.newBuilder();
            // TODO js(ct): add list.size() == 0 handling
            Object firstItem = ((List) value).get( 0 );
            Vector vector = toVectorData( value );
            /*if ( firstItem instanceof Integer ) {
                return builder.setVectorData(
                        vectorBuilder.setIntVector(
                                IntVector.newBuilder().addAllVector( (List<Integer>) value ).build() ) ).build();
            } else if ( firstItem instanceof Double ) {
                return builder.setVectorData(
                        vectorBuilder.setDoubleVector(
                                DoubleVector.newBuilder().addAllVector( (List<Double>) value ).build() ) ).build();
            } else if ( firstItem instanceof Long ) {
                return builder.setVectorData(
                        vectorBuilder.setLongVector(
                                LongVector.newBuilder().addAllVector( (List<Long>) value ).build() ) ).build();
            } else if ( firstItem instanceof Float ) {
                return builder.setVectorData(
                        vectorBuilder.setFloatVector(
                                FloatVector.newBuilder().addAllVector( (List<Float>) value ).build() ) ).build();
            } else if ( firstItem instanceof Boolean ) {
                return builder.setVectorData(
                        vectorBuilder.setBoolVector(
                                BoolVector.newBuilder().addAllVector( (List<Boolean>) value ).build() ) ).build();*/
            if ( vector != null ) {
                return builder.setVectorData( vector ).build();
            } else {
                return builder.setStringData( org.polypheny.db.adapter.cottontail.util.CottontailSerialisation.GSON.toJson( (List<Object>) value ) ).build();
            }
        } else {
            throw new RuntimeException( "Cottontail data type error: Type not handled." );
        }
    }


    public static Vector toVectorData( Object vectorObject ) {
        Vector.Builder vectorBuilder = Vector.newBuilder();
        // TODO js(ct): add list.size() == 0 handling
        Object firstItem = ((List) vectorObject).get( 0 );
        if ( firstItem instanceof Integer ) {
            return vectorBuilder.setIntVector(
                    IntVector.newBuilder().addAllVector( (List<Integer>) vectorObject ).build() ).build();
        } else if ( firstItem instanceof Double ) {
            return vectorBuilder.setDoubleVector(
                    DoubleVector.newBuilder().addAllVector( (List<Double>) vectorObject ).build() ).build();
        } else if ( firstItem instanceof Long ) {
            return vectorBuilder.setLongVector(
                    LongVector.newBuilder().addAllVector( (List<Long>) vectorObject ).build() ).build();
        } else if ( firstItem instanceof Float ) {
            return vectorBuilder.setFloatVector(
                    FloatVector.newBuilder().addAllVector( (List<Float>) vectorObject ).build() ).build();
        } else if ( firstItem instanceof Boolean ) {
            return vectorBuilder.setBoolVector(
                    BoolVector.newBuilder().addAllVector( (List<Boolean>) vectorObject ).build() ).build();
        } else {
            return null;
        }
    }


    private static Expression arrayListToExpression( List<RexNode> operands, PolyType innerType ) {
        List<Object> list = arrayCallToList( operands, innerType );

        switch ( innerType ) {
            case DECIMAL: {
                List<Object> stringEncoded = convertBigDecimalArray( list );
                return Expressions.call(
                        Types.lookupMethod( Linq4JFixer.class, "fixBigDecimalArray", List.class ),
                        Expressions.constant( stringEncoded ) );
            }
            default:
                return Expressions.constant( list );
        }
    }


    private static List convertBigDecimalArray( List<Object> bigDecimalArray ) {
        List<Object> fixedList = new ArrayList<>( bigDecimalArray.size() );
        for ( Object o : bigDecimalArray ) {
            if ( o instanceof BigDecimal ) {
                fixedList.add( ((BigDecimal) o).toString() );
            } else {
                fixedList.add( convertBigDecimalArray( (List) o ) );
            }
        }
        return fixedList;
    }


    private static List<Object> arrayCallToList( List<RexNode> operands, PolyType innerType ) {
        List<Object> list = new ArrayList<>( operands.size() );
        for ( RexNode node : operands ) {
            if ( node instanceof RexLiteral ) {
                list.add( rexLiteralToJavaClass( (RexLiteral) node, innerType ) );
            } else if ( node instanceof RexCall ) {
                list.add( arrayCallToList( ((RexCall) node).operands, innerType ) );
            } else {
                throw new RuntimeException( "Invalid array." );
            }
        }

        return list;
    }


    private static Object rexLiteralToJavaClass( RexLiteral rexLiteral, PolyType actualType ) {
        switch ( actualType ) {
            case BOOLEAN:
                return rexLiteral.getValueAs( Boolean.class );
            case INTEGER:
                return rexLiteral.getValueAs( Integer.class );
            case BIGINT:
                return rexLiteral.getValueAs( Long.class );
            case DOUBLE:
                return rexLiteral.getValueAs( Double.class );
            case REAL:
            case FLOAT:
                return rexLiteral.getValueAs( Float.class );
            case VARCHAR:
            case CHAR:
                return rexLiteral.getValueAs( String.class );
            case TIMESTAMP:
                return rexLiteral.getValueAs( Long.class );
            case DATE:
            case TIME:
                return rexLiteral.getValueAs( Integer.class );
            case DECIMAL:
                return rexLiteral.getValueAs( BigDecimal.class );
            case VARBINARY:
            case BINARY:
                return rexLiteral.getValueAs( ByteString.class );
            case TINYINT:
                return rexLiteral.getValueAs( Byte.class );
            case SMALLINT:
                return rexLiteral.getValueAs( Short.class );
            default:
                throw new RuntimeException( "Type " + actualType + " is not supported by the cottontail adapter." );
        }
    }


    public static From fromFromTableAndSchema( String table, String schema ) {
        return From.newBuilder().setEntity(
                Entity.newBuilder().setName( table ).setSchema(
                        Schema.newBuilder().setName( schema )
                ) ).build();
    }


    public static Expression knnCallToFunctionExpression( RexCall knnCall, List<String> physicalColumnNames, RexNode limitNode ) {

        BlockBuilder inner = new BlockBuilder();
        ParameterExpression dynamicParameterMap_ = Expressions.parameter( Modifier.FINAL, Map.class, inner.newName( "dynamicParameters" ) );

        Expression targetColumn = knnCallTargetColumn( knnCall.getOperands().get( 0 ), physicalColumnNames, dynamicParameterMap_ );

        Expression targetVector = knnCallVector( knnCall.getOperands().get( 1 ), dynamicParameterMap_ );

        Expression distance = knnCallDistance( knnCall.getOperands().get( 2 ), dynamicParameterMap_ );

        Expression weightsVector;
        Expression optimisationFactor;

        if ( knnCall.getOperands().size() == 4 ) {
            weightsVector = knnCallVector( knnCall.getOperands().get( 3 ), dynamicParameterMap_ );
        } else {
            weightsVector = Expressions.constant( null );
        }

        optimisationFactor = knnCallOptimisationFactor( limitNode, dynamicParameterMap_ );

        return Expressions.lambda(
                Expressions.block(
                        Expressions.return_( null,
                                Expressions.call( COTTONTAIL_KNN_BUILDER_METHOD,
                                        targetColumn,
                                        optimisationFactor,
                                        distance,
                                        targetVector,
                                        weightsVector
                                        ) ) ),
                dynamicParameterMap_ );
    }


    private static Expression knnCallTargetColumn( RexNode node, List<String> physicalColumnNames, ParameterExpression dynamicParamMap ) {
        if ( node instanceof RexInputRef ) {
            RexInputRef inputRef = (RexInputRef) node;
            return Expressions.constant( physicalColumnNames.get( inputRef.getIndex() ) );
        } else if ( node instanceof RexDynamicParam ) {
            RexDynamicParam dynamicParam = (RexDynamicParam) node;
            return Expressions.call( dynamicParamMap, BuiltInMethod.MAP_GET.method,
                    Expressions.constant( dynamicParam.getIndex() ) );
        }

        throw new RuntimeException( "first argument is neither an input ref nor a dynamic parameter" );
    }


    private static Expression knnCallVector( RexNode node, ParameterExpression dynamicParamMap ) {
        if ( (node instanceof RexCall) && (((RexCall) node).getOperator() instanceof SqlArrayValueConstructor) ) {
//            List<Object> arrayList = arrayCallToList( ((RexCall) node).getOperands(), node.getType().getComponentType().getPolyType() );
            Expression arrayList = arrayListToExpression( ((RexCall) node).getOperands(), node.getType().getComponentType().getPolyType() );
            return Expressions.call( CottontailTypeUtil.class, "toVectorData", arrayList );
        } else if ( node instanceof RexDynamicParam ) {
            RexDynamicParam dynamicParam = (RexDynamicParam) node;
            return Expressions.call( CottontailTypeUtil.class, "toVectorData", Expressions.call( dynamicParamMap, BuiltInMethod.MAP_GET.method,
                    Expressions.constant( dynamicParam.getIndex() ) ) );
        }

        throw new RuntimeException( "argument is neither an array call nor a dynamic parameter" );
    }


    private static Expression knnCallDistance( RexNode node, ParameterExpression dynamicParamMap ) {
        if ( node instanceof RexLiteral ) {
            return Expressions.constant( ((RexLiteral) node).getValue2() );
        } else if ( node instanceof RexDynamicParam ) {
            RexDynamicParam dynamicParam = (RexDynamicParam) node;
            return Expressions.call( dynamicParamMap, BuiltInMethod.MAP_GET.method,
                    Expressions.constant( dynamicParam.getIndex() ) );
        }

        throw new RuntimeException( "argument is neither an array call nor a dynamic parameter" );
    }


    private static Expression knnCallOptimisationFactor( RexNode node, ParameterExpression dynamicParamMap ) {
        if ( node instanceof RexLiteral ) {
            return Expressions.constant( ((RexLiteral) node).getValueAs( Integer.class ) );
        } else if ( node instanceof RexDynamicParam ) {
            RexDynamicParam dynamicParam = (RexDynamicParam) node;
            return Expressions.call( dynamicParamMap, BuiltInMethod.MAP_GET.method,
                    Expressions.constant( dynamicParam.getIndex() ) );
        }

        throw new RuntimeException( "argument is neither an int nor a dynamic parameter" );

    }


    public static Object defaultValueParser( CatalogDefaultValue catalogDefaultValue, PolyType actualType ) {
        if ( actualType == PolyType.ARRAY ) {
            throw new RuntimeException( "Default values are not supported for array types" );
        }

        Object literal;
        switch ( actualType ) {
            case BOOLEAN:
                literal = Boolean.parseBoolean( catalogDefaultValue.value );
                break;
            case INTEGER:
                literal = SqlLiteral.createExactNumeric( catalogDefaultValue.value, SqlParserPos.ZERO ).getValueAs( Integer.class );
                break;
            case DECIMAL:
                literal = SqlLiteral.createExactNumeric( catalogDefaultValue.value, SqlParserPos.ZERO ).getValueAs( BigDecimal.class );
                break;
            case BIGINT:
                literal = SqlLiteral.createExactNumeric( catalogDefaultValue.value, SqlParserPos.ZERO ).getValueAs( Long.class );
                break;
            case REAL:
            case FLOAT:
                literal = SqlLiteral.createApproxNumeric( catalogDefaultValue.value, SqlParserPos.ZERO ).getValueAs( Float.class );
                break;
            case DOUBLE:
                literal = SqlLiteral.createApproxNumeric( catalogDefaultValue.value, SqlParserPos.ZERO ).getValueAs( Double.class );
                break;
            case VARCHAR:
                literal = catalogDefaultValue.value;
                break;
            default:
                throw new PolyphenyDbException( "Not yet supported default value type: " + actualType );
        }

        return literal;
    }
}
