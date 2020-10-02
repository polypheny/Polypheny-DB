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


import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.BoolVector;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.Data;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.DoubleVector;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.FloatVector;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.IntVector;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.LongVector;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.Type;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.Vector;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.fun.SqlArrayValueConstructor;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;


public class CottontailTypeUtil {

    public static final Method COTTONTAIL_SIMPLE_CONSTANT_TO_DATA_METHOD = Types.lookupMethod(
            CottontailTypeUtil.class,
            "toData",
            Object.class );

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
                case DATE:
                    return Type.LONG;
                case TIME:
                case TIMESTAMP:
                    return Type.INTEGER;
                case DECIMAL:
                case VARBINARY:
                case BINARY:
                    return Type.STRING;
            }
        }

        throw new RuntimeException( "Type " + logicalType + " is not supported by the cottontail adapter." );
    }


    public static CottontailGrpc.Data rexLiteralToData( RexLiteral rexLiteral ) {
        CottontailGrpc.Data.Builder builder = Data.newBuilder();
        return rexLiteralToData( rexLiteral, builder );
    }

    public static CottontailGrpc.Data rexLiteralToData( RexLiteral rexLiteral, CottontailGrpc.Data.Builder builder ) {
        builder.clear();
        switch ( rexLiteral.getTypeName() ) {
            case BOOLEAN:
                return builder.setBooleanData( rexLiteral.getValueAs( Boolean.class ) ).build();
            case INTEGER:
                return builder.setIntData( rexLiteral.getValueAs( Integer.class ) ).build();
            case BIGINT:
                return builder.setLongData( rexLiteral.getValueAs( Long.class ) ).build();
            case DOUBLE:
                return builder.setDoubleData( rexLiteral.getValueAs( Double.class ) ).build();
            case REAL:
            case FLOAT:
                return builder.setFloatData( rexLiteral.getValueAs( Float.class ) ).build();
            case VARCHAR:
            case CHAR:
                return builder.setStringData( rexLiteral.getValueAs( String.class ) ).build();
            case TIMESTAMP:
                return builder.setLongData( rexLiteral.getValueAs( Long.class ) ).build();
            case DATE:
            case TIME:
                return builder.setIntData( rexLiteral.getValueAs( Integer.class ) ).build();
            case DECIMAL:
                return builder.setStringData( org.polypheny.db.adapter.cottontail.util.CottontailSerialisation.GSON.toJson( rexLiteral.getValueAs( BigDecimal.class ) ) ).build();
            case VARBINARY:
            case BINARY:
                return builder.setStringData( rexLiteral.getValueAs( ByteString.class ).toBase64String() ).build();
        }
        throw new RuntimeException( "Type " + rexLiteral.getTypeName() + " is not supported by the cottontail adapter." );
    }


    public static Expression rexDynamicParamToDataExpression( RexDynamicParam dynamicParam ) {
        
    }

    public static Expression rexLiteralToDataExpression( RexLiteral rexLiteral ) {
        ConstantExpression constantExpression;
        switch ( rexLiteral.getTypeName() ) {
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
            case DECIMAL:
                constantExpression = Expressions.constant( org.polypheny.db.adapter.cottontail.util.CottontailSerialisation.GSON.toJson( rexLiteral.getValueAs( BigDecimal.class ) ) );
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


    public static CottontailGrpc.Data toData( Object value ) {
        CottontailGrpc.Data.Builder builder = Data.newBuilder();
        if ( value instanceof Boolean ) {
            return builder.setBooleanData( (Boolean) value ).build();
        } else if ( value instanceof Integer ) {
            return builder.setIntData( (Integer) value ).build();
        } else if ( value instanceof Long ) {
            return builder.setLongData( (Long) value ).build();
        } else if ( value instanceof Double ) {
            return builder.setDoubleData( (Double) value ).build();
        } else if ( value instanceof Float ) {
            return builder.setFloatData( (Float) value ).build();
        } else if ( value instanceof String ) {
            return builder.setStringData( (String) value ).build();
        } else if ( value instanceof BigDecimal ) {
            return builder.setStringData( org.polypheny.db.adapter.cottontail.util.CottontailSerialisation.GSON.toJson( (BigDecimal) value ) ).build();
        } else if ( value instanceof List ) {
            Vector.Builder vectorBuilder = Vector.newBuilder();
            // TODO js(ct): add list.size() == 0 handling
            Object firstItem = ((List) value).get( 0 );
            if ( firstItem instanceof Integer ) {
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
                                BoolVector.newBuilder().addAllVector( (List<Boolean>) value ).build() ) ).build();
            } else {
                return builder.setStringData( org.polypheny.db.adapter.cottontail.util.CottontailSerialisation.GSON.toJson( (List<Object>) value ) ).build();
            }
        } else {
            throw new RuntimeException( "Cottontail data type error: Type not handled." );
        }
    }

    public static CottontailGrpc.Data rexArrayCallToData( RexCall rexCall ) {
        CottontailGrpc.Data.Builder builder = Data.newBuilder();
        return rexArrayCallToData( rexCall, builder );
    }

    public static CottontailGrpc.Data rexArrayCallToData( RexCall rexCall, CottontailGrpc.Data.Builder builder ) {
        builder.clear();
        Vector.Builder vectorBuilder = Vector.newBuilder();

        if ( !(rexCall.op instanceof SqlArrayValueConstructor) ) {
            throw new RuntimeException( "Not an SqlArrayValueConstructor." );
        }

        if ( !(rexCall.type instanceof ArrayType) ) {
            throw new RuntimeException( "Not an ArrayType." );
        }

        ArrayType arrayType = (ArrayType) rexCall.type;

        if ( arrayType.getDimension() != 1L ) {
            List<Object> arrayValue = arrayConstructorToList( rexCall );
            return builder.setStringData( org.polypheny.db.adapter.cottontail.util.CottontailSerialisation.GSON.toJson( arrayValue ) ).build();
        }

        switch ( arrayType.getComponentType().getPolyType() ) {
            case INTEGER: {
                List<Integer> integerArray = flatArrayCallToList( rexCall.operands );
                return builder.setVectorData(
                        vectorBuilder.setIntVector(
                                IntVector.newBuilder().addAllVector( integerArray ).build() ) ).build();
            }
            case DOUBLE: {
                List<Double> doubleArray = flatArrayCallToList( rexCall.operands );
                return builder.setVectorData(
                        vectorBuilder.setDoubleVector(
                                DoubleVector.newBuilder().addAllVector( doubleArray ).build() ) ).build();
            }
            case BIGINT: {
                List<Long> longArray = flatArrayCallToList( rexCall.operands );
                return builder.setVectorData(
                        vectorBuilder.setLongVector(
                                LongVector.newBuilder().addAllVector( longArray ).build() ) ).build();
            }
            case FLOAT:
            case REAL: {
                List<Float> floatArray = flatArrayCallToList( rexCall.operands );
                return builder.setVectorData(
                        vectorBuilder.setFloatVector(
                                FloatVector.newBuilder().addAllVector( floatArray ).build() ) ).build();
            }
            case BOOLEAN: {
                List<Boolean> boolArray = flatArrayCallToList( rexCall.operands );
                return builder.setVectorData(
                        vectorBuilder.setBoolVector(
                                BoolVector.newBuilder().addAllVector( boolArray ).build() ) ).build();
            }
            default: {
                List<Object> arrayValue = arrayConstructorToList( rexCall );
                return builder.setStringData( org.polypheny.db.adapter.cottontail.util.CottontailSerialisation.GSON.toJson( arrayValue ) ).build();
            }
        }
    }

    public static List<Object> arrayConstructorToList( RexCall rexCall ) {
        return arrayCallToList( rexCall.operands );
    }


    private static <T> List<T> flatArrayCallToList( List<RexNode> operands ) {
        List<T> list = new ArrayList<>( operands.size() );
        for ( RexNode node : operands ) {
            if ( node instanceof RexLiteral ) {
                Object literalValue = rexLiteralToJavaClass( (RexLiteral) node );
                list.add( (T) literalValue );
            } else {
                throw new RuntimeException( "Invalid array." );
            }
        }

        return list;
    }



    private static List<Object> arrayCallToList( List<RexNode> operands ) {
        List<Object> list = new ArrayList<>( operands.size() );
        for ( RexNode node : operands ) {
            if ( node instanceof RexLiteral ) {
                list.add( rexLiteralToJavaClass( (RexLiteral) node ) );
            } else if ( node instanceof RexCall ) {
                list.add( arrayCallToList( ((RexCall) node).operands ) );
            } else {
                throw new RuntimeException( "Invalid array." );
            }
        }

        return list;
    }


    private static Object rexLiteralToJavaClass( RexLiteral rexLiteral ) {
        switch ( rexLiteral.getTypeName() ) {
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
                rexLiteral.getValueAs( ByteString.class );
            default:
                throw new RuntimeException( "Type " + rexLiteral.getTypeName() + " is not supported by the cottontail adapter." );
        }
    }
}
