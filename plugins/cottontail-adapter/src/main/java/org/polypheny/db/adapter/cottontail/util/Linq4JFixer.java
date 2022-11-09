/*
 * Copyright 2019-2022 The Polypheny Project
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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.avatica.util.ByteString;
import org.vitrivr.cottontail.client.language.basics.Distances;
import org.vitrivr.cottontail.grpc.CottontailGrpc.AtomicBooleanOperand;
import org.vitrivr.cottontail.grpc.CottontailGrpc.AtomicBooleanPredicate;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ComparisonOperator;
import org.vitrivr.cottontail.grpc.CottontailGrpc.CompoundBooleanPredicate;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ConnectionOperator;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Expression;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Expressions;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Function;
import org.vitrivr.cottontail.grpc.CottontailGrpc.FunctionName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Literal;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Projection;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Vector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Where;


/**
 * A collections of conversion methods used for data access (sometimes used reflectively).
 */
public class Linq4JFixer {

    /**
     * Converts the given object and returns it as {@link Byte}.
     *
     * @param data The data, expected to be {@link Byte}.
     * @return {@link Byte}
     */
    public static Object getTinyIntData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return ((Integer) data).byteValue();
    }


    /**
     * Converts the given object and returns it as {@link Short}.
     *
     * @param data The data, expected to be {@link Short}.
     * @return {@link Short}
     */
    public static Object getSmallIntData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return ((Integer) data).shortValue();
    }


    /**
     * Converts the given object and returns it as {@link String}.
     *
     * @param data The data, expected to be {@link String}.
     * @return {@link String}
     */
    public static String getStringData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return (String) data;
    }


    /**
     * Converts the given object and returns it as {@link BigDecimal} object.
     *
     * @param data The data, expected to be {@link String}.
     * @return {@link BigDecimal}
     */
    public static Object getDecimalData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return new BigDecimal( (String) data );
    }


    /**
     * Converts the given object and returns it as {@link ByteString} object.
     *
     * @param data The data, expected to be {@link String}.
     * @return {@link ByteString}
     */
    public static Object getBinaryData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return ByteString.parseBase64( (String) data );
    }


    /**
     * Converts the given object and returns it as {@link Integer} object.
     *
     * @param data The data, expected to be {@link Integer}.
     * @return {@link Integer}
     */
    public static Object getTimeData( Object data ) {
        if ( !(data instanceof Integer) ) {
            return null;
        }
        return data;
    }


    /**
     * Converts the given object and returns it as {@link Integer} object.
     *
     * @param data The data, expected to be {@link Integer}.
     * @return {@link Integer}
     */
    public static Object getDateData( Object data ) {
        if ( !(data instanceof Integer) ) {
            return null;
        }
        return data;
    }


    /**
     * Converts a {@link java.util.Date} object to an {@link Integer}.
     *
     * @param data The data, expected to be {@link java.util.Date}.
     * @return {@link Integer}
     */
    public static Object getTimestampData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return ((java.util.Date) data).getTime();
    }


    public static Object getBoolVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<Boolean> list = new ArrayList<>( ((boolean[]) data).length );
        for ( boolean v : ((boolean[]) data) ) {
            list.add( v );
        }
        return list;
    }


    public static Object getTinyIntVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<Byte> list = new ArrayList<>( ((int[]) data).length );
        for ( int v : ((int[]) data) ) {
            list.add( (byte) v );
        }
        return list;
    }


    public static Object getSmallIntVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<Short> list = new ArrayList<>( ((int[]) data).length );
        for ( int v : ((int[]) data) ) {
            list.add( (short) v );
        }
        return list;
    }


    public static Object getIntVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<Integer> list = new ArrayList<>( ((int[]) data).length );
        for ( int v : ((int[]) data) ) {
            list.add( v );
        }
        return list;
    }


    public static Object getLongVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<Long> list = new ArrayList<>( ((long[]) data).length );
        for ( long v : ((long[]) data) ) {
            list.add( v );
        }
        return list;
    }


    public static Object getFloatVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<Float> list = new ArrayList<>( ((float[]) data).length );
        for ( float v : ((float[]) data) ) {
            list.add( v );
        }
        return list;
    }


    public static Object getDoubleVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<Double> list = new ArrayList<>( ((double[]) data).length );
        for ( double v : ((double[]) data) ) {
            list.add( v );
        }
        return list;
    }


    public static CompoundBooleanPredicate generateCompoundPredicate(
            Object operator_,
//                CompoundBooleanPredicate.Operator operator,
            Object left,
            Object right
    ) {
        ConnectionOperator operator = (ConnectionOperator) operator_;
        CompoundBooleanPredicate.Builder builder = CompoundBooleanPredicate.newBuilder();
        builder = builder.setOp( operator );

        if ( left instanceof AtomicBooleanPredicate ) {
            builder.setAleft( (AtomicBooleanPredicate) left );
        } else {
            builder.setCleft( (CompoundBooleanPredicate) left );
        }

        if ( right instanceof AtomicBooleanPredicate ) {
            builder.setAright( (AtomicBooleanPredicate) right );
        } else {
            builder.setCright( (CompoundBooleanPredicate) right );
        }

        return builder.build();
    }


    public static AtomicBooleanPredicate generateAtomicPredicate(
            String attribute,
            Boolean not,
            Object operator_,
            Object data_
    ) {
        final ComparisonOperator operator = (ComparisonOperator) operator_;
        final Literal data = (Literal) data_;
        return AtomicBooleanPredicate.newBuilder().setNot( not )
                .setLeft( ColumnName.newBuilder().setName( attribute ).build() )
                .setOp( operator )
                .setRight( AtomicBooleanOperand.newBuilder().setExpressions( Expressions.newBuilder().addExpression( Expression.newBuilder().setLiteral( data ) ) ) )
                .build();
    }


    public static Where generateWhere( Object filterExpression ) {
        if ( filterExpression instanceof AtomicBooleanPredicate ) {
            return Where.newBuilder().setAtomic( (AtomicBooleanPredicate) filterExpression ).build();
        }

        if ( filterExpression instanceof CompoundBooleanPredicate ) {
            return Where.newBuilder().setCompound( (CompoundBooleanPredicate) filterExpression ).build();
        }

        throw new RuntimeException( "Not a proper filter expression!" );
    }


    /**
     * Generates and returns the kNN query function for the give arguments.
     *
     * @param p The column name of the probing argument
     * @param q The query vector.
     * @param distance The name of the distance to execute.
     * @param alias The alias to use for the resulting column.
     * @return The resulting {@link Function} expression.
     */
    public static Projection.ProjectionElement generateKnn( String p, Vector q, Object distance, String alias ) {
        final Projection.ProjectionElement.Builder builder = Projection.ProjectionElement.newBuilder();
        builder.setFunction( Function.newBuilder()
                .setName( getDistance( (String) distance ) )
                .addArguments( Expression.newBuilder().setColumn( ColumnName.newBuilder().setName( p ) ) )
                .addArguments( Expression.newBuilder().setLiteral( Literal.newBuilder().setVectorData( q ) ) ) );
        if ( alias != null ) {
            builder.setAlias( ColumnName.newBuilder().setName( alias ).build() );
        }

        return builder.build();
    }


    /**
     * Maps the given name to a {@link FunctionName} object.
     *
     * @param norm The name of the distance to execute.
     * @return The corresponding {@link FunctionName}
     */
    public static FunctionName getDistance( String norm ) {
        final String value;
        switch ( norm.toUpperCase() ) {
            case "L1":
                value = Distances.L1.getFunctionName();
                break;
            case "L2":
                value = Distances.L2.getFunctionName();
                break;
            case "L2SQUARED":
                value = Distances.L2SQUARED.getFunctionName();
                break;
            case "CHISQUARED":
                value = Distances.CHISQUARED.getFunctionName();
                break;
            case "COSINE":
                value = Distances.COSINE.getFunctionName();
                break;
            default:
                throw new IllegalArgumentException( "Unknown norm: " + norm );
        }
        return FunctionName.newBuilder().setName( value ).build();
    }


    public static List fixBigDecimalArray( List stringEncodedArray ) {
        List<Object> fixedList = new ArrayList<>( stringEncodedArray.size() );
        for ( Object o : stringEncodedArray ) {
            if ( o instanceof String ) {
                fixedList.add( new BigDecimal( (String) o ) );
            } else {
                fixedList.add( fixBigDecimalArray( (List) o ) );
            }
        }
        return fixedList;
    }

}
