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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.avatica.util.ByteString;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
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
    public static PolyValue getTinyIntData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return PolyInteger.of( ((Integer) data) );
    }


    /**
     * Converts the given object and returns it as {@link Short}.
     *
     * @param data The data, expected to be {@link Short}.
     * @return {@link Short}
     */
    public static PolyInteger getSmallIntData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return PolyInteger.of( (Integer) data );
    }


    /**
     * Converts the given object and returns it as {@link String}.
     *
     * @param data The data, expected to be {@link String}.
     * @return {@link String}
     */
    public static PolyString getStringData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return PolyString.of( (String) data );
    }


    /**
     * Converts the given object and returns it as {@link BigDecimal} object.
     *
     * @param data The data, expected to be {@link String}.
     * @return {@link BigDecimal}
     */
    public static PolyBigDecimal getDecimalData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return PolyBigDecimal.of( (String) data );
    }


    /**
     * Converts the given object and returns it as {@link ByteString} object.
     *
     * @param data The data, expected to be {@link String}.
     * @return {@link ByteString}
     */
    public static PolyBinary getBinaryData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return PolyBinary.of( ByteString.parseBase64( (String) data ) );
    }


    /**
     * Converts the given object and returns it as {@link Integer} object.
     *
     * @param data The data, expected to be {@link Integer}.
     * @return {@link Integer}
     */
    public static PolyTime getTimeData( Object data ) {
        if ( !(data instanceof Integer) ) {
            return null;
        }
        return PolyTime.of( (Integer) data );
    }


    /**
     * Converts the given object and returns it as {@link Double} object.
     *
     * @param data The data, expected to be {@link Double}.
     * @return {@link PolyDouble}
     */
    @SuppressWarnings("unused")
    public static PolyDouble getDoubleData( Object data ) {
        if ( !(data instanceof Double) ) {
            return null;
        }
        return PolyDouble.of( (Double) data );
    }

    /**
     * Converts the given object and returns it as {@link Integer} object.
     *
     * @param data The data, expected to be {@link Integer}.
     * @return {@link Integer}
     */
    public static PolyDate getDateData( Object data ) {
        if ( !(data instanceof Integer) ) {
            return null;
        }
        return PolyDate.ofDays( (Integer) data );
    }


    /**
     * Converts a {@link java.util.Date} object to an {@link Integer}.
     *
     * @param data The data, expected to be {@link java.util.Date}.
     * @return {@link Integer}
     */
    public static PolyTimestamp getTimestampData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return PolyTimestamp.of( (java.util.Date) data );
    }


    @SuppressWarnings("unused")
    public static PolyFloat getRealData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return PolyFloat.of( (Number) data );
    }


    @SuppressWarnings("unused")
    public static PolyLong getBigIntData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return PolyLong.of( (Number) data );
    }


    @SuppressWarnings("unused")
    public static PolyInteger getIntData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return PolyInteger.of( (Integer) data );
    }


    @SuppressWarnings("unused")
    public static PolyBoolean getBoolData( Object data ) {
        if ( data == null ) {
            return null;
        }
        return PolyBoolean.of( (Boolean) data );
    }



    public static PolyList<PolyBoolean> getBoolVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<PolyBoolean> list = new ArrayList<>( ((boolean[]) data).length );
        for ( boolean v : ((boolean[]) data) ) {
            list.add( PolyBoolean.of( v ) );
        }
        return PolyList.copyOf( list );
    }


    public static PolyList<PolyInteger> getTinyIntVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<PolyInteger> list = new ArrayList<>( ((int[]) data).length );
        for ( int v : ((int[]) data) ) {
            list.add( PolyInteger.of( v ) );
        }
        return PolyList.copyOf( list );
    }


    public static PolyList<PolyInteger> getSmallIntVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<PolyInteger> list = new ArrayList<>( ((int[]) data).length );
        for ( int v : ((int[]) data) ) {
            list.add( PolyInteger.of( v ) );
        }
        return PolyList.copyOf( list );
    }


    public static PolyList<PolyInteger> getIntVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<PolyInteger> list = new ArrayList<>( ((int[]) data).length );
        for ( int v : ((int[]) data) ) {
            list.add( PolyInteger.of( v ) );
        }
        return PolyList.copyOf( list );
    }


    public static PolyList<PolyLong> getLongVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<PolyLong> list = new ArrayList<>( ((long[]) data).length );
        for ( long v : ((long[]) data) ) {
            list.add( PolyLong.of( v ) );
        }
        return PolyList.copyOf( list );
    }


    public static PolyList<PolyFloat> getFloatVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<PolyFloat> list = new ArrayList<>( ((float[]) data).length );
        for ( float v : ((float[]) data) ) {
            list.add( PolyFloat.of( v ) );
        }
        return PolyList.copyOf( list );
    }


    public static PolyList<PolyDouble> getDoubleVector( Object data ) {
        if ( data == null ) {
            return null;
        }
        final List<PolyDouble> list = new ArrayList<>( ((double[]) data).length );
        for ( double v : ((double[]) data) ) {
            list.add( PolyDouble.of( v ) );
        }
        return PolyList.copyOf( list );
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

        throw new GenericRuntimeException( "Not a proper filter expression!" );
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
    public static Projection.ProjectionElement generateKnn( String p, Vector q, PolyValue distance, String alias ) {
        final Projection.ProjectionElement.Builder builder = Projection.ProjectionElement.newBuilder();
        builder.setFunction( Function.newBuilder()
                .setName( getDistance( distance ) )
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
    public static FunctionName getDistance( PolyValue norm ) {
        final String value = switch ( norm.asString().value.toUpperCase() ) {
            case "L1" -> Distances.L1.getFunctionName();
            case "L2" -> Distances.L2.getFunctionName();
            case "L2SQUARED" -> Distances.L2SQUARED.getFunctionName();
            case "CHISQUARED" -> Distances.CHISQUARED.getFunctionName();
            case "COSINE" -> Distances.COSINE.getFunctionName();
            default -> throw new IllegalArgumentException( "Unknown norm: " + norm );
        };
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
