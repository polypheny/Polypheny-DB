/*
 * Copyright 2019-2021 The Polypheny Project
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
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.ByteString;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.AtomicBooleanOperand;
import org.vitrivr.cottontail.grpc.CottontailGrpc.AtomicBooleanPredicate;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ComparisonOperator;
import org.vitrivr.cottontail.grpc.CottontailGrpc.CompoundBooleanPredicate;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ConnectionOperator;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Knn;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Knn.Distance;
import org.vitrivr.cottontail.grpc.CottontailGrpc.KnnHint;
import org.vitrivr.cottontail.grpc.CottontailGrpc.KnnHint.NoIndexKnnHint;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Literal;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Literals;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Vector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Where;


public class Linq4JFixer {


    public static Object getBooleanData( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getBooleanData();
    }


    public static Object getIntData( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getIntData();
    }


    public static Object getLongData( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getLongData();
    }


    public static Object getTinyIntData( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return Integer.valueOf( ((CottontailGrpc.Literal) data).getIntData() ).byteValue();
    }


    public static Object getSmallIntData( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return Integer.valueOf( ((CottontailGrpc.Literal) data).getIntData() ).shortValue();
    }


    public static Object getFloatData( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getFloatData();
    }


    public static Object getDoubleData( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getDoubleData();
    }


    public static String getStringData( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getStringData();
    }


    public static Object getDecimalData( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return new BigDecimal( ((CottontailGrpc.Literal) data).getStringData() );
    }


    public static Object getBinaryData( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ByteString.parseBase64( ((CottontailGrpc.Literal) data).getStringData() );
    }


    public static Object getTimeData( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getIntData();
    }


    public static Object getDateData( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getIntData();
    }


    public static Object getTimestampData( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getDateData().getUtcTimestamp();
    }


    public static Object getNullData( Object data ) {
        return ((CottontailGrpc.Literal) data).getNullData();
    }


    public static Object getBoolVector( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getVectorData().getBoolVector().getVectorList();
    }


    public static Object getTinyIntVector( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getVectorData().getIntVector().getVectorList().stream().map( Integer::byteValue ).collect( Collectors.toList() );
    }


    public static Object getSmallIntVector( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getVectorData().getIntVector().getVectorList().stream().map( Integer::shortValue ).collect( Collectors.toList() );
    }


    public static Object getIntVector( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getVectorData().getIntVector().getVectorList();
    }


    public static Object getFloatVector( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getVectorData().getFloatVector().getVectorList();
    }


    public static Object getDoubleVector( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getVectorData().getDoubleVector().getVectorList();
    }


    public static Object getLongVector( Object data ) {
        if ( ((CottontailGrpc.Literal) data).hasNullData() ) {
            return null;
        }
        return ((CottontailGrpc.Literal) data).getVectorData().getLongVector().getVectorList();
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
                .setRight( AtomicBooleanOperand.newBuilder().setLiterals( Literals.newBuilder().addLiteral( data ).build() ).build() )
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


    public static Knn generateKnn(
            Object column,
            Object k,
            Object distance,
            Object target,
            Object weights ) {
        Knn.Builder knnBuilder = Knn.newBuilder();

        knnBuilder.setAttribute( ColumnName.newBuilder().setName( (String) column ) );

        if ( k != null ) {
            knnBuilder.setK( (Integer) k );
        } else {
            // Cottontail requires a k to be set for these queries.
            // Setting Integer.MAX_VALUE will cause an out of memory with cottontail
            // 2050000000 still works, 2100000000 doesn't work
            // I will just decide that 1000000 is a reasonable default value!
            knnBuilder.setK( 1000000 );
        }

        if ( target != null ) {
            knnBuilder.setQuery( (Vector) target );
        }

        if ( weights != null ) {
            knnBuilder.setWeight( (Vector) weights );
        }

        knnBuilder.setDistance( getDistance( (String) distance ) );

        knnBuilder.setHint( KnnHint.newBuilder().setNoIndexHint( NoIndexKnnHint.newBuilder() ) );

        return knnBuilder.build();
    }


    public static Knn.Distance getDistance( String norm ) {
        switch ( norm ) {
            case "L1":
                return Distance.L1;
            case "L2":
                return Distance.L2;
            case "L2SQUARED":
                return Distance.L2SQUARED;
            case "CHISQUARED":
                return Distance.CHISQUARED;
            case "COSINE":
                return Distance.COSINE;
            default:
                throw new IllegalArgumentException( "Unknown norm: " + norm );
        }
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
