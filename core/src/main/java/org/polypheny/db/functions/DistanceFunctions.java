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

package org.polypheny.db.functions;


import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.util.Pair;


public class DistanceFunctions {

    private DistanceFunctions() {
        // empty on purpose
    }


    protected static PolyDouble l1Metric( List<PolyNumber> value, List<PolyNumber> target ) {
        double result = 0;
        for ( int i = 0; i < value.size(); i++ ) {
            result += Math.abs( value.get( i ).doubleValue() - target.get( i ).doubleValue() );
        }
        return PolyDouble.of( result );
        // Apparently java for loops are faster than the stream api. It also looks easier to maintain, even though I love streams :/
    }


    protected static PolyDouble l1MetricWeighted( List<PolyNumber> value, List<PolyNumber> target, List<PolyNumber> weights ) {
        double result = 0;
        for ( int i = 0; i < value.size(); i++ ) {
            result += Math.abs( value.get( i ).doubleValue() - target.get( i ).doubleValue() ) * weights.get( i ).doubleValue();
        }
        return PolyDouble.of( result );
    }


    protected static PolyDouble l2SquaredMetric( List<PolyNumber> value, List<PolyNumber> target ) {
        double result = 0;
        for ( int i = 0; i < value.size(); i++ ) {
            result += Math.pow( value.get( i ).doubleValue() - target.get( i ).doubleValue(), 2.0 );
        }
        return PolyDouble.of( result );
    }


    protected static PolyDouble l2SquaredMetricWeighted( List<PolyNumber> value, List<PolyNumber> target, List<PolyNumber> weights ) {
        double result = 0;
        for ( int i = 0; i < value.size(); i++ ) {
            result += Math.pow( value.get( i ).doubleValue() - target.get( i ).doubleValue(), 2.0 ) * weights.get( i ).doubleValue();
        }
        return PolyDouble.of( result );
    }


    protected static PolyDouble l2Metric( List<PolyNumber> value, List<PolyNumber> target ) {
        return PolyDouble.of( Math.sqrt( l2SquaredMetric( value, target ).doubleValue() ) );
    }


    protected static PolyDouble l2MetricWeighted( List<PolyNumber> value, List<PolyNumber> target, List<PolyNumber> weights ) {
        return PolyDouble.of( Math.sqrt( l2SquaredMetricWeighted( value, target, weights ).doubleValue() ) );
    }


    protected static PolyDouble chiSquaredMetric( List<PolyNumber> value, List<PolyNumber> target ) {
        double result = 0;
        for ( int i = 0; i < value.size(); i++ ) {
            double a = value.get( i ).doubleValue();
            double b = target.get( i ).doubleValue();
            result += Math.pow( a - b, 2.0 ) / (b + a);
        }
        return PolyDouble.of( result );
    }


    protected static PolyDouble chiSquaredMetricWeighted( List<PolyNumber> value, List<PolyNumber> target, List<PolyNumber> weights ) {
        double result = 0;
        for ( int i = 0; i < value.size(); i++ ) {
            double a = value.get( i ).doubleValue();
            double b = target.get( i ).doubleValue();
            double weight = weights.get( i ).doubleValue();
            result += Math.pow( a - b, 2.0 ) / (b + a) * weight;
        }
        return PolyDouble.of( result );
    }


    protected static PolyDouble cosineMetric( List<PolyNumber> value, List<PolyNumber> target ) {
        return PolyDouble.of( 1 - dot( value, target ).doubleValue() / (norm2( value ) * norm2( target )) );
    }


    protected static PolyDouble cosineMetricWeighted( List<PolyNumber> value, List<PolyNumber> target, List<PolyNumber> weights ) {
        List<PolyNumber> valueWeighted = Pair.zip( value, weights ).stream().map( p -> PolyDouble.of( p.left.doubleValue() * p.right.doubleValue() ) ).collect( Collectors.toList() );
        List<PolyNumber> targetWeighted = Pair.zip( target, weights ).stream().map( p -> PolyDouble.of( p.left.doubleValue() * p.right.doubleValue() ) ).collect( Collectors.toList() );
        return cosineMetric( valueWeighted, targetWeighted );
    }


    private static PolyDouble dot( List<PolyNumber> a, List<PolyNumber> b ) {
        double result = 0;
        for ( int i = 0; i < a.size(); i++ ) {
            result += a.get( i ).doubleValue() * b.get( i ).doubleValue();
        }
        return PolyDouble.of( result );
    }


    private static double norm2( List<PolyNumber> list ) {
        return Math.sqrt( list.stream().mapToDouble( a -> Math.pow( a.doubleValue(), 2.0 ) ).sum() );
    }


    protected static void verifyInputs( List<?> a, List<?> b, List<?> w ) {
        if ( a.isEmpty() && b.isEmpty() && (w == null || w.isEmpty()) ) {
            return;
        }

        if ( (a.size() != b.size()) || (w != null && a.size() != w.size()) ) {
            throw new RuntimeException( "Sizes of inputs do not match." );
        }

        if ( !a.get( 0 ).getClass().isArray() || !b.get( 0 ).getClass().isArray() || (w != null && !w.get( 0 ).getClass().isArray()) ) {
            if ( !(a.get( 0 ) instanceof PolyNumber) || !(b.get( 0 ) instanceof PolyNumber) || (w != null && !(w.get( 0 ) instanceof PolyNumber)) ) {
                throw new RuntimeException( "Inputs are not Numbers." );
            }
        } else {
            throw new RuntimeException( "Not usable Arrays" );
        }
    }

}
