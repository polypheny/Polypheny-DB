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

package org.polypheny.db.runtime.functions;


import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.util.Pair;


public class DistanceFunctions {

    private DistanceFunctions() {
        // empty on purpose
    }


    protected static double l1Metric( List<Number> value, List<Number> target ) {
        double result = 0;
        for ( int i = 0; i < value.size(); i++ ) {
            result += Math.abs( value.get( i ).doubleValue() - target.get( i ).doubleValue() );
        }
        return result;
        // Apparently java for loops are faster than the stream api. It also looks easier to maintain, even though I love streams :/
    }


    protected static double l1MetricWeighted( List<Number> value, List<Number> target, List<Number> weights ) {
        double result = 0;
        for ( int i = 0; i < value.size(); i++ ) {
            result += Math.abs( value.get( i ).doubleValue() - target.get( i ).doubleValue() ) * weights.get( i ).doubleValue();
        }
        return result;
    }


    protected static double l2SquaredMetric( List<Number> value, List<Number> target ) {
        double result = 0;
        for ( int i = 0; i < value.size(); i++ ) {
            result += Math.pow( value.get( i ).doubleValue() - target.get( i ).doubleValue(), 2.0 );
        }
        return result;
    }


    protected static double l2SquaredMetricWeighted( List<Number> value, List<Number> target, List<Number> weights ) {
        double result = 0;
        for ( int i = 0; i < value.size(); i++ ) {
            result += Math.pow( value.get( i ).doubleValue() - target.get( i ).doubleValue(), 2.0 ) * weights.get( i ).doubleValue();
        }
        return result;
    }


    protected static double l2Metric( List<Number> value, List<Number> target ) {
        return Math.sqrt( l2SquaredMetric( value, target ) );
    }


    protected static double l2MetricWeighted( List<Number> value, List<Number> target, List<Number> weights ) {
        return Math.sqrt( l2SquaredMetricWeighted( value, target, weights ) );
    }


    protected static double chiSquaredMetric( List<Number> value, List<Number> target ) {
        double result = 0;
        for ( int i = 0; i < value.size(); i++ ) {
            double a = value.get( i ).doubleValue();
            double b = target.get( i ).doubleValue();
            result += Math.pow( a - b, 2.0 ) / (b + a);
        }
        return result;
    }


    protected static double chiSquaredMetricWeighted( List<Number> value, List<Number> target, List<Number> weights ) {
        double result = 0;
        for ( int i = 0; i < value.size(); i++ ) {
            double a = value.get( i ).doubleValue();
            double b = target.get( i ).doubleValue();
            double weight = weights.get( i ).doubleValue();
            result += Math.pow( a - b, 2.0 ) / (b + a) * weight;
        }
        return result;
    }


    protected static double cosineMetric( List<Number> value, List<Number> target ) {
        return 1 - dot( value, target ) / (norm2( value ) * norm2( target ));
    }


    protected static double cosineMetricWeighted( List<Number> value, List<Number> target, List<Number> weights ) {
        List<Number> valueWeighted = Pair.zip( value, weights ).stream().map( p -> p.left.doubleValue() * p.right.doubleValue() ).collect( Collectors.toList() );
        List<Number> targetWeighted = Pair.zip( target, weights ).stream().map( p -> p.left.doubleValue() * p.right.doubleValue() ).collect( Collectors.toList() );
        return cosineMetric( valueWeighted, targetWeighted );
    }


    private static double dot( List<Number> a, List<Number> b ) {
        double result = 0;
        for ( int i = 0; i < a.size(); i++ ) {
            result += a.get( i ).doubleValue() * b.get( i ).doubleValue();
        }
        return result;
    }


    private static double norm2( List<Number> list ) {
        return Math.sqrt( list.stream().mapToDouble( a -> Math.pow( a.doubleValue(), 2.0 ) ).sum() );
    }


    protected static void verifyInputs( List a, List b, List w ) {
        if ( a.isEmpty() && b.isEmpty() && (w == null || w.isEmpty()) ) {
            return;
        }

        if ( (a.size() != b.size()) || (w != null && a.size() != w.size()) ) {
            throw new RuntimeException( "Sizes of inputs do not match." );
        }

        if ( !a.get( 0 ).getClass().isArray() || !b.get( 0 ).getClass().isArray() || (w != null && !w.get( 0 ).getClass().isArray()) ) {
            if ( !(a.get( 0 ) instanceof Number) || !(b.get( 0 ) instanceof Number) || (w != null && !(w.get( 0 ) instanceof Number)) ) {
                throw new RuntimeException( "Inputs are not Numbers." );
            }
        } else {
            throw new RuntimeException( "Not useable Arrays, ask jan." );
        }
    }

}
