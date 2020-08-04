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

package org.polypheny.db.runtime;


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.util.Pair;


public class KnnFunctions {

    protected static double l1Metric( List<Pair<Number, Number>> input ) {
        return input.stream().mapToDouble( p -> Math.abs( p.left.doubleValue() - p.right.doubleValue() ) ).sum();
    }


    protected static double l1MetricWeighted( List<Pair<Number, Number>> input, List<Number> weights ) {
        List<Pair<Pair<Number, Number>, Number>> temp = Pair.zip( input, weights );
        return temp.stream().mapToDouble( p -> Math.abs( p.left.left.doubleValue() - p.left.right.doubleValue() ) * p.right.doubleValue() ).sum();
    }


    protected static double l2SquaredMetric( List<Pair<Number, Number>> input ) {
        return input.stream().mapToDouble( p -> Math.pow(p.left.doubleValue() - p.right.doubleValue(), 2.0) ).sum();
    }


    protected static double l2Metric( List<Pair<Number, Number>> input ) {
        return Math.sqrt( l2SquaredMetric( input ) );
    }


    protected static double l2SquaredMetricWeighted( List<Pair<Number, Number>> input, List<Number> weights ) {
        List<Pair<Pair<Number, Number>, Number>> temp = Pair.zip( input, weights );
        return temp.stream().mapToDouble(
                p -> Math.pow(p.left.left.doubleValue() - p.left.right.doubleValue(), 2.0) * p.right.doubleValue()
        ).sum();
    }


    protected static double l2MetricWeighted( List<Pair<Number, Number>> input, List<Number> weights ) {
        return Math.sqrt( l2SquaredMetricWeighted( input, weights ) );
    }


    protected static List<Pair<Number, Number>> makeNumberPairList( List a, List b ) {
        if ( a.isEmpty() || b.isEmpty() ) {
            return new ArrayList<>();
        }

        if ( !a.get( 0 ).getClass().isArray() || !b.get( 0 ).getClass().isArray() ) {
            if ( !(a.get( 0 ) instanceof Number) || !(b.get( 0 ) instanceof Number)  ) {
                throw new RuntimeException( " not numbers. ask jan. ");
            }

            return Pair.zip( a, b );
        } else {
            throw new RuntimeException( "Not useable Arrays, ask jan. " );
        }
    }
}
