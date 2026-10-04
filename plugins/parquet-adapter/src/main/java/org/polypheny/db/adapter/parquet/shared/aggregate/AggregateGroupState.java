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

package org.polypheny.db.adapter.parquet.shared.aggregate;

import java.util.Arrays;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyLong;


/**
 * A container for the aggregated values.
 */
public class AggregateGroupState {

    private final AggregateCallDescriptor[] aggregateCalls;
    private final double[] sums;
    private final double[] mins;
    private final double[] maxes;
    private final long[] counts;


    public AggregateGroupState( AggregateCallDescriptor[] aggregateCalls ) {
        this.aggregateCalls = aggregateCalls;
        this.sums = new double[aggregateCalls.length];
        this.mins = new double[aggregateCalls.length];
        this.maxes = new double[aggregateCalls.length];
        this.counts = new long[aggregateCalls.length];
        Arrays.fill( this.mins, Double.POSITIVE_INFINITY );
        Arrays.fill( this.maxes, Double.NEGATIVE_INFINITY );
    }


    /**
     * Increments a star counter by the provided number.
     *
     * @param rowCount a row count.
     */
    public void increment( long rowCount ) {
        for ( int i = 0; i < aggregateCalls.length; i++ ) {
            AggregateCallDescriptor aggregateCall = aggregateCalls[i];
            if ( aggregateCall.kind() == AggregateCallDescriptor.Kind.COUNT && aggregateCall.argumentIndex() == AggregateCallDescriptor.NO_ARGUMENT ) {
                counts[i] += rowCount;
            }
        }
    }


    public void add( Object[] row ) {
        for ( int i = 0; i < aggregateCalls.length; i++ ) {
            AggregateCallDescriptor aggregateCall = aggregateCalls[i];
            if ( aggregateCall.argumentIndex() == AggregateCallDescriptor.NO_ARGUMENT ) {
                if ( aggregateCall.kind() == AggregateCallDescriptor.Kind.COUNT ) {
                    counts[i]++;
                }
                continue;
            }
            Object value = row[aggregateCall.argumentIndex()];
            if ( isNull( value ) ) {
                continue;
            }
            if ( aggregateCall.kind() == AggregateCallDescriptor.Kind.COUNT ) {
                counts[i]++;
                continue;
            }
            addValue( i, doubleValue( value ) );
        }
    }


    public void addCount( int aggregateIndex, long rows ) {
        counts[aggregateIndex] += rows;
    }


    public void addDouble( int aggregateIndex, double value ) {
        sums[aggregateIndex] += value;
        counts[aggregateIndex]++;
    }


    public void addSum( int aggregateIndex, double value ) {
        sums[aggregateIndex] += value;
    }


    public void addMin( int aggregateIndex, double value ) {
        if ( counts[aggregateIndex] == 0 || value < mins[aggregateIndex] ) {
            mins[aggregateIndex] = value;
        }
        counts[aggregateIndex]++;
    }


    public void addMax( int aggregateIndex, double value ) {
        if ( counts[aggregateIndex] == 0 || value > maxes[aggregateIndex] ) {
            maxes[aggregateIndex] = value;
        }
        counts[aggregateIndex]++;
    }


    public void merge( AggregateGroupState values ) {
        for ( int i = 0; i < aggregateCalls.length; i++ ) {
            AggregateCallDescriptor.Kind kind = aggregateCalls[i].kind();
            if ( values.counts[i] > 0 ) {
                if ( kind == AggregateCallDescriptor.Kind.MIN && (counts[i] == 0 || values.mins[i] < mins[i]) ) {
                    mins[i] = values.mins[i];
                } else if ( kind == AggregateCallDescriptor.Kind.MAX && (counts[i] == 0 || values.maxes[i] > maxes[i]) ) {
                    maxes[i] = values.maxes[i];
                }
            }
            sums[i] += values.sums[i];
            counts[i] += values.counts[i];
        }
    }


    public long count( int index ) {
        return counts[index];
    }


    public PolyValue result( int index ) {
        AggregateCallDescriptor.Kind kind = aggregateCalls[index].kind();
        if ( kind == AggregateCallDescriptor.Kind.COUNT ) {
            return PolyLong.of( counts[index] );
        }
        if ( counts[index] == 0 ) {
            return PolyNull.NULL;
        }
        if ( kind == AggregateCallDescriptor.Kind.MIN ) {
            return PolyDouble.of( mins[index] );
        }
        if ( kind == AggregateCallDescriptor.Kind.MAX ) {
            return PolyDouble.of( maxes[index] );
        }
        return PolyDouble.of( sums[index] );
    }


    private void addValue( int aggregateIndex, double value ) {
        switch ( aggregateCalls[aggregateIndex].kind() ) {
            case MIN -> addMin( aggregateIndex, value );
            case MAX -> addMax( aggregateIndex, value );
            default -> addDouble( aggregateIndex, value );
        }
    }


    private boolean isNull( Object value ) {
        return value == null || value instanceof PolyValue polyValue && polyValue.isNull();
    }


    private double doubleValue( Object value ) {
        if ( value instanceof PolyValue polyValue ) {
            PolyNumber number = polyValue.asNumber();
            return number.doubleValue();
        }
        return ((Number) value).doubleValue();
    }

}
