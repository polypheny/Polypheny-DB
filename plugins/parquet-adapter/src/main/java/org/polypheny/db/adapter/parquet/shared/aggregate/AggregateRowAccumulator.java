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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;

/**
 * Applies all aggregate calls for one input row into an AggregateGroupState.
 */
public class AggregateRowAccumulator {

    private static final int NO_INDEX = -1;

    private final int[] countStarIndexes;
    private final int singleCountStarIndex;
    private final AggregateInputColumn[] columns;


    private AggregateRowAccumulator( int[] countStarIndexes, AggregateInputColumn[] columns ) {
        this.singleCountStarIndex = firstIndex( countStarIndexes );
        this.countStarIndexes = remainingIndexes( countStarIndexes );
        this.columns = columns;
    }


    public static AggregateRowAccumulator build( AggregateCallDescriptor[] aggregateCalls, ColumnDescriptor[] columns ) {
        List<Integer> countStarIndexes = new ArrayList<>();
        Map<Integer, AggregateInputColumnBuilder> builders = new LinkedHashMap<>();
        for ( int i = 0; i < aggregateCalls.length; i++ ) {
            AggregateCallDescriptor aggregateCall = aggregateCalls[i];
            if ( aggregateCall.argumentIndex() == AggregateCallDescriptor.NO_ARGUMENT ) {
                if ( aggregateCall.kind() == AggregateCallDescriptor.Kind.COUNT ) {
                    countStarIndexes.add( i );
                }
                continue;
            }

            AggregateInputColumnBuilder builder = builders.computeIfAbsent( aggregateCall.argumentIndex(), AggregateInputColumnBuilder::new );
            switch ( aggregateCall.kind() ) {
                case COUNT -> builder.countIndexes.add( i );
                case SUM -> builder.doubleIndexes.add( i );
                case MIN -> builder.minIndexes.add( i );
                case MAX -> builder.maxIndexes.add( i );
            }
        }

        AggregateInputColumn[] columnAccumulators = builders.values().stream()
                .map( b -> b.build( columns ) )
                .toArray( AggregateInputColumn[]::new );

        return new AggregateRowAccumulator( countStarIndexes.stream().mapToInt( Integer::intValue ).toArray(), columnAccumulators );
    }


    public void add( AggregateGroupState values, ColumnReader[] readers ) {
        addCountStars( values );
        for ( AggregateInputColumn column : columns ) {
            column.add( values, readers );
        }
    }


    public void add( AggregateGroupState values, ColumnReader[] readers, boolean[] present, double[] numericValues ) {
        addCountStars( values );
        for ( AggregateInputColumn column : columns ) {
            column.add( values, readers, present, numericValues );
        }
    }


    public void add( AggregateGroupState values, ColumnReader[] readers, boolean[] consumed, boolean[] present, double[] numericValues ) {
        addCountStars( values );
        for ( AggregateInputColumn column : columns ) {
            column.add( values, readers, consumed, present, numericValues );
        }
    }


    private void addCountStars( AggregateGroupState values ) {
        if ( singleCountStarIndex != NO_INDEX ) {
            values.addCount( singleCountStarIndex, 1 );
        }
        for ( int countStarIndex : countStarIndexes ) {
            values.addCount( countStarIndex, 1 );
        }
    }


    private static int firstIndex( int[] indexes ) {
        return indexes.length == 0 ? NO_INDEX : indexes[0];
    }


    private static int[] remainingIndexes( int[] indexes ) {
        if ( indexes.length <= 1 ) {
            return new int[0];
        }
        int[] remaining = new int[indexes.length - 1];
        System.arraycopy( indexes, 1, remaining, 0, remaining.length );
        return remaining;
    }


    static int singleIndex( int[] indexes ) {
        return firstIndex( indexes );
    }


    static int[] additionalIndexes( int[] indexes ) {
        return remainingIndexes( indexes );
    }


    static boolean hasIndex( int index ) {
        return index != NO_INDEX;
    }


    @SuppressWarnings("SameParameterValue")
    static void addIndexes( AggregateGroupState values, int singleIndex, int[] indexes, long rows ) {
        if ( hasIndex( singleIndex ) ) {
            values.addCount( singleIndex, rows );
        }
        for ( int index : indexes ) {
            values.addCount( index, rows );
        }
    }


    static void addDoubleIndexes( AggregateGroupState values, int singleIndex, int[] indexes, double value ) {
        if ( hasIndex( singleIndex ) ) {
            values.addDouble( singleIndex, value );
        }
        for ( int index : indexes ) {
            values.addDouble( index, value );
        }
    }


    static void addMinIndexes( AggregateGroupState values, int singleIndex, int[] indexes, double value ) {
        if ( hasIndex( singleIndex ) ) {
            values.addMin( singleIndex, value );
        }
        for ( int index : indexes ) {
            values.addMin( index, value );
        }
    }


    static void addMaxIndexes( AggregateGroupState values, int singleIndex, int[] indexes, double value ) {
        if ( hasIndex( singleIndex ) ) {
            values.addMax( singleIndex, value );
        }
        for ( int index : indexes ) {
            values.addMax( index, value );
        }
    }


    private static class AggregateInputColumnBuilder {

        private final int columnIndex;
        private final List<Integer> countIndexes = new ArrayList<>();
        private final List<Integer> doubleIndexes = new ArrayList<>();
        private final List<Integer> minIndexes = new ArrayList<>();
        private final List<Integer> maxIndexes = new ArrayList<>();


        private AggregateInputColumnBuilder( int columnIndex ) {
            this.columnIndex = columnIndex;
        }


        private AggregateInputColumn build( ColumnDescriptor[] columns ) {
            return new AggregateInputColumn(
                    columnIndex,
                    countIndexes.stream().mapToInt( Integer::intValue ).toArray(),
                    doubleIndexes.stream().mapToInt( Integer::intValue ).toArray(),
                    minIndexes.stream().mapToInt( Integer::intValue ).toArray(),
                    maxIndexes.stream().mapToInt( Integer::intValue ).toArray(),
                    columns[columnIndex].getMaxDefinitionLevel(),
                    columns[columnIndex].getPrimitiveType().getPrimitiveTypeName()
            );
        }

    }

}
