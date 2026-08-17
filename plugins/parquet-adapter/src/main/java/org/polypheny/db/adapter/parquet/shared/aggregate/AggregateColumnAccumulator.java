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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;

/**
 * It reads one Parquet column and accumulates count/sum/min/max. This is the real column-at-a-time accumulator.
 */
public abstract class AggregateColumnAccumulator {

    protected static final int CHECK_INTERVAL = 4096;

    protected final ColumnReader reader;
    protected final long rowCount;
    protected final int maxDefinitionLevel;
    protected final AtomicBoolean cancelFlag;
    protected double sum;
    protected double min = Double.POSITIVE_INFINITY;
    protected double max = Double.NEGATIVE_INFINITY;
    protected long count;


    protected AggregateColumnAccumulator( ColumnReader reader, long rowCount, int maxDefinitionLevel, AtomicBoolean cancelFlag ) {
        this.reader = reader;
        this.rowCount = rowCount;
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.cancelFlag = cancelFlag;
    }


    public static AggregateColumnAccumulator create( ColumnReader reader, ColumnDescriptor descriptor, long rowCount, AtomicBoolean cancelFlag ) {
        return switch ( descriptor.getPrimitiveType().getPrimitiveTypeName() ) {
            case INT32 -> new IntAggregateColumnAccumulator( reader, rowCount, descriptor.getMaxDefinitionLevel(), cancelFlag );
            case INT64 -> new LongAggregateColumnAccumulator( reader, rowCount, descriptor.getMaxDefinitionLevel(), cancelFlag );
            case FLOAT -> new FloatAggregateColumnAccumulator( reader, rowCount, descriptor.getMaxDefinitionLevel(), cancelFlag );
            case DOUBLE -> new DoubleAggregateColumnAccumulator( reader, rowCount, descriptor.getMaxDefinitionLevel(), cancelFlag );
            case BOOLEAN, BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> throw new UnsupportedOperationException( "Unsupported numeric primitive type: " + descriptor.getPrimitiveType().getPrimitiveTypeName() );
        };
    }


    public double sum() {
        return sum;
    }


    public long count() {
        return count;
    }


    public ColumnAggregateResult result() {
        return new ColumnAggregateResult( count, sum, min, max );
    }


    public abstract void read();


    protected void addValue( double value ) {
        sum += value;
        if ( count == 0 || value < min ) {
            min = value;
        }
        if ( count == 0 || value > max ) {
            max = value;
        }
        count++;
    }


    private static class IntAggregateColumnAccumulator extends AggregateColumnAccumulator {

        public IntAggregateColumnAccumulator( ColumnReader reader, long rowCount, int maxDefinitionLevel, AtomicBoolean cancelFlag ) {
            super( reader, rowCount, maxDefinitionLevel, cancelFlag );
        }


        @Override
        public void read() {
            long row = 0;

            while ( row < rowCount ) {
                if ( cancelFlag.get() ) {
                    return;
                }

                long end = Math.min( row + CHECK_INTERVAL, rowCount );
                for ( ; row < end; row++ ) {
                    if ( maxDefinitionLevel == 0 || reader.getCurrentDefinitionLevel() == maxDefinitionLevel ) {
                        addValue( reader.getInteger() );
                    }
                    reader.consume();
                }
            }
        }

    }


    private static class LongAggregateColumnAccumulator extends AggregateColumnAccumulator {

        public LongAggregateColumnAccumulator( ColumnReader reader, long rowCount, int maxDefinitionLevel, AtomicBoolean cancelFlag ) {
            super( reader, rowCount, maxDefinitionLevel, cancelFlag );
        }


        @Override
        public void read() {
            long row = 0;

            while ( row < rowCount ) {
                if ( cancelFlag.get() ) {
                    return;
                }

                long end = Math.min( row + CHECK_INTERVAL, rowCount );
                for ( ; row < end; row++ ) {
                    if ( maxDefinitionLevel == 0 || reader.getCurrentDefinitionLevel() == maxDefinitionLevel ) {
                        addValue( reader.getLong() );
                    }
                    reader.consume();
                }
            }
        }

    }


    private static class FloatAggregateColumnAccumulator extends AggregateColumnAccumulator {

        public FloatAggregateColumnAccumulator( ColumnReader reader, long rowCount, int maxDefinitionLevel, AtomicBoolean cancelFlag ) {
            super( reader, rowCount, maxDefinitionLevel, cancelFlag );
        }


        @Override
        public void read() {
            long row = 0;

            while ( row < rowCount ) {
                if ( cancelFlag.get() ) {
                    return;
                }

                long end = Math.min( row + CHECK_INTERVAL, rowCount );
                for ( ; row < end; row++ ) {
                    if ( maxDefinitionLevel == 0 || reader.getCurrentDefinitionLevel() == maxDefinitionLevel ) {
                        addValue( reader.getFloat() );
                    }
                    reader.consume();
                }
            }
        }

    }


    private static class DoubleAggregateColumnAccumulator extends AggregateColumnAccumulator {

        public DoubleAggregateColumnAccumulator( ColumnReader reader, long rowCount, int maxDefinitionLevel, AtomicBoolean cancelFlag ) {
            super( reader, rowCount, maxDefinitionLevel, cancelFlag );
        }


        @Override
        public void read() {
            long row = 0;

            while ( row < rowCount ) {
                if ( cancelFlag.get() ) {
                    return;
                }

                long end = Math.min( row + CHECK_INTERVAL, rowCount );
                for ( ; row < end; row++ ) {
                    if ( maxDefinitionLevel == 0 || reader.getCurrentDefinitionLevel() == maxDefinitionLevel ) {
                        addValue( reader.getDouble() );
                    }
                    reader.consume();
                }
            }
        }

    }

}
