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

package org.polypheny.db.adapter.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateCallDescriptor;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateGroupState;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateInputColumn;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateRowAccumulator;
import org.polypheny.db.type.entity.PolyNull;

class AggregateAccumulatorTest {

    @Test
    void aggregateInputColumnAddsPresentNumericValuesAndSkipsNulls() {
        AggregateCallDescriptor[] calls = calls();
        AggregateInputColumn column = new AggregateInputColumn( 0, new int[]{ 0, 1 }, new int[]{ 2 }, new int[]{ 3 }, new int[]{ 4 }, 1, PrimitiveTypeName.INT64 );
        AggregateGroupState values = new AggregateGroupState( calls );
        TestColumnReader reader = TestColumnReader.longReader( 1, 7L );

        column.add( values, new ColumnReader[]{ reader } );

        assertEquals( 1L, values.result( 0 ).asNumber().longValue() );
        assertEquals( 1L, values.result( 1 ).asNumber().longValue() );
        assertEquals( 7D, values.result( 2 ).asNumber().doubleValue() );
        assertEquals( 7D, values.result( 3 ).asNumber().doubleValue() );
        assertEquals( 7D, values.result( 4 ).asNumber().doubleValue() );
        assertEquals( 1, reader.consumed );

        AggregateGroupState nullValues = new AggregateGroupState( calls );
        TestColumnReader nullReader = TestColumnReader.longReader( 0, 99L );
        column.add( nullValues, new ColumnReader[]{ nullReader } );

        assertEquals( 0L, nullValues.result( 0 ).asNumber().longValue() );
        assertTrue( nullValues.result( 2 ).isNull() );
        assertEquals( 1, nullReader.consumed );
    }


    @Test
    void aggregateInputColumnSharesConsumedValuesAcrossCalls() {
        AggregateCallDescriptor[] calls = calls();
        AggregateInputColumn column = new AggregateInputColumn( 0, new int[]{ 0 }, new int[]{ 2 }, new int[0], new int[0], 1, PrimitiveTypeName.DOUBLE );
        AggregateGroupState values = new AggregateGroupState( calls );
        TestColumnReader reader = TestColumnReader.doubleReader( 1, 42D );
        boolean[] consumed = new boolean[1];
        boolean[] present = new boolean[1];
        double[] numericValues = new double[1];

        column.add( values, new ColumnReader[]{ reader }, consumed, present, numericValues );
        column.add( values, new ColumnReader[]{ reader }, consumed, present, numericValues );

        assertEquals( 1, reader.consumed );
        assertEquals( 2L, values.result( 0 ).asNumber().longValue() );
        assertEquals( 84D, values.result( 2 ).asNumber().doubleValue() );
    }


    @Test
    void aggregateRowAccumulatorBuildsCountStarAndColumnAggregates() {
        AggregateCallDescriptor[] calls = new AggregateCallDescriptor[]{
                AggregateCallDescriptor.countStar(),
                new AggregateCallDescriptor( AggregateCallDescriptor.Kind.COUNT, 0 ),
                new AggregateCallDescriptor( AggregateCallDescriptor.Kind.SUM, 0 ),
                new AggregateCallDescriptor( AggregateCallDescriptor.Kind.MIN, 0 ),
                new AggregateCallDescriptor( AggregateCallDescriptor.Kind.MAX, 0 )
        };
        @SuppressWarnings("deprecation") ColumnDescriptor[] descriptors = new ColumnDescriptor[]{
                new ColumnDescriptor( new String[]{ "amount" }, PrimitiveTypeName.INT32, 0, 1 )
        };
        AggregateRowAccumulator accumulator = AggregateRowAccumulator.build( calls, descriptors );
        AggregateGroupState values = new AggregateGroupState( calls );

        accumulator.add( values, new ColumnReader[]{ TestColumnReader.intReader( 1, 5 ) } );

        assertEquals( 1L, values.result( 0 ).asNumber().longValue() );
        assertEquals( 1L, values.result( 1 ).asNumber().longValue() );
        assertEquals( 5D, values.result( 2 ).asNumber().doubleValue() );
        assertEquals( 5D, values.result( 3 ).asNumber().doubleValue() );
        assertEquals( 5D, values.result( 4 ).asNumber().doubleValue() );
    }


    @Test
    void aggregateInputColumnRejectsUnsupportedPrimitiveTypes() {
        AggregateInputColumn column = new AggregateInputColumn( 0, new int[0], new int[]{ 0 }, new int[0], new int[0], 1, PrimitiveTypeName.BOOLEAN );

        assertThrows( IllegalArgumentException.class, () -> column.add(
                new AggregateGroupState( new AggregateCallDescriptor[]{ new AggregateCallDescriptor( AggregateCallDescriptor.Kind.SUM, 0 ) } ),
                new ColumnReader[]{ TestColumnReader.booleanReader( true ) } ) );
    }


    @Test
    void aggregateGroupStateHandlesObjectRowsAndMergesPartialStates() {
        AggregateCallDescriptor[] calls = calls();
        AggregateGroupState left = new AggregateGroupState( calls );
        AggregateGroupState right = new AggregateGroupState( calls );

        left.add( new Object[]{ 3 } );
        left.add( new Object[]{ PolyNull.NULL } );
        right.add( new Object[]{ 9 } );
        left.merge( right );

        assertEquals( 3L, left.result( 0 ).asNumber().longValue() );
        assertEquals( 2L, left.result( 1 ).asNumber().longValue() );
        assertEquals( 12D, left.result( 2 ).asNumber().doubleValue() );
        assertEquals( 3D, left.result( 3 ).asNumber().doubleValue() );
        assertEquals( 9D, left.result( 4 ).asNumber().doubleValue() );
    }


    private static AggregateCallDescriptor[] calls() {
        return new AggregateCallDescriptor[]{
                AggregateCallDescriptor.countStar(),
                new AggregateCallDescriptor( AggregateCallDescriptor.Kind.COUNT, 0 ),
                new AggregateCallDescriptor( AggregateCallDescriptor.Kind.SUM, 0 ),
                new AggregateCallDescriptor( AggregateCallDescriptor.Kind.MIN, 0 ),
                new AggregateCallDescriptor( AggregateCallDescriptor.Kind.MAX, 0 )
        };
    }


    private static class TestColumnReader implements ColumnReader {

        private final PrimitiveTypeName type;
        private final int definitionLevel;
        private final Object value;
        private int consumed;


        private TestColumnReader( PrimitiveTypeName type, int definitionLevel, Object value ) {
            this.type = type;
            this.definitionLevel = definitionLevel;
            this.value = value;
        }


        private static TestColumnReader intReader( int definitionLevel, int value ) {
            return new TestColumnReader( PrimitiveTypeName.INT32, definitionLevel, value );
        }


        private static TestColumnReader longReader( int definitionLevel, long value ) {
            return new TestColumnReader( PrimitiveTypeName.INT64, definitionLevel, value );
        }


        private static TestColumnReader doubleReader( int definitionLevel, double value ) {
            return new TestColumnReader( PrimitiveTypeName.DOUBLE, definitionLevel, value );
        }


        private static TestColumnReader booleanReader( boolean value ) {
            return new TestColumnReader( PrimitiveTypeName.BOOLEAN, 1, value );
        }


        @Override
        public long getTotalValueCount() {
            return 1;
        }


        @Override
        public void consume() {
            consumed++;
        }


        @Override
        public int getCurrentRepetitionLevel() {
            return 0;
        }


        @Override
        public int getCurrentDefinitionLevel() {
            return definitionLevel;
        }


        @Override
        public void writeCurrentValueToConverter() {
        }


        @Override
        public void skip() {
        }


        @Override
        public int getCurrentValueDictionaryID() {
            return 0;
        }


        @Override
        public int getInteger() {
            return (Integer) value;
        }


        @Override
        public boolean getBoolean() {
            return (Boolean) value;
        }


        @Override
        public long getLong() {
            return (Long) value;
        }


        @Override
        public Binary getBinary() {
            return Binary.fromString( String.valueOf( value ) );
        }


        @Override
        public float getFloat() {
            return ((Number) value).floatValue();
        }


        @Override
        public double getDouble() {
            return ((Number) value).doubleValue();
        }


        @Override
        public ColumnDescriptor getDescriptor() {
            //noinspection deprecation
            return new ColumnDescriptor( new String[]{ "value" }, type, 0, 1 );
        }

    }

}
