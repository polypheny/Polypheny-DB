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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Objects;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetPrimitivePredicate;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class ParquetPrimitivePredicateTest {

    @BeforeAll
    static void initHomeDir() {
        try {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        } catch ( Exception e ) {
            // Already initialized by another test.
        }
    }


    @Test
    void compileReturnsAlwaysTrueForEmptyFiltersAndRejectsUnsupportedFilters() {
        MessageType schema = schema();

        assertTrue( Objects.requireNonNull( ParquetPrimitivePredicate.compile( schema, List.of() ) ).matches( new ColumnReader[0], new boolean[0], new Object[0] ) );
        assertTrue( Objects.requireNonNull( ParquetPrimitivePredicate.compile( schema, null ) ).matches( new ColumnReader[0], new boolean[0], new Object[0] ) );
        assertNull( ParquetPrimitivePredicate.compile( schema, List.of( new ParquetAdapterFilter<>( 99, Kind.EQUALS, PolyInteger.of( 1 ) ) ) ) );
        assertNull( ParquetPrimitivePredicate.compile( schema, List.of( new ParquetAdapterFilter<>( 0, Kind.PLUS, PolyInteger.of( 1 ) ) ) ) );
        assertNull( ParquetPrimitivePredicate.compile( schema, List.of( new ParquetAdapterFilter<>( 0, Kind.EQUALS, null ) ) ) );
        assertNull( ParquetPrimitivePredicate.compile( schema, List.of( ParquetAdapterFilter.logical( Kind.OR, List.of(
                new ParquetAdapterFilter<>( 0, Kind.EQUALS, PolyInteger.of( 1 ) ),
                new ParquetAdapterFilter<>( 0, Kind.EQUALS, PolyInteger.of( 2 ) ) ) ) ) ) );
    }


    @Test
    void compiledAndPredicateReadsColumnsOnceAndReusesConsumedValues() {
        MessageType schema = schema();
        ParquetPrimitivePredicate predicate = ParquetPrimitivePredicate.compile( schema, List.of(
                new ParquetAdapterFilter<>( 0, Kind.GREATER_THAN, PolyInteger.of( 5 ) ),
                new ParquetAdapterFilter<>( 1, Kind.EQUALS, PolyString.of( "alpha" ) ),
                new ParquetAdapterFilter<>( 2, Kind.EQUALS, PolyBoolean.of( true ) ) ) );
        TestColumnReader amount = TestColumnReader.intReader( 1, 9 );
        TestColumnReader code = TestColumnReader.binaryReader( 1, "alpha" );
        TestColumnReader active = TestColumnReader.booleanReader( 1, true );
        ColumnReader[] readers = { amount, code, active };
        boolean[] consumed = new boolean[3];
        Object[] values = new Object[3];

        assertTrue( Objects.requireNonNull( predicate ).matches( readers, consumed, values ) );
        assertEquals( 1, amount.consumed );
        assertEquals( 1, code.consumed );
        assertEquals( 1, active.consumed );

        assertTrue( predicate.matches( readers, consumed, values ) );
        assertEquals( 1, amount.consumed );
        assertEquals( 1, code.consumed );
        assertEquals( 1, active.consumed );
        assertEquals( 9, values[0] );
        assertEquals( "alpha", ((Binary) values[1]).toStringUsingUTF8() );
        assertEquals( true, values[2] );
    }


    @Test
    void nullPredicatesMatchAbsentAndPresentValues() {
        MessageType schema = schema();
        ParquetPrimitivePredicate isNull = ParquetPrimitivePredicate.compile( schema, List.of( new ParquetAdapterFilter<>( 0, Kind.IS_NULL, null ) ) );
        ParquetPrimitivePredicate isNotNull = ParquetPrimitivePredicate.compile( schema, List.of( new ParquetAdapterFilter<>( 0, Kind.IS_NOT_NULL, null ) ) );

        assertTrue( Objects.requireNonNull( isNull ).matches( new ColumnReader[]{ TestColumnReader.intReader( 0, 0 ) }, new boolean[1], new Object[1] ) );
        assertTrue( Objects.requireNonNull( isNotNull ).matches( new ColumnReader[]{ TestColumnReader.intReader( 1, 7 ) }, new boolean[1], new Object[1] ) );
        assertFalse( isNotNull.matches( new ColumnReader[]{ TestColumnReader.intReader( 0, 0 ) }, new boolean[1], new Object[1] ) );
    }


    private static MessageType schema() {
        return Types.buildMessage()
                .optional( PrimitiveTypeName.INT32 ).named( "amount" )
                .optional( PrimitiveTypeName.BINARY ).named( "code" )
                .optional( PrimitiveTypeName.BOOLEAN ).named( "active" )
                .named( "predicate_schema" );
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


        private static TestColumnReader binaryReader( int definitionLevel, String value ) {
            return new TestColumnReader( PrimitiveTypeName.BINARY, definitionLevel, Binary.fromString( value ) );
        }


        private static TestColumnReader booleanReader( int definitionLevel, boolean value ) {
            return new TestColumnReader( PrimitiveTypeName.BOOLEAN, definitionLevel, value );
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
            return ((Number) value).longValue();
        }


        @Override
        public Binary getBinary() {
            return (Binary) value;
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
