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

package org.polypheny.db.prisminterface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.prisminterface.streaming.Estimate;
import org.polypheny.db.prisminterface.streaming.SerializationHeuristic;
import org.polypheny.db.prisminterface.streaming.StreamingFramework;
import org.polypheny.db.prisminterface.streaming.StreamIndex;
import org.polypheny.db.prisminterface.streaming.StreamingStrategy;
import org.polypheny.db.prisminterface.utils.PolyValueSerializer;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.prism.ProtoValue;

public class SerializationHeuristicTest {

    @BeforeAll
    public static void init() {
        // needed to launch polypheny
        TestHelper.getInstance();
    }


    @Test
    public void polyBinaryHeuristicTestShort() {
        PolyBinary value = PolyBinary.of( new byte[]{ 0b01111111, 0b01111111, 0b01111111 } );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertTrue( estimatedSize.getAllStreamedLength() >= estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyBinaryHeuristicTestLong() {
        byte[] data = new byte[5000];
        for ( int i = 0; i < 5000; i++ ) {
            data[i] = 0b01111111;
        }
        PolyBinary value = PolyBinary.of( data );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertTrue( estimatedSize.getAllStreamedLength() < estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyBinaryHeuristicTestStreaming() {
        byte[] data = new byte[100000001];
        for ( int i = 0; i < 100000001; i++ ) {
            data[i] = 0b01111111;
        }
        PolyBinary value = PolyBinary.of( data );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertTrue( estimatedSize.getAllStreamedLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyBinaryHeuristicTestStreamLimit() {
        assertEquals( StreamingFramework.STREAM_LIMIT, 100000000 );
    }


    @Test
    public void polyBooleanHeuristicTestTrue() {
        PolyBoolean value = new PolyBoolean( true );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyIntegerHeuristicTestSmallValue() {
        PolyInteger value = new PolyInteger( 8 );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
        assertEquals( 8, estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyIntegerHeuristicTestLargeValue() {
        PolyInteger value = new PolyInteger( Integer.MAX_VALUE );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
        assertEquals( 8, estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyLongHeuristicTestSmallValue() {
        PolyLong value = new PolyLong( 4L );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
        assertEquals( 12, estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyLongHeuristicTestLargeValue() {
        PolyLong value = new PolyLong( Long.MAX_VALUE );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
        assertEquals( 12, estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyFloatHeuristicTestLargeValue() {
        PolyFloat value = new PolyFloat( Float.MAX_VALUE );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
        assertEquals( 7, estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyDoubleHeuristicTest() {
        PolyDouble value = new PolyDouble( Double.MAX_VALUE );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertTrue( estimatedSize.getAllStreamedLength() <= estimatedSize.getDynamicLength() );
        assertEquals( 11, estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyStringHeuristicTestShort() {
        PolyString value = new PolyString( "sample" );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyStringHeuristicTestLong() {
        PolyString value = new PolyString( "a".repeat( 500000000 ) );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyBigDecimalHeuristicTestSmallValue() {
        PolyBigDecimal value = new PolyBigDecimal( new BigDecimal( 1L ) );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyBigDecimalHeuristicTestLargeValue() {
        byte[] unscaledValueBytes = new byte[16];
        Arrays.fill( unscaledValueBytes, (byte) 0b0111111 );
        BigInteger unscaledValue = new BigInteger( unscaledValueBytes );
        int scale = Integer.MAX_VALUE;
        BigDecimal bigDecimal = new BigDecimal( unscaledValue, scale );
        PolyBigDecimal value = new PolyBigDecimal( bigDecimal );

        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertTrue( estimatedSize.getAllStreamedLength() <= estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyTimestampHeuristicTestLargeValue() {
        PolyTimestamp value = new PolyTimestamp( Long.MAX_VALUE );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
        assertEquals( 12, estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyimestampHeuristicHeuristicTestSmallValue() {
        PolyLong value = new PolyLong( 4L );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
        assertEquals( 12, estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyDateHeuristicTestSmallValue() {
        PolyDate value = PolyDate.ofDays( 1 );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertTrue( estimatedSize.getAllStreamedLength() <= estimatedSize.getDynamicLength() );
        assertEquals( 12, estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyDateHeuristicTestLargeValue() {
        PolyDate value = PolyDate.ofDays( Integer.MAX_VALUE );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertTrue( estimatedSize.getAllStreamedLength() <= estimatedSize.getDynamicLength() );
        assertEquals( 12, estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyTimeHeuristicTestSmallValue() {
        PolyTime value = PolyTime.of( 1 );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertTrue( estimatedSize.getAllStreamedLength() <= estimatedSize.getDynamicLength() );
        assertEquals( 8, estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyTimeHeuristicTestLargeValue() {
        PolyTime value = PolyTime.of( Integer.MAX_VALUE );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertTrue( estimatedSize.getAllStreamedLength() <= estimatedSize.getDynamicLength() );
        assertEquals( 8, estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyNullHeuristicTestNull() {
        PolyNull value = new PolyNull();
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
        assertEquals( 2, estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyListHeuristicTestNull() {
        PolyList<PolyNull> value = new PolyList<>(
                new PolyNull(),
                new PolyNull(),
                new PolyNull(),
                new PolyNull()
        );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        System.out.println( estimatedSize.getDynamicLength() );
        System.out.println( estimatedSize.getAllStreamedLength() );
        System.out.println( actualSize );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyListHeuristicTestInt() {
        PolyList<PolyInteger> value = new PolyList<>(
                new PolyInteger( Integer.MAX_VALUE ),
                new PolyInteger( Integer.MAX_VALUE ),
                new PolyInteger( Integer.MAX_VALUE ),
                new PolyInteger( Integer.MAX_VALUE )
        );
        ProtoValue protoValue = PolyValueSerializer.serialize( value, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( value );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertEquals( estimatedSize.getAllStreamedLength(), estimatedSize.getDynamicLength() );
    }


    @Test
    public void polyDocumentHeuristicTest() {
        PolyDocument document = new PolyDocument();
        document.put( new PolyString( "1st" ), new PolyInteger( Integer.MAX_VALUE ) );
        document.put( new PolyString( "2nd" ), new PolyInteger( Integer.MAX_VALUE ) );
        document.put( new PolyString( "3rd" ), new PolyInteger( Integer.MAX_VALUE ) );
        document.put( new PolyString( "4th" ), new PolyInteger( Integer.MAX_VALUE ) );
        document.put( new PolyString( "5th" ), new PolyInteger( Integer.MAX_VALUE ) );
        document.put( new PolyString( "6th" ), new PolyInteger( Integer.MAX_VALUE ) );
        document.put( new PolyString( "7th" ), new PolyInteger( Integer.MAX_VALUE ) );
        document.put( new PolyString( "8th" ), new PolyInteger( Integer.MAX_VALUE ) );
        ProtoValue protoValue = PolyValueSerializer.serialize( document, new StreamIndex(), StreamingStrategy.DYNAMIC );
        int actualSize = protoValue.toByteArray().length;
        Estimate estimatedSize = SerializationHeuristic.estimateSize( document );
        System.out.println( estimatedSize.getDynamicLength() );
        System.out.println( estimatedSize.getAllStreamedLength() );
        System.out.println( actualSize );
        assertTrue( estimatedSize.getDynamicLength() >= actualSize );
        assertTrue( estimatedSize.getAllStreamedLength() <= estimatedSize.getDynamicLength() );
    }

}
