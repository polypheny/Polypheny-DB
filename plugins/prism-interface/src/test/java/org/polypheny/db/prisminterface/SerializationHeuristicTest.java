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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.prisminterface.streaming.StreamingIndex;
import org.polypheny.db.prisminterface.utils.PolyValueSerializer;
import org.polypheny.db.prisminterface.streaming.SerializationHeuristic;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
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
    public void polyBigDecimalHeuristicTest() {
        PolyBigDecimal value = new PolyBigDecimal(new BigDecimal(1691879380700L));
        ProtoValue protoValue = PolyValueSerializer.serialize(value, new StreamingIndex());
        int actualSize = protoValue.toByteArray().length;
        int estimatedSize = SerializationHeuristic.estimateSize(value);
        assertTrue(estimatedSize >= actualSize);
        assertTrue(estimatedSize <= actualSize * 1.1);
    }

    @Test
    public void polyBinaryHeuristicTest() {
        PolyBinary value = PolyBinary.of(new byte[]{0x01, 0x02, 0x03});
        ProtoValue protoValue = PolyValueSerializer.serialize(value, new StreamingIndex());
        int actualSize = protoValue.toByteArray().length;
        int estimatedSize = SerializationHeuristic.estimateSize(value);
        assertTrue(estimatedSize >= actualSize);
        assertTrue(estimatedSize <= actualSize * 1.1);
    }

    @Test
    public void polyBooleanHeuristicTest() {
        PolyBoolean value = new PolyBoolean(true);
        ProtoValue protoValue = PolyValueSerializer.serialize(value, new StreamingIndex());
        int actualSize = protoValue.toByteArray().length;
        int estimatedSize = SerializationHeuristic.estimateSize(value);
        assertTrue(estimatedSize >= actualSize);
        assertTrue(estimatedSize <= actualSize * 1.1);
    }

    @Test
    public void polyDateHeuristicTest() {
        PolyDate value = PolyDate.ofDays(119581);
        ProtoValue protoValue = PolyValueSerializer.serialize(value, new StreamingIndex());
        int actualSize = protoValue.toByteArray().length;
        int estimatedSize = SerializationHeuristic.estimateSize(value);
        assertTrue(estimatedSize >= actualSize);
        assertTrue(estimatedSize <= actualSize * 1.1);
    }

    @Test
    public void polyDoubleHeuristicTest() {
        PolyDouble value = new PolyDouble(123.456);
        ProtoValue protoValue = PolyValueSerializer.serialize(value, new StreamingIndex());
        int actualSize = protoValue.toByteArray().length;
        int estimatedSize = SerializationHeuristic.estimateSize(value);
        assertTrue(estimatedSize >= actualSize);
        assertTrue(estimatedSize <= actualSize * 1.1);
    }

    @Test
    public void polyFloatHeuristicTest() {
        PolyFloat value = new PolyFloat(45.67f);
        ProtoValue protoValue = PolyValueSerializer.serialize(value, new StreamingIndex());
        int actualSize = protoValue.toByteArray().length;
        int estimatedSize = SerializationHeuristic.estimateSize(value);
        assertTrue(estimatedSize >= actualSize);
        assertTrue(estimatedSize <= actualSize * 1.1);
    }

    @Test
    public void polyIntegerHeuristicTest() {
        PolyInteger value = new PolyInteger(1234);
        ProtoValue protoValue = PolyValueSerializer.serialize(value, new StreamingIndex());
        int actualSize = protoValue.toByteArray().length;
        int estimatedSize = SerializationHeuristic.estimateSize(value);
        assertTrue(estimatedSize >= actualSize);
        assertTrue(estimatedSize <= actualSize * 1.1);
    }

    @Test
    public void polyListHeuristicTest() {
        PolyList<PolyInteger> value = new PolyList<>(
                new PolyInteger(1),
                new PolyInteger(2),
                new PolyInteger(3),
                new PolyInteger(4)
        );
        ProtoValue protoValue = PolyValueSerializer.serialize(value, new StreamingIndex());
        int actualSize = protoValue.toByteArray().length;
        int estimatedSize = SerializationHeuristic.estimateSize(value);
        assertTrue(estimatedSize >= actualSize);
        assertTrue(estimatedSize <= actualSize * 1.1);
    }

    @Test
    public void polyLongHeuristicTest() {
        PolyLong value = new PolyLong(1234567890L);
        ProtoValue protoValue = PolyValueSerializer.serialize(value, new StreamingIndex());
        int actualSize = protoValue.toByteArray().length;
        int estimatedSize = SerializationHeuristic.estimateSize(value);
        assertTrue(estimatedSize >= actualSize);
        assertTrue(estimatedSize <= actualSize * 1.1);
    }

    @Test
    public void polyStringHeuristicTest() {
        PolyString value = new PolyString("sample string");
        ProtoValue protoValue = PolyValueSerializer.serialize(value, new StreamingIndex());
        int actualSize = protoValue.toByteArray().length;
        int estimatedSize = SerializationHeuristic.estimateSize(value);
        assertTrue(estimatedSize >= actualSize);
        assertTrue(estimatedSize <= actualSize * 1.1);
    }

    @Test
    public void polyTimeHeuristicTest() {
        PolyTime value = PolyTime.of((3600 * 9 + 60 * 42 + 11) * 1000);
        ProtoValue protoValue = PolyValueSerializer.serialize(value, new StreamingIndex());
        int actualSize = protoValue.toByteArray().length;
        int estimatedSize = SerializationHeuristic.estimateSize(value);
        assertTrue(estimatedSize >= actualSize);
        assertTrue(estimatedSize <= actualSize * 1.1);
    }

    @Test
    public void polyTimestampHeuristicTest() {
        PolyTimestamp value = new PolyTimestamp(1691879380700L);
        ProtoValue protoValue = PolyValueSerializer.serialize(value, new StreamingIndex());
        int actualSize = protoValue.toByteArray().length;
        int estimatedSize = SerializationHeuristic.estimateSize(value);
        assertTrue(estimatedSize >= actualSize);
        assertTrue(estimatedSize <= actualSize * 1.1);
    }

    @Test
    public void polyDocumentHeuristicTest() {
        PolyDocument document = new PolyDocument();
        document.put(new PolyString("1st"), new PolyBoolean(false));
        document.put(new PolyString("2nd"), PolyBinary.of(new byte[]{0, 1, 2, 3, 4}));
        document.put(new PolyString("3rd"), new PolyDate(47952743435L));
        ProtoValue protoValue = PolyValueSerializer.serialize(document, new StreamingIndex());
        int actualSize = protoValue.toByteArray().length;
        int estimatedSize = SerializationHeuristic.estimateSize(document);
        assertTrue(estimatedSize >= actualSize);
        assertTrue(estimatedSize <= actualSize * 1.1);
    }

    @Test
    public void polyNodeHeuristicTest() {

    }

    @Test
    public void polyEdgeHeuristicTest() {

    }
}

