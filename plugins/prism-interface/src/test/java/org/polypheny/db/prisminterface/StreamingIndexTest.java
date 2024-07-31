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

import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.prisminterface.streaming.StreamableBinaryWrapper;
import org.polypheny.db.prisminterface.streaming.StreamableBlobWrapper;
import org.polypheny.db.prisminterface.streaming.StreamableWrapper;
import org.polypheny.db.prisminterface.streaming.StreamingIndex;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.prism.StreamFrame;

public class StreamingIndexTest {

    @BeforeAll
    public static void init() {
        // needed to launch polypheny
        TestHelper.getInstance();
    }


    @Test
    public void testInsertionAndRetrieval() {
        // first value
        byte[] expected1 = new byte[]{
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A
        };

        PolyBinary polyBinary1 = PolyBinary.of(expected1);
        StreamableWrapper wrapper1 = new StreamableBinaryWrapper(polyBinary1);

        // second value
        byte[] expected2 = new byte[]{
                0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14
        };

        PolyBinary polyBinary2 = PolyBinary.of(expected2);
        StreamableWrapper wrapper2 = new StreamableBinaryWrapper(polyBinary2);

        // add to index
        StreamingIndex index = new StreamingIndex();
        long streamId1 = index.register(wrapper1);
        long streamId2 = index.register(wrapper2);

        // check first value
        StreamFrame frame1;
        try {
            frame1 = index.get(streamId1).get(0, expected1.length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] result1 = frame1.getData().toByteArray();
        Assertions.assertArrayEquals(expected1, result1);
        assertTrue(frame1.getIsLast());

        // check second value
        StreamFrame frame2;
        try {
            frame2 = index.get(streamId2).get(0, expected2.length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] result2 = frame2.getData().toByteArray();
        Assertions.assertArrayEquals(expected2, result2);
        assertTrue(frame2.getIsLast());
    }

}
