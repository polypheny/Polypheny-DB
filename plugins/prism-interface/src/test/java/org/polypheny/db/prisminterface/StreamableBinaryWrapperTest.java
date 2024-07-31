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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.prisminterface.streaming.StreamableBinaryWrapper;
import org.polypheny.db.prisminterface.streaming.StreamableBlobWrapper;
import org.polypheny.db.prisminterface.streaming.StreamableWrapper;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.prism.StreamFrame;

public class StreamableBinaryWrapperTest {

    @BeforeAll
    public static void init() {
        // needed to launch polypheny
        TestHelper.getInstance();
    }


    @Test
    public void testGetEntireData() throws IOException {
        byte[] expected = new byte[]{
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
                0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14,
                0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
                0x1F, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
                0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32
        };

        PolyBinary polyBinary = PolyBinary.of( expected );
        StreamableWrapper wrapper = new StreamableBinaryWrapper( polyBinary );

        StreamFrame frame = wrapper.get( 0, expected.length );
        byte[] result = frame.getData().toByteArray();
        Assertions.assertArrayEquals( expected, result );
        assertTrue( frame.getIsLast() );
    }


    @Test
    public void testGetBeginningSubset() throws IOException {
        byte[] expected = new byte[]{
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
                0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14,
                0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
                0x1F, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
                0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32
        };

        PolyBinary polyBinary = PolyBinary.of( expected );
        StreamableWrapper wrapper = new StreamableBinaryWrapper( polyBinary );

        int subsetLength = 10;
        byte[] expectedSubset = Arrays.copyOfRange( expected, 0, subsetLength );
        StreamFrame frame = wrapper.get( 0, subsetLength );
        byte[] result = frame.getData().toByteArray();
        Assertions.assertArrayEquals( expectedSubset, result );
        assertFalse( frame.getIsLast() );
    }


    @Test
    public void testGetMiddleSubset() throws IOException {
        byte[] expected = new byte[]{
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
                0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14,
                0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
                0x1F, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
                0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32
        };

        PolyBinary polyBinary = PolyBinary.of( expected );
        StreamableWrapper wrapper = new StreamableBinaryWrapper( polyBinary );

        int subsetLength = 10;
        int offset = 20;
        byte[] expectedMiddleSubset = Arrays.copyOfRange( expected, offset, offset + subsetLength );
        StreamFrame frame = wrapper.get( offset, subsetLength );
        byte[] result = frame.getData().toByteArray();
        Assertions.assertArrayEquals( expectedMiddleSubset, result );
        assertFalse( frame.getIsLast() );
    }


    @Test
    public void testGetEndSubset() throws IOException {
        byte[] expected = new byte[]{
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
                0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14,
                0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
                0x1F, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
                0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32
        };

        PolyBinary polyBinary = PolyBinary.of( expected );
        StreamableWrapper wrapper = new StreamableBinaryWrapper( polyBinary );

        int subsetLength = 10;
        int offset = expected.length - subsetLength;
        byte[] expectedEndSubset = Arrays.copyOfRange( expected, offset, expected.length );
        StreamFrame frame = wrapper.get( offset, subsetLength );
        byte[] result = frame.getData().toByteArray();
        Assertions.assertArrayEquals( expectedEndSubset, result );
        assertTrue( frame.getIsLast() );
    }


}
