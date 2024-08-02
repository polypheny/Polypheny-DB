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

package org.polypheny.db.prisminterface.streaming;

import java.io.IOException;
import java.util.Arrays;
import org.polypheny.db.prisminterface.utils.PrismUtils;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.prism.StreamFrame;

public class StreamableBlobWrapper implements StreamableWrapper {

    private PolyBlob blob;


    public StreamableBlobWrapper( PolyBlob polyBinary ) {
        this.blob = polyBinary;
    }


    public StreamFrame get( long position, int length ) throws IOException {
        if ( position < 0 || length < 0 ) {
            throw new IllegalArgumentException( "Position and length must be non-negative" );
        }
        int end = ((int) position + length);
        byte[] data = Arrays.copyOfRange( blob.value, (int) position, end );
        return PrismUtils.buildBinaryStreamFrame( data, end >= blob.value.length );
    }

}
