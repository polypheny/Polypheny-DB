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

public class StreamableBlobWrapper {

    private PolyBlob blob;


    public StreamableBlobWrapper( PolyBlob polyBlob ) {
        this.blob = polyBlob;
    }


    public StreamFrame get( long position, int length ) throws IOException {
        if ( position < 0 || length < 0 ) {
            throw new IllegalArgumentException( "Position and length must be non-negative" );
        }
        if ( blob.value != null ) {
            int end = ((int) position + length);
            byte[] data = Arrays.copyOfRange( blob.value, (int) position, end );
            return PrismUtils.buildStreamFrame( data, end >= blob.value.length );
        }
        //TODO: handle stream. we ignore position for now as input streams don't support random access
        byte[] result = new byte[length];
        boolean is_last = false;
        int bytesRead = blob.stream.read( result );
        if ( bytesRead < length ) {
            is_last = true;
            byte[] truncatedResult = new byte[bytesRead];
            System.arraycopy( result, 0, truncatedResult, 0, bytesRead );
            result = truncatedResult;
        }
        return PrismUtils.buildStreamFrame( result, is_last );
    }

}
