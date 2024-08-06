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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.polypheny.db.prisminterface.PIServiceException;
import org.polypheny.prism.StreamAcknowledgement;
import org.polypheny.prism.StreamFrame;
import org.polypheny.prism.StreamFrame.DataCase;
import org.polypheny.prism.StreamSendRequest;

public class PIInputStreamManager {

    private final ConcurrentHashMap<Long, PIInputStream> streams = new ConcurrentHashMap<>();

    public int getSize() {
        return streams.size();
    }

    public StreamAcknowledgement appendOrRegister( StreamSendRequest request ) throws IOException, InterruptedException {
        StreamFrame frame = request.getFrame();
        PIInputStream stream = getOrCreateStream( request.getStreamId(), request.getFrame().getDataCase() );
        return stream.appendFrame( frame );
    }


    public PIInputStream getOrCreateStream( long streamId, DataCase streamType ) {
        PIInputStream stream = streams.get( streamId );
        if ( stream == null ) {
            switch ( streamType ) {
                case STRING -> {
                    StringPIInputStream stringPIInputStream = new StringPIInputStream();
                    streams.put( streamId, stringPIInputStream );
                    stream = stringPIInputStream;
                }
                case BINARY -> {
                    BinaryPIInputStream binaryPIInputStream = new BinaryPIInputStream();
                    streams.put( streamId, binaryPIInputStream );
                    stream = binaryPIInputStream;
                }
                default -> throw new PIServiceException( "Unknown stream frame type: " + streamType );
            }
        }
        return stream;
    }


    public BinaryPIInputStream getBinaryStreamOrRegister( long streamId) {
        PIInputStream stream = getOrCreateStream( streamId, DataCase.BINARY );
        if ( !(stream instanceof BinaryPIInputStream) ) {
            throw new PIServiceException( "Stream " + streamId + " is not a binary stream" );
        }
        return (BinaryPIInputStream) stream;
    }

    public StringPIInputStream getStringStreamOrRegister(long streamId) {
        PIInputStream stream = getOrCreateStream( streamId, DataCase.STRING );
        if (!(stream instanceof StringPIInputStream)) {
            throw new PIServiceException( "Stream " + streamId + " is not a string stream" );
        }
        return (StringPIInputStream) stream;
    }


    public void removeAllAndClose() {
        streams.keySet().forEach( streams::remove );
    }


    public void removeAndClose( long streamId ) {
        PIInputStream stream = streams.remove( streamId );
        if ( stream.isClosed() ) {
            return;
        }
        stream.close();
    }

}
