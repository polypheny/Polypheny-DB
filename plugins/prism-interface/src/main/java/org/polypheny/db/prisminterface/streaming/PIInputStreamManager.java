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
import org.polypheny.db.prisminterface.PIServiceException;
import org.polypheny.prism.StreamAcknowledgement;
import org.polypheny.prism.StreamFrame;
import org.polypheny.prism.StreamSendRequest;

public class PIInputStreamManager {

    private final Map<Long, PIInputStream> streams = new HashMap<>();


    public StreamAcknowledgement appendOrRegister( StreamSendRequest request ) throws IOException {
        StreamFrame frame = request.getFrame();
        PIInputStream stream = streams.get( request.getStreamId() );
        if ( stream == null ) {
            switch ( frame.getDataCase() ) {
                case STRING -> {
                    StringPIInputStream stringPIInputStream = new StringPIInputStream();
                    streams.put( request.getStreamId(), stringPIInputStream );
                    stream = stringPIInputStream;
                }
                case BINARY -> {
                    BinaryPIInputStream binaryPIInputStream = new BinaryPIInputStream();
                    streams.put( request.getStreamId(), binaryPIInputStream );
                    stream = binaryPIInputStream;
                }
                default -> throw new PIServiceException( "Unknown stream frame type: " + frame.getDataCase() );
            }
        }
        stream.appendFrame( frame );
        return stream.requestStreamAcknowledgement();
    }


    public BinaryPIInputStream getBinaryStream( long streamId ) {
        PIInputStream stream = streams.get( streamId );
        if ( !(stream instanceof BinaryPIInputStream) ) {
            throw new PIServiceException( "Stream " + streamId + " is not a binary stream" );
        }
        return (BinaryPIInputStream) stream;
    }


    public void removeAndCloseAll() {
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
