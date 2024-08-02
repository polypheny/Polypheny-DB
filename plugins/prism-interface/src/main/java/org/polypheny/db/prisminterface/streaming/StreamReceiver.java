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

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.polypheny.db.prisminterface.PIServiceException;
import org.polypheny.prism.StreamAcknowledgement;
import org.polypheny.prism.StreamSendRequest;

public class StreamReceiver {

    private final Map<Long, StreamWrapper> streams = new HashMap<>();
    private final AtomicLong streamIdGenerator = new AtomicLong();


    public StreamAcknowledgement appendOrCreateNew( StreamSendRequest request ) {
        StreamWrapper piStream;
        long streamId;
        if ( !request.hasStreamId() ) {
            switch ( request.getFrame().getDataCase() ) {
                case BINARY -> {
                    ByteArrayOutputStream stream = new ByteArrayOutputStream();
                    stream.writeBytes( request.getFrame().getBinary().toByteArray() );
                    piStream = new StreamWrapper( stream );
                }
                case STRING -> {
                    StringWriter writer = new StringWriter();
                    writer.write( request.getFrame().getString() );
                    piStream = new StreamWrapper( writer );
                }
                default -> throw new PIServiceException( "Should never be thrown" );
            }
            streamId = streamIdGenerator.getAndIncrement();
            streams.put( streamId, piStream );
            return StreamAcknowledgement.newBuilder().setStreamId( streamId ).build();
        }
        streamId = request.getStreamId();
        piStream = streams.get( streamId );
        if ( piStream == null ) {
            throw new PIServiceException( "Stream " + streamId + " not found" );
        }
        if ( piStream.getStreamType() != request.getFrame().getDataCase() ) {
            throw new PIServiceException( "Stream " + streamId + " has incompatible data type" );
        }
        switch ( request.getFrame().getDataCase() ) {
            case BINARY -> piStream.getBinaryStream().writeBytes( request.getFrame().getBinary().toByteArray() );
            case STRING -> piStream.getStringStream().write( request.getFrame().getString() );
            default -> throw new PIServiceException( "Should never be thrown" );
        }
        return StreamAcknowledgement.newBuilder().setStreamId( streamId ).build();
    }


    public void remove( long streamId ) {
        streams.remove( streamId );
    }

}
