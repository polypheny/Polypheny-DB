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

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.prisminterface.transport.Transport;

@Slf4j
class PIRequestReader {

    private final Selector selector;
    private final Map<SelectionKey, Connection> connections = new HashMap<>();


    PIRequestReader( String name ) throws IOException {
        this.selector = Selector.open();
        new Thread( this::loop, String.format( "PIRequestReader for %s", name ) ).start();
    }


    public void addConnection( Transport transport, BlockingQueue<byte[]> queue ) throws ClosedChannelException {
        SelectableChannel chan = transport.getChannel();
        SelectionKey k = chan.register( selector, SelectionKey.OP_READ );
        connections.put( k, new Connection( transport, queue ) );
        selector.wakeup();
    }


    private void loop() {
        while ( true ) {
            try {
                selector.select( key -> {
                    Connection c = connections.get( key );
                    try {
                        Optional<byte[]> maybeMessage = c.transport.tryReceiveMessage();
                        if ( maybeMessage.isPresent() ) {
                            byte[] msg = maybeMessage.get();
                            c.queue.put( msg );
                        }
                    } catch ( EOFException | ClosedChannelException e ) {
                        // TODO: Close Transport?
                        key.cancel();
                        connections.remove( key );
                    } catch ( IOException | InterruptedException e ) {
                        log.error( "tryReceiveMessage", e );
                        throw new GenericRuntimeException( e );
                    }
                } );
            } catch ( IOException e ) {
                log.error( "select", e );
            }
        }
    }


    private record Connection( Transport transport, BlockingQueue<byte[]> queue ) {

    }

}
