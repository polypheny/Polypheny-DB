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

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.prisminterface.transport.Transport;
import org.polypheny.db.util.Util;

@Slf4j
class PIRequestReader implements Closeable {

    private final Selector selector;
    private boolean closed = false;


    PIRequestReader( String name ) throws IOException {
        this.selector = Selector.open();
        new Thread( this::loop, String.format( "PIRequestReader for %s", name ) ).start();
    }


    public void addConnection( Transport transport, BlockingQueue<byte[]> queue, long connectionId ) throws ClosedChannelException {
        SelectableChannel chan = transport.getChannel();
        chan.register( selector, SelectionKey.OP_READ, new Connection( transport, queue, connectionId ) );
        selector.wakeup();
    }


    private void loop() {
        try {
            while ( !closed ) {
                selector.select( key -> {
                    Connection c = (Connection) key.attachment();
                    try {
                        Optional<byte[]> maybeMessage = c.transport.tryReceiveMessage();
                        if ( maybeMessage.isPresent() ) {
                            byte[] msg = maybeMessage.get();
                            c.queue.put( msg );
                        }
                    } catch ( EOFException | ClosedChannelException e ) {
                        // TODO: Close Transport?
                        key.cancel();
                    } catch ( IOException | InterruptedException e ) {
                        log.error( "Failed to receive message from connection with id {}", c.connectionId, e );
                        throw new GenericRuntimeException( e );
                    }
                } );
            }
        } catch ( IOException e ) {
            log.error( "Failed to select key", e );
        } finally {
            Util.closeNoThrow( selector );
        }
    }


    @Override
    public void close() throws IOException {
        closed = true;
        selector.wakeup();
    }


    private record Connection( Transport transport, BlockingQueue<byte[]> queue, long connectionId ) {

    }

}
