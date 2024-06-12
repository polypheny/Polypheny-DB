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
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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


    public BlockingQueue<Optional<byte[]>> addConnection( Transport transport, long connectionId ) throws ClosedChannelException {
        BlockingQueue<Optional<byte[]>> queue = new LinkedBlockingQueue<>();
        SelectableChannel chan = transport.getChannel();
        chan.register( selector, SelectionKey.OP_READ, new Connection( transport, queue, connectionId ) );
        selector.wakeup();
        return queue;
    }


    private void putIgnoreInterrupt( BlockingQueue<Optional<byte[]>> q, byte[] element ) {
        try {
            q.put( Optional.ofNullable( element ) );
        } catch ( InterruptedException e ) {
            // ignore
        }
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
                            putIgnoreInterrupt( c.queue, msg );
                        }
                    } catch ( IOException e ) {
                        putIgnoreInterrupt( c.queue, null );
                        key.cancel();
                    }
                } );
            }
        } catch ( IOException e ) {
            log.error( "Failed to select key", e );
        } finally {
            selector.keys().forEach( k -> putIgnoreInterrupt( ((Connection) k.attachment()).queue, null ) );
            Util.closeNoThrow( selector );
        }
    }


    @Override
    public void close() throws IOException {
        closed = true;
        selector.wakeup();
    }


    private record Connection( Transport transport, BlockingQueue<Optional<byte[]>> queue, long connectionId ) {

    }

}
