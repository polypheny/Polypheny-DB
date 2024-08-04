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
import java.io.InputStream;
import org.polypheny.prism.StreamAcknowledgement;
import org.polypheny.prism.StreamFrame;

public class BinaryPIInputStream extends InputStream implements PIInputStream {

    private byte[] buffer = null;
    private int currentBufferIndex = 0;
    private boolean isClosed = false;
    private final Object lock = new Object();


    @Override
    public int read() throws IOException {
        synchronized ( lock ) {
            while ( buffer == null && !isClosed ) {
                try {
                    lock.wait();
                } catch ( InterruptedException e ) {
                    Thread.currentThread().interrupt();
                    throw new IOException( "Stream interrupted", e );
                }
            }
            if ( buffer == null && isClosed ) {
                return -1;
            }
            int data = buffer[currentBufferIndex++] & 0xFF;
            if ( currentBufferIndex >= buffer.length ) {
                buffer = null;
                currentBufferIndex = 0;
                lock.notifyAll();
            }
            return data;
        }
    }


    public void appendFrame( StreamFrame frame ) {
        synchronized ( lock ) {
            while ( buffer != null && !isClosed ) {
                try {
                    lock.wait();
                } catch ( InterruptedException e ) {
                    Thread.currentThread().interrupt();
                }
            }
            if ( isClosed ) {
                return;
            }
            buffer = frame.getBinary().toByteArray();
            lock.notifyAll();
        }
    }


    @Override
    public boolean isClosed() {
        return isClosed;
    }


    public StreamAcknowledgement requestStreamAcknowledgement() {
        synchronized ( lock ) {
            while ( buffer != null && !isClosed ) {
                try {
                    lock.wait();
                } catch ( InterruptedException e ) {
                    Thread.currentThread().interrupt();
                }
            }
            lock.notifyAll();
            return StreamAcknowledgement.newBuilder().setCloseStream( isClosed ).build();

        }
    }


    @Override
    public void close() {
        synchronized ( lock ) {
            isClosed = true;
            buffer = null;
            lock.notifyAll();
        }
    }

}
