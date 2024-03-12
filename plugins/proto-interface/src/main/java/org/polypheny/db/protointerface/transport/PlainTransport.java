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

package org.polypheny.db.protointerface.transport;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Util;

public class PlainTransport implements Transport {

    protected final SocketChannel con;


    PlainTransport( SocketChannel con ) throws IOException {
        this.con = con;
    }


    @Override
    public Optional<String> getPeer() {
        return Optional.empty();
    }


    private void writeEntireBuffer( ByteBuffer bb ) throws IOException {
        synchronized ( con ) {
            while ( bb.remaining() > 0 ) {
                int i = con.write( bb );
                if ( i == -1 ) {
                    throw new EOFException();
                }
            }
        }
    }


    @Override
    public void sendMessage( byte[] msg ) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate( 8 + msg.length );
        bb.order( ByteOrder.LITTLE_ENDIAN );
        bb.putLong( msg.length );
        bb.put( msg );
        bb.rewind();
        writeEntireBuffer( bb );
    }


    private void readEntireBuffer( ByteBuffer bb ) throws IOException {
        while ( bb.remaining() > 0 ) {
            int i = con.read( bb );
            if ( i == -1 ) {
                throw new EOFException();
            }
        }
        bb.rewind();
    }


    @Override
    public byte[] receiveMessage() throws IOException {
        ByteBuffer bb = ByteBuffer.allocate( 8 );
        readEntireBuffer( bb );
        bb.order( ByteOrder.LITTLE_ENDIAN ); // TODO Big endian like other network protocols?
        long length = bb.getLong();
        if ( length == 0 ) {
            throw new IOException( "Invalid message length" );
        }
        bb = ByteBuffer.allocate( (int) length );
        readEntireBuffer( bb );
        return bb.array();
    }


    @Override
    public void close() {
        Util.closeNoThrow( con );
    }


    public static Transport accept( SocketChannel con ) {
        try {
            return new PlainTransport( con );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( e );
        }
    }

}
