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

package org.polypheny.db.prisminterface.transport;

import java.io.EOFException;
import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Util;

public class PlainTransport implements Transport {

    private final static String VERSION = "plain-v1@polypheny.com";

    protected final SocketChannel con;


    PlainTransport( SocketChannel con ) throws IOException {
        this( con, VERSION );
    }


    protected PlainTransport( SocketChannel con, String version ) throws IOException {
        this.con = con;
        exchangeVersion( version );
    }


    @Override
    public Optional<String> getPeer() {
        return Optional.empty();
    }


    void exchangeVersion( String version ) throws IOException {
        if ( !version.matches( "\\A[a-z0-9@.-]+\\z" ) ) {
            throw new IOException( "Invalid version name" );
        }
        byte len = (byte) (version.length() + 1); // Trailing '\n'
        if ( len <= 0 ) {
            throw new IOException( "Version too long" );
        }
        ByteBuffer bb = ByteBuffer.allocate( 1 + len ); // Leading size
        bb.put( len );
        bb.put( version.getBytes( StandardCharsets.US_ASCII ) );
        bb.put( (byte) '\n' );
        bb.rewind();
        writeEntireBuffer( bb );
        byte[] response = readVersionResponse( len );
        if ( !Arrays.equals( bb.array(), response ) ) {
            throw new IOException( "Invalid client version" );
        }
    }


    private byte[] readVersionResponse( byte len ) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate( 1 + len ); // Leading size
        ByteBuffer length = bb.slice( 0, 1 );
        bb.position( 1 );
        readEntireBuffer( length );
        if ( length.get() != len ) {
            throw new IOException( "Invalid version response length" );
        }
        readEntireBuffer( bb );
        return bb.array();
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
            con.setOption( StandardSocketOptions.TCP_NODELAY, true );
            return new PlainTransport( con );
        } catch ( IOException e ) {
            Util.closeNoThrow( con );
            throw new GenericRuntimeException( e );
        }
    }

}
