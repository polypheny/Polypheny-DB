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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Util;

public class PlainTransport implements Transport {

    protected Socket con;
    protected InputStream in;
    protected OutputStream out;


    PlainTransport( Socket con ) throws IOException {
        this.con = con;
        this.in = con.getInputStream();
        this.out = con.getOutputStream();
    }


    @Override
    public void sendMessage( byte[] msg ) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate( 8 + msg.length );
        bb.order( ByteOrder.LITTLE_ENDIAN );
        bb.putLong( msg.length );
        bb.put( msg );
        out.write( bb.array() );
    }


    @Override
    public byte[] receiveMessage() throws IOException {
        byte[] b = in.readNBytes( 8 );
        if ( b.length != 8 ) {
            if ( b.length == 0 ) { // EOF
                throw new EOFException();
            }
            throw new IOException( "short read" );
        }
        ByteBuffer bb = ByteBuffer.wrap( b );
        bb.order( ByteOrder.LITTLE_ENDIAN ); // TODO Big endian like other network protocols?
        long length = bb.getLong();
        if ( length == 0 ) {
            throw new IOException( "Invalid message length" );
        }
        return in.readNBytes( (int) length );
    }


    @Override
    public void close() {
        Util.closeNoThrow( con );
    }


    public static Transport accept( Socket con ) {
        try {
            return new PlainTransport( con );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( e );
        }
    }

}
