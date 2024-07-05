package org.polypheny.db.prisminterface.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

public class PlainTransport extends SocketTransport {

    private final static String VERSION = "plain-v1@polypheny.com";


    PlainTransport( SocketChannel con ) throws IOException {
        super( con );
        exchangeVersion( VERSION );
    }


    protected PlainTransport( SocketChannel con, String version ) throws IOException {
        super( con );
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
        byte i_length = length.get();
        if ( i_length != len ) {
            throw new IOException( "Invalid version response length" );
        }
        readEntireBuffer( bb );
        return bb.array();
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

}
