package org.polypheny.db.prisminterface.streaming;

import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.polypheny.prism.StreamAcknowledgement;
import org.polypheny.prism.StreamFrame;

public class BinaryPIInputStream extends InputStream implements PIInputStream {

    BlockingQueue<Byte> buffer = new LinkedBlockingQueue<>();
    boolean isLast = false;
    boolean isClosed = false;


    @Override
    public int read() {
        if ( buffer.isEmpty() && isLast ) {
            return -1;
        }
        try {
            return buffer.take() & 0xFF;
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }


    public StreamAcknowledgement appendFrame( StreamFrame frame ) {
        byte[] data = frame.getBinary().toByteArray();
        try {
            for ( byte element : data ) {
                buffer.put( element );
            }
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
        isLast = frame.getIsLast();
        return StreamAcknowledgement.newBuilder().setCloseStream( isClosed ).build();
    }


    @Override
    public boolean isClosed() {
        return false;
    }


    @Override
    public void close() {
        isClosed = true;
    }

}
