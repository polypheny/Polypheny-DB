package org.polypheny.db.prisminterface.streaming;

import java.io.IOException;
import java.io.Reader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.polypheny.prism.StreamAcknowledgement;
import org.polypheny.prism.StreamFrame;

public class StringPIInputStream extends Reader implements PIInputStream {

    private final BlockingQueue<String> bufferQueue = new LinkedBlockingQueue<>();
    private String currentBuffer = null;
    private int currentIndex = 0;
    private boolean isLast = false;
    private boolean isClosed = false;


    @Override
    public int read( char[] cbuf, int off, int len ) throws IOException {
        if ( currentBuffer == null || currentIndex >= currentBuffer.length() ) {
            if ( bufferQueue.isEmpty() && isLast ) {
                return -1;
            }
            try {
                currentBuffer = bufferQueue.take();
                currentIndex = 0;
            } catch ( InterruptedException e ) {
                throw new RuntimeException( e );
            }
        }

        int charsToRead = Math.min( len, currentBuffer.length() - currentIndex );
        currentBuffer.getChars( currentIndex, currentIndex + charsToRead, cbuf, off );
        currentIndex += charsToRead;
        return charsToRead;
    }


    public StreamAcknowledgement appendFrame( StreamFrame frame ) {
        String data = frame.getString();
        try {
            bufferQueue.put( data );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
        isLast = frame.getIsLast();
        return StreamAcknowledgement.newBuilder().setCloseStream( isClosed ).build();
    }


    @Override
    public void close() {
        isClosed = true;
    }


    public boolean isClosed() {
        return isClosed;
    }

}
