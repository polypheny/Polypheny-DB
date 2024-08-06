package org.polypheny.db.prisminterface.streaming;

import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.polypheny.prism.StreamAcknowledgement;
import org.polypheny.prism.StreamFrame;

public class BinaryPIInputStream extends InputStream implements PIInputStream {

    BlockingQueue<byte[]> bufferQueue = new LinkedBlockingQueue<>();
    byte[] currentBuffer = null;
    int currentIndex = 0;
    boolean isLast = false;
    boolean isClosed = false;

    @Override
    public int read() {
        if (currentBuffer == null || currentIndex >= currentBuffer.length) {
            if (bufferQueue.isEmpty() && isLast) {
                return -1;
            }
            try {
                currentBuffer = bufferQueue.take();
                currentIndex = 0;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return currentBuffer[currentIndex++] & 0xFF;
    }

    public StreamAcknowledgement appendFrame(StreamFrame frame) {
        byte[] data = frame.getBinary().toByteArray();
        try {
            bufferQueue.put(data);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        isLast = frame.getIsLast();
        return StreamAcknowledgement.newBuilder().setCloseStream(isClosed).build();
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() {
        isClosed = true;
    }

}
