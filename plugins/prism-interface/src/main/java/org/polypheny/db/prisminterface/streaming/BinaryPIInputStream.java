package org.polypheny.db.prisminterface.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.SneakyThrows;
import org.polypheny.prism.StreamAcknowledgement;
import org.polypheny.prism.StreamFrame;

public class BinaryPIInputStream extends InputStream implements PIInputStream {

    private final Lock lock = new ReentrantLock();
    private final Condition hasData = lock.newCondition();
    private final Condition bufferEmpty = lock.newCondition();
    private volatile byte[] buffer;
    private int currentBufferIndex = 0;
    private volatile boolean isClosed = false;
    private volatile boolean isLast = false;

    @SneakyThrows
    @Override
    public int read() throws IOException {
        lock.lock();
        try {
            while (buffer == null) {
                if (isLast) {
                    System.out.println("EOS");
                    return -1;
                }
                System.out.println("Wait on more data");
                hasData.await();
            }
            int data = buffer[currentBufferIndex++] & 0xFF;
            if (currentBufferIndex == buffer.length) {
                buffer = null;
                currentBufferIndex = 0;
                bufferEmpty.signalAll();
                System.out.println("Notify fetch");
            }
            System.out.println("Read from binary PI stream");
            return data;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread interrupted during read", e);
        } finally {
            lock.unlock();
        }
    }

    public StreamAcknowledgement appendFrame(StreamFrame frame) throws InterruptedException {
        lock.lock();
        try {
            while (buffer != null) {
                bufferEmpty.await();
            }
            this.buffer = frame.getBinary().toByteArray();
            this.isLast = frame.getIsLast();
            System.out.println("Append binary PI stream");
            hasData.signalAll();
            System.out.println("Ack binary PI stream");
            return StreamAcknowledgement.newBuilder().setCloseStream(isClosed).build();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() {
        isClosed = true;
        lock.lock();
        try {
            hasData.signalAll(); // Notify any waiting threads that the stream is closed
        } finally {
            lock.unlock();
        }
    }
}
