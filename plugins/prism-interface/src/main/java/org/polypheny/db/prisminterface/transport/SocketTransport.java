package org.polypheny.db.prisminterface.transport;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Util;

public abstract class SocketTransport implements Transport {

    protected final SocketChannel con;

    protected SocketTransport(SocketChannel con) throws IOException {
        this.con = con;
    }

    protected void writeEntireBuffer(ByteBuffer bb) throws IOException {
        synchronized (con) {
            while (bb.remaining() > 0) {
                int i = con.write(bb);
                if (i == -1) {
                    throw new EOFException();
                }
            }
        }
    }

    protected void readEntireBuffer(ByteBuffer bb) throws IOException {
        while (bb.remaining() > 0) {
            int i = con.read(bb);
            if (i == -1) {
                throw new EOFException();
            }
        }
        bb.rewind();
    }

    @Override
    public void close() {
        Util.closeNoThrow(con);
    }

    public static Transport accept(SocketChannel con) {
        try {
            return new PlainTransport(con);
        } catch (IOException e) {
            Util.closeNoThrow(con);
            throw new GenericRuntimeException(e);
        }
    }

}
