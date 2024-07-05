package org.polypheny.db.prisminterface.transport;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Optional;
import java.nio.ByteOrder;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

public class SecureTransport extends SocketTransport {

    private final static String VERSION = "tls-v1@polypheny.com";
    private final SSLEngine sslEngine;
    private final SSLEngineResult.HandshakeStatus handshakeStatus;

    SecureTransport(SocketChannel con) throws IOException, NoSuchAlgorithmException, KeyManagementException {
        super(con);
        this.sslEngine = createSSLEngine();
        this.handshakeStatus = sslEngine.getHandshakeStatus();
        exchangeVersion(VERSION);
        performSSLHandshake();
    }

    protected SecureTransport(SocketChannel con, String version) throws IOException, NoSuchAlgorithmException, KeyManagementException {
        super(con);
        this.sslEngine = createSSLEngine();
        this.handshakeStatus = sslEngine.getHandshakeStatus();
        exchangeVersion(version);
        performSSLHandshake();
    }

    @Override
    public Optional<String> getPeer() {
        return Optional.of(sslEngine.getSession().getPeerHost());
    }

    void exchangeVersion(String version) throws IOException {
        if (!version.matches("\\A[a-z0-9@.-]+\\z")) {
            throw new IOException("Invalid version name");
        }
        byte len = (byte) (version.length() + 1); // Trailing '\n'
        if (len <= 0) {
            throw new IOException("Version too long");
        }
        ByteBuffer bb = ByteBuffer.allocate(1 + len); // Leading size
        bb.put(len);
        bb.put(version.getBytes( StandardCharsets.US_ASCII));
        bb.put((byte) '\n');
        bb.rewind();
        writeEntireBuffer(bb);
        byte[] response = readVersionResponse(len);
        if (!Arrays.equals(bb.array(), response)) {
            throw new IOException("Invalid client version");
        }
    }

    private byte[] readVersionResponse(byte len) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(1 + len);
        ByteBuffer length = bb.slice(0, 1);
        bb.position(1);
        readEntireBuffer(length);
        byte i_length = length.get();
        if (i_length != len) {
            throw new IOException("Invalid version response length");
        }
        readEntireBuffer(bb);
        return bb.array();
    }

    @Override
    public void sendMessage(byte[] msg) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(8 + msg.length);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.putLong(msg.length);
        bb.put(msg);
        bb.rewind();
        writeEntireBuffer(encrypt(bb));
    }

    @Override
    public byte[] receiveMessage() throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(8);
        readEntireBuffer(decrypt(bb));
        bb.order(ByteOrder.LITTLE_ENDIAN); // TODO Big endian like other network protocols?
        long length = bb.getLong();
        if (length == 0) {
            throw new IOException("Invalid message length");
        }
        bb = ByteBuffer.allocate((int) length);
        readEntireBuffer(decrypt(bb));
        return bb.array();
    }

    private SSLEngine createSSLEngine() throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, null, null);
        SSLEngine engine = sslContext.createSSLEngine();
        engine.setUseClientMode(false);
        return engine;
    }

    private void performSSLHandshake() throws IOException {
        sslEngine.beginHandshake();
        SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
        ByteBuffer myAppData = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        ByteBuffer peerAppData = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        ByteBuffer myNetData = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        ByteBuffer peerNetData = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());

        while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED &&
                handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {

            switch (handshakeStatus) {
                case NEED_UNWRAP:
                    if (con.read(peerNetData) < 0) {
                        throw new EOFException("Connection closed unexpectedly during handshake");
                    }
                    peerNetData.flip();
                    SSLEngineResult res = sslEngine.unwrap(peerNetData, peerAppData);
                    peerNetData.compact();
                    handshakeStatus = res.getHandshakeStatus();
                    switch (res.getStatus()) {
                        case OK:
                            break;
                        case BUFFER_OVERFLOW:
                            peerAppData = enlargeApplicationBuffer(sslEngine, peerAppData);
                            break;
                        case BUFFER_UNDERFLOW:
                            peerNetData = handleBufferUnderflow(sslEngine, peerNetData);
                            break;
                        case CLOSED:
                            throw new SSLException("SSLEngine closed during handshake");
                        default:
                            throw new IllegalStateException("Invalid SSL status: " + res.getStatus());
                    }
                    break;

                case NEED_WRAP:
                    myNetData.clear();
                    res = sslEngine.wrap(myAppData, myNetData);
                    handshakeStatus = res.getHandshakeStatus();
                    switch (res.getStatus()) {
                        case OK:
                            myNetData.flip();
                            while (myNetData.hasRemaining()) {
                                con.write(myNetData);
                            }
                            break;
                        case BUFFER_OVERFLOW:
                            myNetData = enlargePacketBuffer(sslEngine, myNetData);
                            break;
                        case BUFFER_UNDERFLOW:
                            throw new SSLException("Buffer underflow during handshake");
                        case CLOSED:
                            throw new SSLException("SSLEngine closed during handshake");
                        default:
                            throw new IllegalStateException("Invalid SSL status: " + res.getStatus());
                    }
                    break;

                case NEED_TASK:
                    Runnable task;
                    while ((task = sslEngine.getDelegatedTask()) != null) {
                        task.run();
                    }
                    handshakeStatus = sslEngine.getHandshakeStatus();
                    break;

                default:
                    throw new IllegalStateException("Invalid SSL handshake status: " + handshakeStatus);
            }
        }
    }

    private ByteBuffer enlargeApplicationBuffer(SSLEngine engine, ByteBuffer buffer) {
        return ByteBuffer.allocate(engine.getSession().getApplicationBufferSize() + buffer.position()).put(buffer);
    }

    private ByteBuffer enlargePacketBuffer(SSLEngine engine, ByteBuffer buffer) {
        return ByteBuffer.allocate(engine.getSession().getPacketBufferSize() + buffer.position()).put(buffer);
    }

    private ByteBuffer handleBufferUnderflow(SSLEngine engine, ByteBuffer buffer) {
        if (buffer.position() < buffer.limit()) {
            buffer.compact();
            return buffer;
        } else {
            return enlargePacketBuffer(engine, buffer);
        }
    }


    private ByteBuffer encrypt(ByteBuffer plainBuffer) throws IOException {
        ByteBuffer encryptedBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        SSLEngineResult result = sslEngine.wrap(plainBuffer, encryptedBuffer);
        return switch ( result.getStatus() ) {
            case OK -> {
                encryptedBuffer.flip();
                yield encryptedBuffer;
            }
            case BUFFER_OVERFLOW -> throw new IOException( "Buffer overflow during encryption" );
            case BUFFER_UNDERFLOW -> throw new IOException( "Buffer underflow during encryption" );
            case CLOSED -> throw new IOException( "SSLEngine is closed" );
            default -> throw new IOException( "Unexpected SSLEngine status: " + result.getStatus() );
        };
    }

    private ByteBuffer decrypt(ByteBuffer encryptedBuffer) throws IOException {
        ByteBuffer decryptedBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        SSLEngineResult result = sslEngine.unwrap(encryptedBuffer, decryptedBuffer);
        return switch ( result.getStatus() ) {
            case OK -> {
                decryptedBuffer.flip();
                yield decryptedBuffer;
            }
            case BUFFER_OVERFLOW -> throw new IOException( "Buffer overflow during decryption" );
            case BUFFER_UNDERFLOW -> throw new IOException( "Buffer underflow during decryption" );
            case CLOSED -> throw new IOException( "SSLEngine is closed" );
            default -> throw new IOException( "Unexpected SSLEngine status: " + result.getStatus() );
        };
    }

}
