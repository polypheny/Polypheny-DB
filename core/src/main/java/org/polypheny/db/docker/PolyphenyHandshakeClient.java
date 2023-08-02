/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.docker;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.util.DigestFactory;
import org.bouncycastle.tls.AlertDescription;
import org.bouncycastle.tls.TlsFatalAlert;
import org.bouncycastle.tls.TlsFatalAlertReceived;
import org.bouncycastle.util.Arrays;
import org.bouncycastle.util.encoders.Base64;
import org.polypheny.db.config.RuntimeConfig;

@Slf4j
final class PolyphenyHandshakeClient {

    // DO NOT INCREASE TO 255 WITHOUT FIXING THE AUTHENTICATION VALUE
    private static final byte HANDSHAKE_VERSION = 1;

    @Getter
    private State state;
    private final String hostname;
    private final int port;
    private final AtomicLong timeout;
    private final byte[] psk;

    private final PolyphenyKeypair kp;

    @Getter
    private final String handshakeParameters;
    private final byte[] polyphenyUUID;

    @Getter
    private String lastErrorMessage;

    private final Runnable onCompletion;


    enum State {
        STARTING,
        NOT_RUNNING,
        RUNNING,
        SUCCESS,
        FAILED,
    }


    PolyphenyHandshakeClient( String hostname, int port, AtomicLong timeout, Runnable onCompletion ) throws IOException {
        this.hostname = hostname;
        this.port = port;
        this.timeout = timeout;
        this.psk = new byte[32];
        new SecureRandom().nextBytes( this.psk );

        // Computed here, so we don't have to handle a possible UnsupportedCodingException when checking the
        // authentication value.
        this.polyphenyUUID = RuntimeConfig.INSTANCE_UUID.getString().getBytes( StandardCharsets.UTF_8 );

        this.kp = PolyphenyCertificateManager.generateOrLoadClientKeypair( "docker", hostname );
        this.handshakeParameters = getHandshakeParameters( kp );

        this.lastErrorMessage = "";

        this.onCompletion = onCompletion;

        this.state = State.STARTING;
    }


    private static byte[] sumSHA256( byte[] data ) {
        byte[] sum = new byte[32];
        Digest d = DigestFactory.createSHA256();
        d.update( data, 0, data.length );
        d.doFinal( sum, 0 );
        return sum;

    }


    private String getHandshakeParameters( PolyphenyKeypair kp ) {
        String a = Base64.toBase64String( sumSHA256( kp.getEncodedCertificate() ) );
        String b = Base64.toBase64String( psk );
        String c = Base64.toBase64String( polyphenyUUID );

        return HANDSHAKE_VERSION + "," + a + "," + b + "," + c;
    }


    private void appendLengthAndValue( ByteBuffer bb, byte[] data ) {
        bb.putInt( data.length );
        bb.put( data );
    }


    private byte[] computeAuthenticationValueV1( byte[] psk, byte[] uuid, byte[] channelBinding, byte[] serverCert, byte[] clientCert ) {
        // When changing the format of this, the HANDSHAKE_VERSION MUST be incremented
        int length = 1 + 4 + uuid.length + 4 + channelBinding.length + 4 + serverCert.length + 4 + clientCert.length;
        ByteBuffer bb = ByteBuffer.allocate( length );
        bb.order( ByteOrder.LITTLE_ENDIAN );
        bb.put( HANDSHAKE_VERSION );
        appendLengthAndValue( bb, uuid );
        appendLengthAndValue( bb, channelBinding );
        appendLengthAndValue( bb, serverCert );
        appendLengthAndValue( bb, clientCert );

        byte[] authValue = new byte[32];
        HMac mac = new HMac( DigestFactory.createSHA256() );
        mac.init( new KeyParameter( psk ) );
        mac.update( bb.array(), 0, length );
        mac.doFinal( authValue, 0 );

        return authValue;
    }


    void prepareNextTry() {
        // TOOD: merge into doHandshake somehow
        synchronized ( this ) {
            if ( this.state == State.SUCCESS || this.state == State.RUNNING || this.state == State.FAILED ) {
                return;
            }
            this.lastErrorMessage = "";
            this.state = State.STARTING;
        }
    }


    boolean doHandshake() {
        synchronized ( this ) {
            if ( state != State.NOT_RUNNING && state != State.STARTING ) {
                return false;
            }
            lastErrorMessage = "";
            state = State.RUNNING;
        }

        PolyphenyTlsClient client = null;
        boolean first = true;
        while ( client == null && Instant.now().getEpochSecond() < timeout.get() ) {
            if ( !first ) {
                try {
                    TimeUnit.SECONDS.sleep( 1 );
                } catch ( InterruptedException ignored ) {
                    // Ok, no issue with retrying sooner
                }
            }
            first = false;
            Socket con;
            try {
                con = new Socket( hostname, port );
            } catch ( IOException e ) {
                lastErrorMessage = "Could not connect to " + hostname + ":" + port;
                continue;
            }
            try {
                client = PolyphenyTlsClient.insecureClient( kp, con.getInputStream(), con.getOutputStream() );
            } catch ( IOException e ) {
                if ( e instanceof TlsFatalAlert ) {
                    TlsFatalAlert tlsAlert = (TlsFatalAlert) e;
                    short code = tlsAlert.getAlertDescription();
                    // This is if the container is up, but nothing is listening inside the container
                    if ( code == AlertDescription.handshake_failure ) {
                        lastErrorMessage = "Failed to perform handshake, is the command running?";
                        continue;
                    }
                    if ( code == AlertDescription.bad_certificate ) {
                        // The most likely reason is that the user pasted the wrong string
                        lastErrorMessage = "Client is using the wrong certificate, did you paste the right string?";
                        log.error( "Server is expecting a different certificate" );
                        break;
                    }
                } else if ( e instanceof SocketException ) {
                    SocketException socketException = (SocketException) e;
                    lastErrorMessage = socketException.getMessage();
                    if ( !socketException.getMessage().equals( "Connection reset" ) ) {
                        log.error( "SocketException", socketException );
                    }
                    continue;
                }
                lastErrorMessage = "Unknown error, check the logs";
                log.error( "Failed to open TLS connection", e );
                break;
            }
        }
        if ( client == null ) {
            synchronized ( this ) {
                state = State.NOT_RUNNING;
            }
            return false;
        }

        byte[] serverValue = new byte[32];
        try {
            int n = client.getInsecureInputStream().read( serverValue, 0, 32 );
            if ( n != 32 ) {
                throw new IOException( "Short read" );
            }
        } catch ( IOException e ) {
            if ( e instanceof TlsFatalAlertReceived ) {
                TlsFatalAlertReceived tlsAlert = (TlsFatalAlertReceived) e;
                short code = tlsAlert.getAlertDescription();
                if ( code == AlertDescription.bad_certificate ) {
                    // The most likely reason is that the user pasted the wrong string
                    lastErrorMessage = "Client is using the wrong certificate, did you paste the right string?";
                    log.error( "Server is expecting a different certificate" );
                }
            } else {
                lastErrorMessage = "Failed to read response from server";
                log.error( "Reading authentication value: ", e );
            }
            synchronized ( this ) {
                state = State.NOT_RUNNING;
            }
            return false;
        }

        byte[] cb = client.getChannelBinding().get();
        byte[] serverCertificate = client.getServerCertificate().get();
        byte[] authValue = computeAuthenticationValueV1( psk, polyphenyUUID, cb, serverCertificate, kp.getEncodedCertificate() );
        if ( Arrays.constantTimeAreEqual( authValue, serverValue ) ) {
            try {
                PolyphenyCertificateManager.saveServerCertificate( "docker", hostname, serverCertificate );
            } catch ( IOException e ) {
                lastErrorMessage = "Failed to save the server certificate to disk";
                log.error( "Failed to save server certificate", e );
                synchronized ( this ) {
                    state = State.NOT_RUNNING;
                }
                return false;
            }

            lastErrorMessage = new Date().toString();

            // timeout 0 means cancelled
            if ( onCompletion != null && timeout.get() != 0 ) {
                onCompletion.run();
            }

            synchronized ( this ) {
                state = State.SUCCESS;
            }
            return true;
        } else {
            log.error( "Server " + hostname + " has send an invalid authentication value, aborting handshake" );
            // On purpose not NOT_RUNNING, because it could be an attack attempt.  This forces regeneration
            // of the psk, so the attacker has one chance to guess.  The more likely explanation is pasting
            // an old command, hence this error message.
            lastErrorMessage = "Failed to authenticate the server: Be sure to use the displayed command, not an old one";
            synchronized ( this ) {
                state = State.FAILED;
            }
            return false;
        }
    }

}
