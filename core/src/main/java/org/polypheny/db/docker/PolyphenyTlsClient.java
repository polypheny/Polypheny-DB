package org.polypheny.db.docker;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Optional;
import java.security.SecureRandom;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.tls.Certificate;
import org.bouncycastle.tls.CertificateEntry;
import org.bouncycastle.tls.CertificateRequest;
import org.bouncycastle.tls.ProtocolVersion;
import org.bouncycastle.tls.TlsAuthentication;
import org.bouncycastle.tls.TlsContext;
import org.bouncycastle.tls.TlsServerCertificate;
import org.bouncycastle.tls.crypto.TlsCertificate;
import org.bouncycastle.tls.crypto.TlsCryptoParameters;
import org.bouncycastle.tls.crypto.impl.bc.BcDefaultTlsCredentialedSigner;
import org.bouncycastle.tls.crypto.impl.bc.BcTlsCertificate;
import org.bouncycastle.tls.crypto.impl.bc.BcTlsCrypto;
import org.bouncycastle.tls.DefaultTlsClient;
import org.bouncycastle.tls.TlsClientProtocol;
import org.bouncycastle.tls.TlsCredentials;
import org.bouncycastle.tls.AlertDescription;
import org.bouncycastle.tls.TlsFatalAlert;
import org.bouncycastle.util.Arrays;

@Slf4j
final class PolyphenyTlsClient {

    private static final BcTlsCrypto bcTlsCrypto = new BcTlsCrypto( new SecureRandom() );
    private final PolyphenyKeypair kp;
    private final MyTlsClient client;
    private final TlsClientProtocol proto;

    // Invariant ( serverCertificate != null && ! insecure ) || ( serverCertificate == null && insecure )
    private final byte[] serverCertificate;
    private final boolean insecure; // do not verify server certificate if true


    public PolyphenyTlsClient( PolyphenyKeypair kp, byte[] serverCertificate, InputStream in, OutputStream out ) throws IOException {
        this.insecure = false;
        this.serverCertificate = serverCertificate;
        this.kp = kp;
        this.client = new MyTlsClient();
        this.proto = new TlsClientProtocol( in, out );
        proto.connect( client );
    }


    // This returns a client where the server certificate is not verified.  For safety reasons this is not exposed directly,
    //but rather through the insecureClient method, making it obvious that caution is required.
    private PolyphenyTlsClient( PolyphenyKeypair kp, InputStream in, OutputStream out ) throws IOException {
        this.insecure = true;
        this.serverCertificate = null;
        this.kp = kp;
        this.client = new MyTlsClient();
        this.proto = new TlsClientProtocol( in, out );
        proto.connect( client );
    }


    public static PolyphenyTlsClient insecureClient( PolyphenyKeypair kp, InputStream in, OutputStream out ) throws IOException {
        return new PolyphenyTlsClient( kp, in, out );
    }


    public Optional<byte[]> getChannelBinding() {
        return client.getChannelBinding();
    }


    public Optional<InputStream> getInputStream() {
        if ( !insecure && client.getIsVerified() ) {
            return Optional.of( proto.getInputStream() );
        }

        return Optional.empty();
    }


    public Optional<OutputStream> getOutputStream() {
        if ( !insecure && client.getIsVerified() ) {
            return Optional.of( proto.getOutputStream() );
        }

        return Optional.empty();
    }


    /**
     * Insecure here means that the server certificate was not verified.
     */
    public InputStream getInsecureInputStream() {
        return proto.getInputStream();
    }


    public Optional<byte[]> getServerCertificate() {
        return client.auth.getServerCertificate();
    }


    public void close() {
        try {
            proto.close();
        } catch ( IOException ignore ) {
        }
    }


    private class MyTlsClient extends DefaultTlsClient {

        private byte[] channelBinding = null;
        private MyTlsAuthentication auth = null;


        public MyTlsClient() {
            super( bcTlsCrypto );
            protocolVersions = ProtocolVersion.TLSv13.only();
        }


        public TlsAuthentication getAuthentication() {
            synchronized ( this ) {
                if ( auth == null ) {
                    auth = new MyTlsAuthentication( context );
                }
            }

            return auth;
        }


        public void notifyHandshakeComplete() {
            channelBinding = context.exportKeyingMaterial( "EXPORTER-Channel-Binding", new byte[]{}, 32 );
        }


        public Optional<byte[]> getChannelBinding() {
            return Optional.ofNullable( channelBinding );
        }


        public boolean getIsVerified() {
            return auth != null && auth.getIsVerified();
        }


        private class MyTlsAuthentication implements TlsAuthentication {

            private final TlsContext ctx;
            private byte[] rawServerCertificate = null;

            private boolean isVerified = false;


            public MyTlsAuthentication( TlsContext ctx ) {
                this.ctx = ctx;
            }


            public TlsCredentials getClientCredentials( CertificateRequest certificateRequest ) {
                try {
                    BcTlsCertificate c = new BcTlsCertificate( bcTlsCrypto, kp.toASN1Structure() );
                    Certificate ce = new Certificate( certificateRequest.getCertificateRequestContext(), new CertificateEntry[]{ new CertificateEntry( c, null ) } );
                    return new BcDefaultTlsCredentialedSigner( new TlsCryptoParameters( ctx ), bcTlsCrypto, kp.getPrivateKey(), ce, kp.getSignatureAndHashAlgorithm() );
                } catch ( Exception e ) {
                    return null;
                }
            }


            public void notifyServerCertificate( TlsServerCertificate presentedCertificate ) throws IOException {
                Certificate c = presentedCertificate.getCertificate();

                // we want exactly one certificate
                if ( c.getLength() != 1 ) {
                    throw new TlsFatalAlert( AlertDescription.bad_certificate );
                }

                TlsCertificate cert = c.getCertificateAt( 0 );

                if ( insecure && serverCertificate == null ) {
                    rawServerCertificate = cert.getEncoded();
                    return;
                }

                if ( !insecure && serverCertificate != null ) {
                    if ( Arrays.constantTimeAreEqual( cert.getEncoded(), serverCertificate ) ) {
                        isVerified = true;
                        return;
                    }
                    throw new TlsFatalAlert( AlertDescription.bad_certificate );
                }
                // Reaching this point is only possible if the PolyphenyTlsClient class invariant was violated
                log.error( "Violated invariant in notifyServerCertificate" );
                throw new TlsFatalAlert( AlertDescription.bad_certificate );
            }


            public boolean getIsVerified() {
                return isVerified;
            }


            public Optional<byte[]> getServerCertificate() {
                return Optional.ofNullable( rawServerCertificate );
            }

        }

    }

}
