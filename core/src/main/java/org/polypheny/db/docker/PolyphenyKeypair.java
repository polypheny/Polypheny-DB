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

package org.polypheny.db.docker;

import java.io.File;
import java.io.IOException;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x509.Certificate;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.crypto.util.PrivateKeyInfoFactory;
import org.bouncycastle.tls.SignatureAndHashAlgorithm;

final class PolyphenyKeypair {

    private final X509CertificateHolder cert;
    private final AsymmetricKeyParameter key;

    /**
     * Cache value for DER encoded certificate.  This is not done for
     * performance, but rather so that callers of
     * getEncodedCertificate() do not have to handle a potential
     * IOException.
     */
    private final transient byte[] derEncoding;


    PolyphenyKeypair( X509CertificateHolder cert, AsymmetricKeyParameter key ) throws IOException {
        this.cert = cert;
        this.key = key;
        this.derEncoding = cert.toASN1Structure().getEncoded( "DER" );
    }


    void saveToDiskOverwrite( File certfile, File keyfile ) throws IOException {
        PolyphenyCertificateUtils.saveAsPemOverwrite( certfile, "CERTIFICATE", cert.toASN1Structure().getEncoded( "DER" ) );

        // XXX: If we would save pkinfo1 directly, it would not be
        // understood by the openssl(1).  With this dance,
        // so openssl(1) can read the key.
        PrivateKeyInfo pkinfo1 = PrivateKeyInfoFactory.createPrivateKeyInfo( key );
        PrivateKeyInfo pkinfo = new PrivateKeyInfo( pkinfo1.getPrivateKeyAlgorithm(), pkinfo1.parsePrivateKey() );
        PolyphenyCertificateUtils.saveAsPemOverwrite( keyfile, "PRIVATE KEY", pkinfo.getEncoded( "DER" ) );
    }


    static PolyphenyKeypair loadFromDisk( File certfile, File keyfile, String uuid ) throws IOException {
        byte[] rawKey = PolyphenyCertificateUtils.loadPemFromFile( keyfile, "PRIVATE KEY" );
        AsymmetricKeyParameter sk = PrivateKeyFactory.createKey( rawKey );

        byte[] rawCertificate = PolyphenyCertificateUtils.loadPemFromFile( certfile, "CERTIFICATE" );
        X509CertificateHolder cert = new X509CertificateHolder( rawCertificate );

        PolyphenyKeypair kp = new PolyphenyKeypair( cert, sk );

        if ( !kp.getUuid().equals( uuid ) ) {
            throw new IOException( "Loaded certificate UUID does not match the expected UUID" );
        }

        return kp;
    }


    Certificate toASN1Structure() {
        return cert.toASN1Structure();
    }


    /**
     * Returns the DER encoded certificate
     */
    byte[] getEncodedCertificate() {
        return derEncoding;
    }


    AsymmetricKeyParameter getPrivateKey() {
        return key;
    }


    String getUuid() {
        // 2.5.4.3 is the common name
        return cert.getSubject().getRDNs( new ASN1ObjectIdentifier( "2.5.4.3" ) )[0].getFirst().getValue().toString();
    }


    /**
     * This exists only because we need it in the TLS client.  If
     * there ever is a way, that the TlsCredentialedSigner can
     * determine this automatically, it should be used.
     */
    SignatureAndHashAlgorithm getSignatureAndHashAlgorithm() {
        if ( key instanceof Ed25519PrivateKeyParameters ) {
            return SignatureAndHashAlgorithm.ed25519;
        }
        return null;
    }

}
