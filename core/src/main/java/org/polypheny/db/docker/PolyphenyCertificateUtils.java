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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Calendar;
import java.util.Date;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.crypto.AsymmetricCipherKeyPair;
import org.bouncycastle.crypto.generators.Ed25519KeyPairGenerator;
import org.bouncycastle.crypto.params.Ed25519KeyGenerationParameters;
import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters;
import org.bouncycastle.crypto.util.SubjectPublicKeyInfoFactory;
import org.bouncycastle.math.ec.rfc8032.Ed25519.Algorithm;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.bouncycastle.util.io.pem.PemWriter;

final class PolyphenyCertificateUtils {

    /**
     * Generate or load a keypair using the default algorithm.
     */
    static PolyphenyKeypair generateOrLoadCertificates( File certfile, File keyfile, String uuid ) throws IOException {
        if ( !keyfile.exists() || keyfile.length() == 0 ) {
            generateEd25519Keypair( uuid ).saveToDiskOverwrite( certfile, keyfile );
        }
        return PolyphenyKeypair.loadFromDisk( certfile, keyfile, uuid );
    }


    private static String encodeToPem( String description, byte[] data ) throws IOException {
        StringWriter stringWriter = new StringWriter();
        PemWriter pemWriter = new PemWriter( stringWriter );
        pemWriter.writeObject( new PemObject( description, data ) );
        pemWriter.flush();
        return stringWriter.toString();
    }


    private static void overWriteFile( File file, String content ) throws IOException {
        OutputStream os = new FileOutputStream( file, false );
        os.write( content.getBytes( StandardCharsets.UTF_8 ) );
        os.close();
    }


    static void saveAsPemOverwrite( File file, String description, byte[] data ) throws IOException {
        String encoded = encodeToPem( description, data );
        overWriteFile( file, encoded );
    }


    static byte[] loadPemFromFile( File file, String description ) throws IOException {
        PemObject o = new PemReader( new FileReader( file ) ).readPemObject();
        if ( o == null ) {
            throw new IOException( "No PEM object present in " + file.getAbsolutePath() );
        }
        if ( !description.equals( o.getType() ) ) {
            throw new IOException( "Unexpected object of type " + o.getType() + " expected " + description );
        }
        return o.getContent();
    }


    static byte[] loadCertificateFromFile( File file ) throws IOException {
        return loadPemFromFile( file, "CERTIFICATE" );
    }


    private static PolyphenyKeypair generateEd25519Keypair( String uuid ) throws IOException {
        Ed25519KeyPairGenerator gen = new Ed25519KeyPairGenerator();
        gen.init( new Ed25519KeyGenerationParameters( new SecureRandom() ) );
        AsymmetricCipherKeyPair keys = gen.generateKeyPair();

        SubjectPublicKeyInfo info = SubjectPublicKeyInfoFactory.createSubjectPublicKeyInfo( keys.getPublic() );
        X500Name issuer = new X500Name( "CN=issuer" );
        X500Name subject = new X500Name( "CN=" + uuid );
        Date notBefore = new Date();
        Calendar c = Calendar.getInstance();
        c.add( Calendar.YEAR, 10 );
        Date notAfter = c.getTime();
        X509v3CertificateBuilder builder = new X509v3CertificateBuilder( issuer, BigInteger.ONE, notBefore, notAfter, subject, info );
        X509CertificateHolder cert = builder.build( new ContentSigner() {
            ByteArrayOutputStream out = new ByteArrayOutputStream();


            public AlgorithmIdentifier getAlgorithmIdentifier() {
                return info.getAlgorithm();
            }


            public OutputStream getOutputStream() {
                return out;
            }


            public byte[] getSignature() {
                byte[] data = out.toByteArray();
                Ed25519PrivateKeyParameters params = (Ed25519PrivateKeyParameters) keys.getPrivate();
                byte[] sig = new byte[Ed25519PrivateKeyParameters.SIGNATURE_SIZE];
                params.sign( Algorithm.Ed25519, null, data, 0, data.length, sig, 0 );
                out = new ByteArrayOutputStream();
                return sig;
            }
        } );
        return new PolyphenyKeypair( cert, keys.getPrivate() );
    }

}
