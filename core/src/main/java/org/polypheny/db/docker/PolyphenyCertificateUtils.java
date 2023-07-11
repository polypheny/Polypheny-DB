package org.polypheny.db.docker;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
import org.polypheny.db.config.RuntimeConfig;

final class PolyphenyCertificateUtils {

    /**
     * Generate or load a keypair using the default algorithm.
     */
    public static PolyphenyKeypair generateOrLoadCertificates( String certfile, String keyfile ) throws IOException {
        Path keyfilePath = Paths.get( keyfile );
        if ( !Files.exists( keyfilePath ) || Files.size( keyfilePath ) == 0 ) {
            generateEd25519Keypair().saveToDiskOverwrite( certfile, keyfile );
        }
        return PolyphenyKeypair.loadFromDisk( certfile, keyfile );
    }


    private static String encodeToPem( String description, byte[] data ) throws IOException {
        StringWriter stringWriter = new StringWriter();
        PemWriter pemWriter = new PemWriter( stringWriter );
        pemWriter.writeObject( new PemObject( description, data ) );
        pemWriter.flush();
        return stringWriter.toString();
    }


    private static void overWriteFile( String filename, String content ) throws IOException {
        Path p = Paths.get( filename );

        OutputStream os = Files.newOutputStream( p, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING );
        os.write( content.getBytes( StandardCharsets.UTF_8 ) );
        os.close();
    }


    public static void saveAsPemOverwrite( String filename, String description, byte[] data ) throws IOException {
        String encoded = encodeToPem( description, data );
        overWriteFile( filename, encoded );
    }


    public static byte[] loadPemFromFile( String filename, String description ) throws IOException {
        Path p = Paths.get( filename );
        PemObject o = new PemReader( Files.newBufferedReader( p ) ).readPemObject();
        if ( o == null ) {
            throw new IOException( "No PEM object present" );
        }
        if ( !description.equals( o.getType() ) ) {
            throw new IOException( "Unexpected object of type " + o.getType() + " expected " + description );
        }
        return o.getContent();
    }


    public static byte[] loadCertificateFromFile( String filename ) throws IOException {
        return loadPemFromFile( filename, "CERTIFICATE" );
    }


    private static PolyphenyKeypair generateEd25519Keypair() throws IOException {
        Ed25519KeyPairGenerator gen = new Ed25519KeyPairGenerator();
        gen.init( new Ed25519KeyGenerationParameters( new SecureRandom() ) );
        AsymmetricCipherKeyPair keys = gen.generateKeyPair();

        SubjectPublicKeyInfo info = SubjectPublicKeyInfoFactory.createSubjectPublicKeyInfo( keys.getPublic() );
        X500Name issuer = new X500Name( "CN=issuer" );
        X500Name subject = new X500Name( "CN=" + RuntimeConfig.INSTANCE_UUID.getString() );
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
