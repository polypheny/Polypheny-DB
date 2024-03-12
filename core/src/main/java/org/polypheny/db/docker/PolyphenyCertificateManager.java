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
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.util.PolyphenyHomeDirManager;

public final class PolyphenyCertificateManager {


    private static boolean isSafeHostname( String hostname ) {
        return !(hostname.contains( "/" ) || hostname.contains( "\\" ) || hostname.startsWith( "." ));
    }


    private static String getBaseDirectory( String context, String hostname ) throws IOException {
        if ( !context.matches( "^[a-z]+$" ) ) {
            throw new IOException( "Invalid context string" );
        }
        if ( !isSafeHostname( hostname ) ) {
            throw new IOException( "Insecure hostname" );
        }
        return "certs/" + context + "/" + hostname + "/";
    }


    private static File getServerCertificateFile( String context, String hostname ) throws IOException {
        PolyphenyHomeDirManager dirManager = PolyphenyHomeDirManager.getInstance();
        String basePath = getBaseDirectory( context, hostname );
        return dirManager.registerNewFile( basePath + "server.pem" );
    }


    static byte[] loadServerCertificate( String context, String hostname ) throws IOException {
        byte[] cert = PolyphenyCertificateUtils.loadCertificateFromFile( getServerCertificateFile( context, hostname ) );
        if ( cert.length == 0 ) {
            throw new IOException( "Empty server certificate" );
        }
        return cert;
    }


    static void saveServerCertificate( String context, String hostname, byte[] serverCertificate ) throws IOException {
        PolyphenyCertificateUtils.saveAsPemOverwrite( getServerCertificateFile( context, hostname ), "CERTIFICATE", serverCertificate );
    }


    static PolyphenyKeypair loadClientKeypair( String context, String hostname ) throws IOException {
        PolyphenyHomeDirManager dirManager = PolyphenyHomeDirManager.getInstance();
        String basePath = getBaseDirectory( context, hostname );
        File clientKeyFile = dirManager.getHomeFile( basePath + "key.pem" ).orElseThrow( () -> new IOException( String.format( "Cannot read file %s", basePath + "key.pem" ) ) );
        File clientCertificateFile = dirManager.getHomeFile( basePath + "cert.pem" ).orElseThrow(() -> new IOException( String.format( "Cannot read file %s", basePath + "key.pem" ) ) );
        return PolyphenyKeypair.loadFromDisk( clientCertificateFile, clientKeyFile, RuntimeConfig.INSTANCE_UUID.getString() );
    }


    static PolyphenyKeypair generateOrLoadClientKeypair( String context, String hostname ) throws IOException {
        PolyphenyHomeDirManager dirManager = PolyphenyHomeDirManager.getInstance();
        String basePath = getBaseDirectory( context, hostname );
        File clientKeyFile = dirManager.registerNewFile( basePath + "key.pem" );
        File clientCertificateFile = dirManager.registerNewFile( basePath + "cert.pem" );
        return PolyphenyCertificateUtils.generateOrLoadCertificates( clientCertificateFile, clientKeyFile, RuntimeConfig.INSTANCE_UUID.getString() );
    }

}
