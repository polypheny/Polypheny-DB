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

import java.io.IOException;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.docker.exceptions.DockerUserException;
import org.polypheny.db.docker.models.DockerHost;
import org.polypheny.db.docker.models.HandshakeInfo;
import org.polypheny.db.docker.models.UpdateDockerResponse;

@Slf4j
public final class DockerSetupHelper {

    private DockerSetupHelper() {
    }


    private static void tryConnectDirectly( DockerHost host ) throws IOException {
        PolyphenyCertificateManager.loadServerCertificate( "docker", host.hostname() );
        PolyphenyCertificateManager.loadClientKeypair( "docker", host.hostname() );

        PolyphenyDockerClient client = PolyphenyDockerClient.connect( "docker", host.hostname(), host.communicationPort() );
        client.ping();
        client.close();
    }


    public static Optional<HandshakeInfo> newDockerInstance( @NonNull String hostname, @NonNull String alias, @NonNull String registry, int communicationPort, int handshakePort, int proxyPort, boolean startHandshake ) {
        DockerHost host = new DockerHost( hostname, alias, registry, communicationPort, handshakePort, proxyPort );
        if ( DockerManager.getInstance().hasHost( host.hostname() ) ) {
            throw new DockerUserException( "There is already a Docker instance connected to " + hostname );
        }

        if ( DockerManager.getInstance().hasAlias( host.alias() ) ) {
            throw new DockerUserException( "There is already a Docker instance with alias " + alias );
        }

        try {
            tryConnectDirectly( host );
            DockerManager.getInstance().addDockerInstance( host, null );
            return Optional.empty();
        } catch ( IOException e ) {
            return Optional.of( HandshakeManager.getInstance()
                    .newHandshake(
                            host,
                            () -> DockerManager.getInstance().addDockerInstance( host, null ),
                            startHandshake
                    )
            );
        }

    }


    public static UpdateDockerResponse updateDockerInstance( int id, String hostname, String alias, String registry ) {
        return DockerManager.getInstance().updateDockerInstance( id, hostname, alias, registry );
    }


    public static HandshakeInfo reconnectToInstance( int id ) {
        DockerInstance dockerInstance = DockerManager.getInstance().getInstanceById( id ).orElseThrow( () -> new DockerUserException( 404, "No Docker instance with that id" ) );

        return HandshakeManager.getInstance().newHandshake(
                dockerInstance.getHost(),
                () -> DockerManager.getInstance().getInstanceById( id ).ifPresent( DockerInstance::reconnect ),
                true
        );
    }


    public static void removeDockerInstance( int id ) {
        DockerInstance dockerInstance = DockerManager.getInstance().getInstanceById( id ).orElseThrow( () -> new DockerUserException( 404, "No Docker instance with that id" ) );
        try {
            if ( dockerInstance.hasContainers() ) {
                throw new DockerUserException( "Docker instance still in use by at least one container" );
            }
        } catch ( IOException e ) {
            log.info( "Failed to retrieve list of docker containers " + e );
        }

        HandshakeManager.getInstance().cancelHandshakes( dockerInstance.getHost().hostname() );
        DockerManager.getInstance().removeDockerInstance( id );
    }

}
