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
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class DockerSetupHelper {


    private static void tryConnectDirectly( String hostname, int communicationPort ) throws IOException {
        byte[] serverCertificate;

        try {
            serverCertificate = PolyphenyCertificateManager.loadServerCertificate( hostname );
        } catch ( IOException e ) {
            throw new IOException( "No valid server certificate present" );
        }

        PolyphenyKeypair kp = PolyphenyCertificateManager.loadClientKeypair( hostname );
        PolyphenyDockerClient client = new PolyphenyDockerClient( hostname, communicationPort, kp, serverCertificate );
        client.ping();
        client.close();
    }


    public static DockerSetupResult newDockerInstance( String hostname, String alias, String registry, int communicationPort, int handshakePort, int proxyPort, boolean startHandshake ) {
        if ( communicationPort == handshakePort || handshakePort == proxyPort || communicationPort == proxyPort ) {
            return new DockerSetupResult( "Communication, handshake and proxy port must be different" );
        }
        if ( hostname.isEmpty() ) {
            return new DockerSetupResult( "Host must not be empty" );
        }

        if ( alias.isEmpty() ) {
            return new DockerSetupResult( "Alias must not be empty" );
        }

        if ( DockerManager.getInstance().hasHost( hostname ) ) {
            return new DockerSetupResult( "There is already a docker instance connected to " + hostname );
        }

        if ( DockerManager.getInstance().hasAlias( alias ) ) {
            return new DockerSetupResult( "There is already a docker instance with alias " + alias );
        }

        try {
            tryConnectDirectly( hostname, communicationPort );
            DockerManager.getInstance().addDockerInstance( hostname, alias, registry, communicationPort, handshakePort, proxyPort, null );
            return new DockerSetupResult( true );
        } catch ( IOException e ) {
            return new DockerSetupResult( HandshakeManager.getInstance()
                    .newHandshake(
                            hostname,
                            registry,
                            communicationPort,
                            handshakePort,
                            proxyPort,
                            () -> DockerManager.getInstance().addDockerInstance( hostname, alias, registry, communicationPort, handshakePort, proxyPort, null ),
                            startHandshake
                    ) );
        }

    }


    public static DockerUpdateResult updateDockerInstance( int id, String hostname, String alias, String registry ) {
        if ( hostname.isEmpty() ) {
            return new DockerUpdateResult( "Host must not be empty" );
        }

        if ( alias.isEmpty() ) {
            return new DockerUpdateResult( "Alias must not be empty" );
        }

        Optional<DockerInstance> maybeDockerInstance = DockerManager.getInstance().getInstanceById( id );

        if ( maybeDockerInstance.isEmpty() ) {
            return new DockerUpdateResult( "No docker instance with that id" );
        }

        DockerInstance dockerInstance = maybeDockerInstance.get();

        boolean hostChanged = !dockerInstance.getHost().equals( hostname );
        DockerManager.getInstance().updateDockerInstance( id, hostname, alias, registry );

        if ( hostChanged && !dockerInstance.isConnected() ) {
            HandshakeManager.getInstance().newHandshake(
                    hostname,
                    registry,
                    dockerInstance.getCommunicationPort(),
                    dockerInstance.getHandshakePort(),
                    dockerInstance.getProxyPort(),
                    () -> DockerManager.getInstance().getInstanceById( id ).ifPresent( DockerInstance::reconnect ),
                    true
            );
            return new DockerUpdateResult( dockerInstance, true );
        } else {
            return new DockerUpdateResult( dockerInstance, false );
        }
    }


    public static DockerReconnectResult reconnectToInstance( int id ) {
        Optional<DockerInstance> maybeDockerInstance = DockerManager.getInstance().getInstanceById( id );
        if ( maybeDockerInstance.isEmpty() ) {
            return new DockerReconnectResult( "No instance with that id" );
        }

        DockerInstance dockerInstance = maybeDockerInstance.get();

        Map<String, String> m = HandshakeManager.getInstance().newHandshake(
                dockerInstance.getHost(),
                dockerInstance.getRegistry(),
                dockerInstance.getCommunicationPort(),
                dockerInstance.getHandshakePort(),
                dockerInstance.getProxyPort(),
                () -> DockerManager.getInstance().getInstanceById( id ).ifPresent( DockerInstance::reconnect ),
                true
        );

        return new DockerReconnectResult( m );
    }


    public static String removeDockerInstance( int id ) {
        Optional<DockerInstance> maybeDockerInstance = DockerManager.getInstance().getInstanceById( id );

        if ( maybeDockerInstance.isEmpty() ) {
            return "No docker instance with that id";
        }

        DockerInstance dockerInstance = maybeDockerInstance.get();
        try {
            if ( dockerInstance.hasContainers() ) {
                return "Docker instance still in use";
            }
        } catch ( IOException e ) {
            log.info( "Failed to retrieve list of docker containers " + e );
        }

        DockerManager.getInstance().removeDockerInstance( id );
        HandshakeManager.getInstance().cancelHandshake( dockerInstance.getHost() );
        return "";
    }


    static public final class DockerSetupResult {

        @Getter
        private String error = "";
        private Map<String, String> handshake = Map.of();
        @Getter
        private boolean success = false;


        private DockerSetupResult( boolean success ) {
            this.success = success;
        }


        private DockerSetupResult( Map<String, String> handshake ) {
            this.handshake = handshake;
        }


        private DockerSetupResult( String error ) {
            this.error = error;
        }


        public Map<String, Object> getMap() {
            return Map.of(
                    "error", error,
                    "handshake", handshake,
                    "success", success
            );
        }

    }


    static public final class DockerUpdateResult {

        private String error = "";
        private Map<String, String> handshake = Map.of();
        private Map<String, Object> instance = Map.of();


        private DockerUpdateResult( String err ) {
            this.error = err;
        }


        private DockerUpdateResult( DockerInstance dockerInstance, boolean handshake ) {
            this.instance = dockerInstance.getMap();

            if ( handshake ) {
                this.handshake = HandshakeManager.getInstance().getHandshake( dockerInstance.getHost() );
            }
        }


        public Map<String, Object> getMap() {
            return Map.of(
                    "error", error,
                    "handshake", handshake,
                    "instance", instance
            );
        }

    }


    static public final class DockerReconnectResult {

        @Getter
        private String error = "";
        private Map<String, String> handshake = Map.of();


        private DockerReconnectResult( String error ) {
            this.error = error;
        }


        private DockerReconnectResult( Map<String, String> handshake ) {
            this.handshake = handshake;
        }


        public Map<String, Object> getMap() {
            return Map.of(
                    "error", error,
                    "handshake", handshake
            );
        }

    }

}
