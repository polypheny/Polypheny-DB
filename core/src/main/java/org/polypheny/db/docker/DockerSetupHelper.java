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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;

@Slf4j
public final class DockerSetupHelper {

    // TODO racy
    private static boolean hasAlias( String alias ) {
        List<ConfigDocker> configlist = RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class );
        for ( ConfigDocker c : configlist ) {
            if ( c.getAlias().equals( alias ) ) {
                return true;
            }
        }
        return false;
    }


    // TODO racy
    private static boolean hasHostname( String hostname ) {
        List<ConfigDocker> configlist = RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class );
        for ( ConfigDocker c : configlist ) {
            if ( c.getHost().equals( hostname ) ) {
                return true;
            }
        }
        return false;
    }


    private static void tryConnectDirectly( String hostname ) throws IOException {
        byte[] serverCertificate;

        try {
            serverCertificate = PolyphenyCertificateManager.loadServerCertificate( hostname );
        } catch ( IOException e ) {
            throw new IOException( "No valid server certificate present" );
        }

        PolyphenyKeypair kp = PolyphenyCertificateManager.loadClientKeypair( hostname );
        PolyphenyDockerClient client = new PolyphenyDockerClient( hostname, ConfigDocker.COMMUNICATION_PORT, kp, serverCertificate );
        client.ping();
        client.close();
    }


    public static DockerSetupResult newDockerInstance( String hostname, String alias ) {
        if ( hostname.equals( "" ) ) {
            return new DockerSetupResult( "hostname must not be empty" );
        }

        if ( hasHostname( hostname ) ) {
            return new DockerSetupResult( "There is already a docker instance connected to " + hostname );
        }

        if ( alias.equals( "" ) ) {
            return new DockerSetupResult( "alias must not be empty" );
        }

        if ( hasAlias( alias ) ) {
            return new DockerSetupResult( "There is already a docker instance with alias " + alias );
        }

        try {
            tryConnectDirectly( hostname );
            int id = DockerManager.getInstance().addDockerInstance( hostname, alias, ConfigDocker.COMMUNICATION_PORT );
            return new DockerSetupResult( true, id );
        } catch ( IOException e ) {
            return new DockerSetupResult( HandshakeManager.getInstance()
                    .startHandshake(
                            hostname,
                            ConfigDocker.COMMUNICATION_PORT,
                            ConfigDocker.HANDSHAKE_PORT,
                            () -> DockerManager.getInstance().addDockerInstance( hostname, alias, ConfigDocker.COMMUNICATION_PORT )
                    ) );
        }

    }


    public static DockerUpdateResult updateDockerInstance( int id, String hostname, String alias ) {
        if ( hostname.equals( "" ) ) {
            return new DockerUpdateResult( "hostname must not be empty" );
        }

        if ( alias.equals( "" ) ) {
            return new DockerUpdateResult( "alias must not be empty" );
        }

        Optional<DockerInstance> maybeDockerInstance = DockerManager.getInstance().getInstanceById( id );

        if ( maybeDockerInstance.isEmpty() ) {
            return new DockerUpdateResult( "No docker instance with that id" );
        }

        DockerInstance dockerInstance = maybeDockerInstance.get();

        boolean hostChanged = !dockerInstance.getHost().equals( hostname );
        DockerManager.getInstance().updateDockerInstance( id, hostname, alias );

        if ( hostChanged && !dockerInstance.isConnected() ) {
            HandshakeManager.getInstance().startHandshake(
                    hostname,
                    ConfigDocker.COMMUNICATION_PORT,
                    ConfigDocker.HANDSHAKE_PORT,
                    () -> DockerManager.getInstance().getInstanceById( id ).ifPresent( DockerInstance::reconnect ) );
        }

        return new DockerUpdateResult( id, hostChanged );
    }


    public static DockerSetupResult removeDockerInstance( int id ) {
        Optional<DockerInstance> maybeDockerInstance = DockerManager.getInstance().getInstanceById( id );

        if ( maybeDockerInstance.isEmpty() ) {
            return new DockerSetupResult( "No docker instance with that id" );
        }

        DockerInstance dockerInstance = maybeDockerInstance.get();
        try {
            if ( dockerInstance.hasContainers() ) {
                return new DockerSetupResult( "DockerInstance still in use" );
            }

            DockerManager.getInstance().removeDockerInstance( id );
            HandshakeManager.getInstance().cancelHandshake( dockerInstance.getHost() );
            return new DockerSetupResult( true );
        } catch ( IOException e ) {
            return new DockerSetupResult( e.toString() );
        }
    }


    static public final class DockerSetupResult {

        private Map<String, String> handshake = Map.of();
        @Getter
        private String error = "";
        @Getter
        private boolean success = false;
        private int dockerId = -1;


        private DockerSetupResult( boolean success ) {
            this.success = success;
        }


        private DockerSetupResult( boolean success, int dockerId ) {
            this.success = success;
            this.dockerId = dockerId;
        }


        private DockerSetupResult( Map<String, String> handshake ) {
            this.handshake = handshake;
        }


        private DockerSetupResult( String error ) {
            this.error = error;
        }


        public Map<String, Object> getMap() {
            return Map.of( "handshake", handshake, "error", error, "success", success, "dockerId", dockerId );
        }

    }


    static public final class DockerUpdateResult {

        private String error = "";
        private Map<String, String> instance = Map.of();

        private Map<String, String> handshake = Map.of();


        private DockerUpdateResult( String err ) {
            this.error = err;
        }


        private DockerUpdateResult( int dockerId, boolean handshake ) {
            ConfigDocker configDocker = RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, dockerId );
            DockerInstance dockerInstance = DockerManager.getInstance().getInstanceById( configDocker.id ).get();

            this.instance = Map.of(
                    "host", configDocker.getHost(),
                    "alias", configDocker.getAlias(),
                    "connected", dockerInstance.isConnected() ? "true" : "false"
            );

            if ( handshake ) {
                this.handshake = HandshakeManager.getInstance().getHandshake( configDocker.getHost() );
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

}
