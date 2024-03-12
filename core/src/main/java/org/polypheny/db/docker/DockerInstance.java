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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.docker.models.DockerHost;


/**
 * This class servers as an organization unit which controls all Docker containers in Polypheny,
 * which are placed on a specific Docker instance.
 * While the callers can and should mostly interact with the underlying containers directly,
 * this instance is used to have a control layer, which allows to restore, start or shutdown multiple of
 * these instances at the same time.
 */
@Slf4j
public final class DockerInstance {

    /**
     * This set is needed for resetDocker and resetCatalog.  The first time we see a new UUID, we save it in the set
     * and remove all the containers belonging to us.
     */
    private static final Set<String> seenUuids = new HashSet<>();

    private final int instanceId;

    @Getter
    private DockerHost host;
    private Set<String> uuids = new HashSet<>();

    /**
     * The UUID of the docker daemon we are talking to.  null if we are currently not connected.
     */
    private String dockerInstanceUuid;

    /**
     * The client object used to communicate with Docker.  null if not connected.
     */
    private PolyphenyDockerClient client;

    private Status status = Status.NEW;


    private enum Status {
        NEW,
        CONNECTED,
        DISCONNECTED,
    }


    DockerInstance( Integer instanceId, DockerHost host ) {
        this.host = host;
        this.instanceId = instanceId;
        this.dockerInstanceUuid = null;
        try {
            checkConnection();
        } catch ( IOException e ) {
            log.error( "Could not connect to docker instance " + host.alias() + ": " + e.getMessage() );
        }
    }


    private void connectToDocker() throws IOException {
        PolyphenyKeypair kp = PolyphenyCertificateManager.loadClientKeypair( "docker", host.hostname() );
        byte[] serverCertificate = PolyphenyCertificateManager.loadServerCertificate( "docker", host.hostname() );
        this.client = new PolyphenyDockerClient( host.hostname(), host.communicationPort(), kp, serverCertificate );
        this.client.ping();
    }


    private void handleNewDockerInstance() throws IOException {
        this.dockerInstanceUuid = this.client.getDockerId();

        // seenUuids is used to lock out all the other DockerInstance instances
        synchronized ( seenUuids ) {
            for ( DockerInstance instance : DockerManager.getInstance().getDockerInstances().values() ) {
                if ( instance != this && instance.dockerInstanceUuid.equals( dockerInstanceUuid ) ) {
                    throw new GenericRuntimeException( "The same docker instance cannot be added twice" );
                }
            }
        }

        boolean first;
        synchronized ( seenUuids ) {
            first = seenUuids.add( this.dockerInstanceUuid ) && (Catalog.resetDocker || Catalog.resetCatalog);
        }

        if ( first ) {
            List<ContainerInfo> containers = this.client.listContainers();
            for ( String uuid : containers.stream().map( ContainerInfo::getUuid ).toList() ) {
                try {
                    this.client.deleteContainer( uuid );
                } catch ( IOException e ) {
                    log.error( "Failed to delete container " + uuid, e );
                }
            }
        }
    }


    private void checkConnection() throws IOException {
        synchronized ( this ) {
            if ( status != Status.CONNECTED || client == null || !client.isConnected() ) {
                connectToDocker();
            }

            if ( status == Status.NEW ) {
                handleNewDockerInstance();
                status = Status.DISCONNECTED; // This is so that the next block is executed as well, but that we never rerun handleNewDockerInstance
            }

            // We only get here, if connectToDocker worked
            if ( status != Status.CONNECTED ) {
                Set<String> uuids = new HashSet<>();
                List<ContainerInfo> containers = this.client.listContainers();
                for ( ContainerInfo containerInfo : containers ) {
                    uuids.add( containerInfo.getUuid() );
                    new DockerContainer( containerInfo.getUuid(), containerInfo.getName() );
                }
                this.uuids = uuids;
                status = Status.CONNECTED;
            } else {
                client.ping();
            }
        }
    }


    public boolean isConnected() {
        try {
            checkConnection();
            return true;
        } catch ( IOException ignore ) {
            return false;
        }
    }


    public void reconnect() {
        synchronized ( this ) {
            try {
                status = Status.DISCONNECTED;
                checkConnection();
            } catch ( IOException e ) {
                log.info( "Failed to reconnect: " + e );
            }
        }
    }


    public DockerStatus probeDockerStatus() {
        return new DockerStatus( instanceId, isConnected() );
    }


    public Map<String, Object> getMap() {
        synchronized ( this ) {
            int numberOfContainers = -1;
            try {
                if ( client != null ) {
                    numberOfContainers = client.listContainers().size();
                }
            } catch ( IOException e ) {
                // ignore
            }
            return Map.of(
                    "id", instanceId,
                    "host", host.hostname(),
                    "alias", host.alias(),
                    "connected", isConnected(),
                    "registry", host.registry(),
                    "communicationPort", host.communicationPort(),
                    "handshakePort", host.handshakePort(),
                    "proxyPort", host.proxyPort(),
                    "numberOfContainers", numberOfContainers
            );
        }
    }


    void startContainer( DockerContainer container ) throws IOException {
        synchronized ( this ) {
            client.startContainer( container.getContainerId() );
        }
    }


    void stopContainer( DockerContainer container ) throws IOException {
        synchronized ( this ) {
            client.stopContainer( container.getContainerId() );
        }
    }


    /**
     * Destroy the container.  Local resources are cleaned up regardless of whether the deallocation on the docker host
     * actually succeeded.
     */
    void destroyContainer( DockerContainer container ) {
        synchronized ( this ) {
            try {
                uuids.remove( container.getContainerId() );
                client.deleteContainer( container.getContainerId() );
            } catch ( IOException e ) {
                if ( e.getMessage().startsWith( "No such container" ) ) {
                    log.info( "Cannot delete container: No container with UUID " + container.getContainerId() );
                } else {
                    log.error( "Failed to delete container with UUID " + container.getContainerId(), e );
                }
            }
        }
    }


    int execute( DockerContainer container, List<String> cmd ) throws IOException {
        synchronized ( this ) {
            return client.executeCommand( container.getContainerId(), cmd );
        }
    }


    Map<Integer, Integer> getPorts( DockerContainer container ) throws IOException {
        synchronized ( this ) {
            return client.getPorts( Collections.singletonList( container.getContainerId() ) ).getOrDefault( container.getContainerId(), Map.of() );
        }
    }


    boolean hasContainer( String uuid ) {
        synchronized ( this ) {
            return uuids.contains( uuid );
        }
    }


    boolean hasContainers() throws IOException {
        synchronized ( this ) {
            if ( client == null ) {
                throw new IOException( "Client not connected" );
            }
            return !client.listContainers().isEmpty();
        }
    }


    void updateConfig( String host, String alias, String registry ) {
        throw new NotImplementedException( "Updating configurations has been temporarily disabled" );
        /*
        synchronized ( this ) {
            if ( !this.host.hostname().equals( host ) ) {
                client.close();
                this.host = host;
                status = Status.NEW;
                try {
                    checkConnection();
                } catch ( IOException e ) {
                    log.info( "Failed to connect to " + host );
                }
            }
            this.alias = alias;
            this.registry = registry;
        }
         */
    }


    void close() {
        synchronized ( this ) {
            if ( client != null ) {
                client.close();
                client = null;
            }
            status = Status.DISCONNECTED;
        }
    }


    public ContainerBuilder newBuilder( String imageName, String uniqueName ) {
        return new ContainerBuilder( imageName, uniqueName );
    }


    public class ContainerBuilder {

        private final String uniqueName;
        private final String imageName;
        private List<String> initCommand = List.of();
        private final List<Integer> exposedPorts = new ArrayList<>();
        private final Map<String, String> environmentVariables = new HashMap<>();


        private ContainerBuilder( String imageName, String uniqueName ) {
            this.imageName = imageName;
            this.uniqueName = uniqueName;
        }


        public ContainerBuilder withExposedPort( int port ) {
            exposedPorts.add( port );
            return this;
        }


        public ContainerBuilder withEnvironmentVariable( String key, String value ) {
            environmentVariables.put( key, value );
            return this;
        }


        public ContainerBuilder withCommand( List<String> cmd ) {
            initCommand = cmd;
            return this;
        }


        public DockerContainer createAndStart() throws IOException {
            synchronized ( DockerInstance.this ) {
                final String registry = host.getRegistryOrDefault();

                final String imageNameWithRegistry;
                if ( registry.isEmpty() || registry.endsWith( "/" ) ) {
                    imageNameWithRegistry = registry + imageName;
                } else {
                    imageNameWithRegistry = registry + "/" + imageName;
                }

                String uuid = client.createAndStartContainer( DockerContainer.getPhysicalUniqueName( uniqueName ), imageNameWithRegistry, exposedPorts, initCommand, environmentVariables, List.of() );
                uuids.add( uuid );
                return new DockerContainer( uuid, uniqueName );
            }
        }

    }

}
