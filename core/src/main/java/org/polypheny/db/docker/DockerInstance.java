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
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.docker.exceptions.DockerUserException;
import org.polypheny.db.docker.models.DockerHost;
import org.polypheny.db.docker.models.DockerInstanceInfo;
import org.polypheny.db.docker.models.HandshakeInfo;


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
    private static final Set<String> seenInstanceUuids = new HashSet<>();

    private final int instanceId;

    @Getter
    private DockerHost host;
    private Set<String> containerUuids = new HashSet<>();

    /**
     * The UUID of the Docker daemon we are talking to.  null if we are currently not connected.
     */
    private String dockerInstanceUuid;

    /**
     * The client object used to communicate with this Docker instance.  null if not connected.
     */
    private PolyphenyDockerClient client;

    private Status status = Status.NEW;


    private enum Status {
        NEW,
        CONNECTED,
        DISCONNECTED,
    }


    DockerInstance( int instanceId, DockerHost host ) {
        this.instanceId = instanceId;
        this.host = host;
        this.dockerInstanceUuid = null;
        try {
            checkConnection();
        } catch ( IOException e ) {
            log.error( "Could not connect to docker instance " + host.alias() + ": " + e.getMessage() );
        }
    }


    private void connectToDocker() throws IOException {
        this.client = PolyphenyDockerClient.connect( "docker", host.hostname(), host.communicationPort() );
        this.client.ping();
    }


    private void handleNewDockerInstance() throws IOException {
        this.dockerInstanceUuid = this.client.getDockerId();

        // seenUuids is used to synchronize with other DockerInstance instances
        synchronized ( seenInstanceUuids ) {
            for ( DockerInstance instance : DockerManager.getInstance().getDockerInstances().values() ) {
                if ( instance != this && instance.dockerInstanceUuid != null && instance.dockerInstanceUuid.equals( dockerInstanceUuid ) ) {
                    throw new DockerUserException( String.format( "Already connected to instance at '%s' with alias '%s'", this.host.hostname(), instance.host.alias() ) );
                }
            }
        }
        // What follows here is only to clean up old containers when Polypheny is reset.
        boolean first;
        synchronized ( seenInstanceUuids ) {
            first = seenInstanceUuids.add( this.dockerInstanceUuid );
        }

        if ( first && (Catalog.resetDocker || Catalog.resetCatalog) ) {
            this.client.listContainers().forEach(
                    containerInfo -> {
                        try {
                            this.client.deleteContainer( containerInfo.getUuid() );
                        } catch ( IOException e ) {
                            log.error( "Failed to delete container " + containerInfo.getUuid(), e );
                        }
                    }
            );
        }
    }


    private void checkConnection() throws IOException {
        synchronized ( this ) {
            if ( status != Status.CONNECTED || client == null || !client.isConnected() ) {
                connectToDocker();
            }

            if ( status == Status.NEW ) {
                handleNewDockerInstance();
                status = Status.DISCONNECTED; // This is so that the next block is executed as well, but that we never run handleNewDockerInstance again
            }

            // We only get here, if connectToDocker worked
            if ( status != Status.CONNECTED ) {
                Set<String> uuids = new HashSet<>();
                this.client.listContainers().forEach( c -> {
                    uuids.add( c.getUuid() );
                    new DockerContainer( c.getUuid(), c.getName() );
                } );
                this.containerUuids = uuids;
                status = Status.CONNECTED;
            } else {
                client.ping();
            }
        }
    }


    boolean isConnected() {
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
                if ( status != Status.NEW ) {
                    status = Status.DISCONNECTED;
                }
                checkConnection();
            } catch ( IOException e ) {
                log.info( "Failed to reconnect: " + e );
            }
        }
    }


    public void ping() {
        synchronized ( this ) {
            if ( status == Status.CONNECTED && client != null && client.isConnected() ) {
                try {
                    client.ping();
                } catch ( IOException e ) {
                    throw new DockerUserException( e );
                }
            } else {
                throw new DockerUserException( "Not connected" );
            }
        }
    }


    public DockerInstanceInfo getInfo() {
        synchronized ( this ) {
            int numberOfContainers = -1;
            try {
                if ( client != null ) {
                    numberOfContainers = client.listContainers().size();
                }
            } catch ( IOException e ) {
                // ignore
            }
            return new DockerInstanceInfo( instanceId, status == Status.CONNECTED, numberOfContainers, host );
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
                containerUuids.remove( container.getContainerId() );
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
            return containerUuids.contains( uuid );
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


    Optional<HandshakeInfo> updateConfig( String hostname, String alias, String registry ) {
        synchronized ( this ) {
            DockerHost newHost = new DockerHost( hostname, alias, registry, this.getHost().communicationPort(), this.getHost().handshakePort(), this.getHost().proxyPort() );
            if ( !this.host.hostname().equals( hostname ) ) {
                client.close();
                status = Status.NEW;
                // TODO: Copy/Move keys...
                // TODO: Restart all proxy connections
                try {
                    this.host = newHost;
                    checkConnection();
                } catch ( IOException e ) {
                    log.info( "Failed to connect to '" + hostname + "': " + e.getMessage() );
                    return Optional.of( HandshakeManager.getInstance().newHandshake( newHost, null, true ) );
                }
            }
            this.host = newHost;
            return Optional.empty();
        }
    }


    void close() {
        synchronized ( this ) {
            if ( client != null ) {
                client.close();
                client = null;
            }
            if ( status != Status.NEW ) {
                status = Status.DISCONNECTED;
            }
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
                containerUuids.add( uuid );
                return new DockerContainer( uuid, uniqueName );
            }
        }

    }

}
