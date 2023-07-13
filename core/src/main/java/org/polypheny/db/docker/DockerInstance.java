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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;


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

    @Getter
    private ConfigDocker currentConfig;

    /**
     * The UUID of the docker daemon we are talking to.  null if we are currently not connected.
     */
    private String dockerInstanceUuid;

    /**
     * The client object used to communicate with Docker.  null if not connected.
     */
    private PolyphenyDockerClient client;

    private final int instanceId;

    private Status status = Status.NEW;


    DockerInstance( Integer instanceId ) {
        this.currentConfig = RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, instanceId );
        this.instanceId = instanceId;
        this.dockerInstanceUuid = null;
        try {
            checkConnection();
        } catch ( IOException e ) {
            log.error( "Could not connect to docker ", e );
        }
    }


    private void connectToDocker() throws IOException {
        PolyphenyKeypair kp = PolyphenyCertificateManager.loadClientKeypair( currentConfig.getHost() );
        byte[] serverCertificate = PolyphenyCertificateManager.loadServerCertificate( currentConfig.getHost() );
        this.client = new PolyphenyDockerClient( currentConfig.getHost(), currentConfig.getPort(), kp, serverCertificate );
        this.client.ping();
    }


    private void handleNewDockerInstance() throws IOException {
        this.dockerInstanceUuid = this.client.getDockerId();

        // seenUuids is used to lock out all the other DockerInstance instances
        synchronized ( seenUuids ) {
            for ( DockerInstance instance : DockerManager.getInstance().getDockerInstances().values() ) {
                if ( instance.dockerInstanceUuid.equals( dockerInstanceUuid ) ) {
                    throw new RuntimeException( "The same docker instance cannot be added twice" );
                }
            }
        }

        boolean first;
        synchronized ( seenUuids ) {
            first = seenUuids.add( this.dockerInstanceUuid ) && (Catalog.resetDocker || Catalog.resetCatalog);
        }

        if ( first ) {
            List<ContainerInfo> containers = this.client.listContainers();
            for ( String uuid : containers.stream().map( ContainerInfo::getUuid ).collect( Collectors.toList() ) ) {
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

            if ( status != Status.CONNECTED ) {
                List<ContainerInfo> containers = this.client.listContainers();
                for ( ContainerInfo containerInfo : containers ) {
                    DockerManager.getInstance().takeOwnership( containerInfo.getUuid(), this );
                    Optional<DockerContainer> maybeContainer = DockerContainer.getContainerByUUID( containerInfo.getUuid() );
                    if ( maybeContainer.isPresent() ) {
                        maybeContainer.get().updateStatus( containerInfo.getStatus() );
                    } else {
                        new DockerContainer( containerInfo.getUuid(), containerInfo.getName(), containerInfo.getStatus().toUpperCase() );
                    }
                }
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
                DockerManager.getInstance().removeContainer( container.getContainerId() );
                client.deleteContainer( container.getContainerId() );
            } catch ( IOException e ) {
                log.error( "Failed to delete container with UUID " + container.getContainerId(), e );
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


    boolean hasContainers() throws IOException {
        synchronized ( this ) {
            return client.listContainers().size() > 0;
        }
    }


    public ContainerBuilder newBuilder( String imageName, String uniqueName ) {
        return new ContainerBuilder( imageName, uniqueName );
    }


    void updateConfigs() {
        ConfigDocker newConfig = RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, instanceId );
        if ( !currentConfig.equals( newConfig ) ) {
            synchronized ( this ) {
                currentConfig = newConfig;
                try {
                    this.client = null;
                    this.dockerInstanceUuid = null;
                    this.status = Status.NEW;
                    checkConnection();
                } catch ( IOException e ) {
                    log.error( "Failed to update config for instance " + instanceId, e );
                }
            }
        }
    }


    public DockerStatus probeDockerStatus() {
        try {
            checkConnection();
            return new DockerStatus( instanceId, true );
        } catch ( IOException e ) {
            return new DockerStatus( instanceId, false );
        }
    }


    private enum Status {
        NEW,
        CONNECTED,
        DISCONNECTED,
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
                String uuid = client.createAndStartContainer( DockerContainer.getPhysicalUniqueName( uniqueName ), imageName, exposedPorts, initCommand, environmentVariables, List.of() );
                DockerManager.getInstance().takeOwnership( uuid, DockerInstance.this );
                return new DockerContainer( uuid, uniqueName, "RUNNING" );
            }
        }

    }

}
