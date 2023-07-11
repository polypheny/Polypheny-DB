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
import java.util.Collections;
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
        synchronized ( this ) {
            PolyphenyKeypair kp = PolyphenyCertificateManager.loadClientKeypair( currentConfig.getHost() );
            byte[] serverCertificate = PolyphenyCertificateManager.loadServerCertificate( currentConfig.getHost() );
            this.client = new PolyphenyDockerClient( currentConfig.getHost(), currentConfig.getPort(), kp, serverCertificate );
            this.client.ping();
        }
    }


    private void handleNewDockerInstance() throws IOException {
        this.dockerInstanceUuid = this.client.getDockerId();

        // seenUuids used ust to lock out all DockerInstance instances
        synchronized ( seenUuids ) {
            for ( DockerInstance instance : DockerManager.getInstance().getDockerInstances().values() ) {
                if ( instance.dockerInstanceUuid.equals( dockerInstanceUuid ) ) {
                    throw new RuntimeException( "The same docker instance cannot be added twice" );
                }
            }
        }

        boolean first;
        // This is to prevent races in seenUuids when multiple instances are loaded at the same time
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


    /**
     * This method may only be called from checkConnection, otherwise there is a race condition with dockerInstanceUuids,
     * where two instances could be connected to the same host simultaneously when this method is called at the right moment.
     */
    private void connectionLoss() {
        this.client = null;
        this.dockerInstanceUuid = null;
        this.status = Status.DISCONNECTED;
    }


    public void startContainer( DockerContainer container ) throws IOException {
        client.startContainer( container.getContainerId() );
    }


    public void stopContainer( DockerContainer container ) throws IOException {
        client.stopContainer( container.getContainerId() );
    }


    /**
     * Destroy the container.  Local resources are cleaned up regardless of whether the deallocation on the docker host
     * actually succeeded.
     */
    public void destroyContainer( DockerContainer container ) {
        try {
            client.deleteContainer( container.getContainerId() );
        } catch ( IOException e ) {
            log.error( "Failed to delete container with UUID " + container.getContainerId(), e );
        }
        DockerManager.getInstance().removeContainer( container.getContainerId() );
    }


    public int execute( DockerContainer container, List<String> cmd ) throws IOException {
        return client.executeCommand( container.getContainerId(), cmd );
    }


    public Map<Integer, Integer> getPorts( DockerContainer container ) throws IOException {
        Map<String, Map<Integer, Integer>> result = client.getPorts( Collections.singletonList( container.getContainerId() ) );
        return result.getOrDefault( container.getContainerId(), Map.of() );
    }


    public boolean hasContainers() throws IOException {
        return client.listContainers().size() > 0;
    }


    public ContainerBuilder newBuilder( String imageName, String uniqueName ) {
        return new ContainerBuilder( imageName, uniqueName );
    }


    void updateConfigs() {
        ConfigDocker newConfig = RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, instanceId );
        if ( !currentConfig.equals( newConfig ) ) {
            currentConfig = newConfig;
            try {
                connectionLoss();
                status = Status.NEW;
                checkConnection();
            } catch ( IOException e ) {
                log.error( "Failed to update config for instance " + instanceId, e );
            }
        }
    }


    public DockerStatus probeDockerStatus() {
        try {
            checkConnection();
            client.ping();
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
        private final PolyphenyDockerClient.ContainerBuilder builder;


        private ContainerBuilder( String imageName, String uniqueName ) {
            this.uniqueName = uniqueName;
            builder = client.newBuilder( imageName, uniqueName );
        }


        public ContainerBuilder withExposedPort( int port ) {
            builder.addPort( port );
            return this;
        }


        public ContainerBuilder withEnvironmentVariable( String key, String value ) {
            builder.putEnvironmentVariable( key, value );
            return this;
        }


        public ContainerBuilder withCommand( List<String> cmd ) {
            builder.setInitCommand( cmd );
            return this;
        }


        public DockerContainer build() {
            try {
                String uuid = builder.deploy();
                DockerManager.getInstance().takeOwnership( uuid, DockerInstance.this );
                return new DockerContainer( uuid, uniqueName, "RUNNING" );
            } catch ( IOException e ) {
                throw new RuntimeException( "Could not create container ", e );
            }
        }

    }

}
