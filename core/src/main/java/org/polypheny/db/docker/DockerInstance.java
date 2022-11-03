/*
 * Copyright 2019-2022 The Polypheny Project
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

import static java.lang.String.format;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.async.ResultCallbackTemplate;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.exception.InternalServerErrorException;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DefaultDockerClientConfig.Builder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.exceptions.NameExistsRuntimeException;
import org.polypheny.db.docker.exceptions.PortInUseRuntimeException;
import org.polypheny.db.util.PolyphenyHomeDirManager;


/**
 * This class servers as a organization unit which controls all Docker containers in Polypheny,
 * which are placed on a specific Docker instance.
 * While the callers can and should mostly interact with the underlying containers directly,
 * this instance is used to have a control layer, which allows to restore, start or shutdown multiple of
 * these instances at the same time.
 *
 * For now, we have no way to determent if a previously created/running container with the same name
 * was created by Polypheny, so we try to reuse it.
 */
@Slf4j
public class DockerInstance extends DockerManager {

    public static final String DOCKER_NETWORK_NAME = "bridge";
    @Getter
    private ConfigDocker currentConfig;

    private DockerClient client;
    private final Map<String, Container> availableContainers = new HashMap<>();
    private final List<Image> availableImages = new ArrayList<>();
    private final HashMap<Integer, ImmutableList<String>> containersOnAdapter = new HashMap<>();

    // As Docker does not allow two containers with the same name or which expose the same port (ports only for running containers )
    // we have to track them, so we can return correct messages to the user
    @Getter
    private final List<Integer> usedPorts = new ArrayList<>();
    @Getter
    private final List<String> usedNames = new ArrayList<>();
    private final int instanceId;

    @Getter
    @Setter
    private boolean dockerRunning;


    DockerInstance( Integer instanceId ) {
        this.currentConfig = RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, instanceId );
        this.instanceId = instanceId;
        this.client = generateClient( this.instanceId );

        dockerRunning = probeDocker( client ).isSuccessful();
        RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, instanceId ).setDockerRunning( dockerRunning );

        if ( dockerRunning ) {
            updateUsedValues( client );
        }
    }


    private void updateUsedValues( DockerClient client ) {
        client.listImagesCmd().exec().forEach( image -> {
            if ( image.getRepoTags() != null ) {
                for ( String tag : image.getRepoTags() ) {
                    String[] splits = tag.split( ":" );

                    availableImages.add( new Image( splits[0], splits[1] ) );
                }
            }
        } );

        Map<String, Boolean> idsToRemove = new HashMap<>();
        Catalog catalog = Catalog.getInstance();

        outer:
        for ( com.github.dockerjava.api.model.Container container : client.listContainersCmd().withShowAll( true ).exec() ) {// Docker returns the names with a prefixed "/", so we remove it
            List<String> names = Arrays
                    .stream( container.getNames() )
                    .map( cont -> cont.substring( 1 ) )
                    .collect( Collectors.toList() );

            List<String> normalizedNames = names.stream().map( Container::getFromPhysicalName ).collect( Collectors.toList() );

            // When we have old containers, which belonged to a non-consistent adapter we remove them
            for ( String name : names ) {
                String[] splits = name.split( "_polypheny_" );
                if ( splits.length == 2 ) {
                    String unparsedAdapterId = splits[1];
                    boolean isTestContainer = splits[1].contains( "_test" );
                    // If the container was annotated with "_test", it has to be deleted if a new run in testMode was started
                    if ( isTestContainer ) {
                        unparsedAdapterId = unparsedAdapterId.replace( "_test", "" );
                    }

                    int adapterId = Integer.parseInt( unparsedAdapterId );
                    if ( !catalog.checkIfExistsAdapter( adapterId ) || !catalog.getAdapter( adapterId ).uniqueName.equals( splits[0] ) || isTestContainer || Catalog.resetDocker ) {
                        idsToRemove.put( container.getId(), container.getState().equalsIgnoreCase( "running" ) );
                        // As we remove this container later we skip the name and port adding
                        continue outer;
                    }
                }
            }

            Arrays.stream( container.getPorts() ).forEach( containerPort -> usedPorts.add( containerPort.getPublicPort() ) );
            usedNames.addAll( normalizedNames );
        }

        // We have to check if we accessed a mocking catalog as we don't want to remove all dockerInstance when running tests
        idsToRemove.forEach( ( id, isRunning ) -> {
            try {
                if ( isRunning ) {
                    client.stopContainerCmd( id ).exec();
                }
                client.removeContainerCmd( id ).exec();
            } catch ( Exception e ) {
                log.warn( "Error while removing old docker container." );
            }
        } );
    }


    private DockerClient generateClient( int instanceId ) {
        ConfigDocker settings = RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, instanceId );

        String host;
        if ( settings.getProtocol().equals( "ssh" ) ) {
            if ( settings.getUsername() == null ) {
                throw new RuntimeException( "To use a ssh connection for Docker a username is needed." );
            }
            host = "ssh://" + settings.getUsername() + "@" + settings.getHost();
        } else {
            host = "tcp://" + settings.getHost() + ":" + settings.getPort();
        }

        Builder builder = DefaultDockerClientConfig
                .createDefaultConfigBuilder()
                .withDockerHost( host );
        if ( !settings.isUsingInsecure() ) {
            builder
                    .withDockerTlsVerify( true )
                    .withDockerCertPath( PolyphenyHomeDirManager.getInstance().registerNewFolder( "certs/" + settings.getHost() + "/client" ).getPath() );
        }

        DockerClientConfig config = builder.build();

        ApacheDockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost( config.getDockerHost() )
                .sslConfig( config.getSSLConfig() )
                .build();

        return DockerClientImpl.getInstance( config, httpClient );
    }


    private DockerStatus probeDocker( DockerClient client ) {
        try {
            if ( !dockerRunning ) {
                refreshClient();
            }

            return new DockerStatus( instanceId, null != client.infoCmd().exec() );
        } catch ( Exception e ) {
            // something wrong with the connection
            return getCertStatus();
        }
    }


    private void refreshClient() {
        this.client = generateClient( instanceId );
    }


    private DockerStatus getCertStatus() {
        PolyphenyHomeDirManager dirManager = PolyphenyHomeDirManager.getInstance();
        if ( !dirManager.checkIfExists( "certs" ) ) {
            return new DockerStatus(
                    instanceId,
                    false,
                    "Connection certificates are not present, try restarting the Docker container." );
        }

        if ( !dirManager.checkIfExists( "certs/localhost" ) || !dirManager.checkIfExists( "certs/localhost/client" ) ) {
            return new DockerStatus(
                    instanceId,
                    false,
                    format( "Connection certificates are at the wrong location, try to clear the %s/certs folder and restart the Docker container.",
                            dirManager.getDefaultPath().getAbsolutePath() ) );
        }

        try {
            if ( !dirManager.checkIfExists( "certs/localhost/client/cert.pem" ) ) {
                return new DockerStatus(
                        instanceId,
                        false,
                        format( "Certificates do not exists, try to clear the %s/certs folder and restart the Docker container.",
                                dirManager.getDefaultPath().getAbsolutePath() ) );
            }
            File certFile = dirManager.registerNewFile( "certs/localhost/client/cert.pem" );

            String ca = Files.readString( certFile.toPath() );
            X509Certificate cert = (X509Certificate) CertificateFactory.getInstance( "X509" ).generateCertificate( new ByteArrayInputStream( ca.getBytes() ) );

            cert.checkValidity();

        } catch ( CertificateNotYetValidException ex ) {
            return new DockerStatus( instanceId, false, "Certificate is not yet valid" );
        } catch ( CertificateExpiredException ex ) {
            return new DockerStatus( instanceId, false, "Certificate is expired" );
        } catch ( CertificateException | IOException ex ) {
            return new DockerStatus( instanceId, false, ex.getMessage() );
        }
        return new DockerStatus( instanceId, true, "" );
    }


    protected boolean checkIfUnique( String uniqueName ) {
        return !availableContainers.containsKey( uniqueName );
    }


    private void registerIfAbsent( Container container ) {
        if ( !availableContainers.containsKey( container.uniqueName ) ) {
            availableContainers.put( container.uniqueName, container );

            if ( container.adapterId == null ) {
                return;
            }

            if ( !containersOnAdapter.containsKey( container.adapterId ) ) {
                containersOnAdapter.put( container.adapterId, ImmutableList.of( container.uniqueName ) );
            } else {
                List<String> containerNames = new ArrayList<>( containersOnAdapter.get( container.adapterId ) );
                containerNames.add( container.uniqueName );
                containersOnAdapter.put( container.adapterId, ImmutableList.copyOf( containerNames ) );
            }
        }
    }


    private void unregister( Container container ) {
        usedPorts.removeAll( container.internalExternalPortMapping.values() );
        usedNames.remove( container.uniqueName );
        if ( !availableContainers.containsKey( container.uniqueName ) ) {
            return;
        }
        availableContainers.remove( container.uniqueName );

        List<String> containers = new ArrayList<>( containersOnAdapter.get( container.adapterId ) );
        containers.remove( container.uniqueName );
        containersOnAdapter.put( container.adapterId, ImmutableList.copyOf( containers ) );
    }


    @Override
    public Container initialize( Container container ) {
        if ( !usedNames.contains( container.uniqueName ) ) {
            initContainer( container );
        }
        if ( !availableImages.contains( container.image ) ) {
            download( container.image );
        }

        // We add the name and the ports to our book-keeping functions as all previous checks passed
        // both get added above but the port is not always visible, e.g. when container is stopped
        usedPorts.addAll( container.internalExternalPortMapping.values() );
        usedNames.add( container.uniqueName );
        registerIfAbsent( container );

        return container;
    }


    @Override
    public void start( Container container ) {
        registerIfAbsent( container );

        if ( container.getStatus() == ContainerStatus.DESTROYED ) {
            // We got an already destroyed container which we have to recreate in Docker and call this method again
            initialize( container ).start();
            return;
        }

        // We have to check if the container is running and start it if its not
        InspectContainerResponse containerInfo = client.inspectContainerCmd( "/" + container.getPhysicalName() ).exec();

        if ( RuntimeConfig.USE_DOCKER_NETWORK.getBoolean() ) {
            connectToNetwork( container, containerInfo );
        }

        ContainerState state = containerInfo.getState();
        if ( Objects.equals( state.getStatus(), "exited" ) ) {
            startContainerSafely( container );
        } else if ( Objects.equals( state.getStatus(), "created" ) ) {
            startContainerSafely( container );

            // While the container is started the underlying system is not, so we have to probe it multiple times
            waitTillStarted( container );

            if ( container.afterCommands.size() != 0 ) {
                execAfterInitCommands( container );
            }
        }

        container.setContainerId( containerInfo.getId() );
        container.setStatus( ContainerStatus.RUNNING );
    }


    private void connectToNetwork( Container container, InspectContainerResponse containerInfo ) {
        if ( client.listNetworksCmd().exec().stream().noneMatch( n -> n.getName().equals( DOCKER_NETWORK_NAME ) ) ) {
            client.createNetworkCmd().withName( DOCKER_NETWORK_NAME ).exec();
        }
        String networkId = client.listNetworksCmd().withNameFilter( DOCKER_NETWORK_NAME ).exec().stream().filter( n -> n.getName().equalsIgnoreCase( DOCKER_NETWORK_NAME ) ).findFirst().orElseThrow().getId();
        client.connectToNetworkCmd().withContainerId( containerInfo.getId() ).withNetworkId( networkId ).exec();
        container.updateIpAddress();
    }


    @Override
    public void updateIpAddress( Container container ) {
        if ( !RuntimeConfig.USE_DOCKER_NETWORK.getBoolean() ) {
            return;
        }

        InspectContainerResponse containerInfo = client.inspectContainerCmd( "/" + container.getPhysicalName() ).exec();
        container.setIpAddress( containerInfo.getNetworkSettings().getNetworks().get( DOCKER_NETWORK_NAME ).getIpAddress() );
    }


    /**
     * While the DockerInstance knows the parameters of the corresponding Docker application
     * there still can be other application on the system, which lead to a fail
     * therefore the start of the container has to be handled correctly if something goes wrong
     *
     * @param container The container to start
     */
    private void startContainerSafely( Container container ) {
        try {
            client.startContainerCmd( container.getPhysicalName() ).exec();
        } catch ( InternalServerErrorException e ) {
            unregister( container );
            if ( container.getStatus() == ContainerStatus.INIT ) {
                // this container was freshly created, so we remove it that it can be tried again
                client.removeContainerCmd( container.getPhysicalName() ).exec();
            }
            throw new RuntimeException( "The specified port is likely already used by another application in the system. "
                    + "The adapter was not created." );
        }
    }


    /**
     * The container gets probed until the defined ready supplier returns true or the timeout is reached
     *
     * @param container the container which is waited for
     */
    private void waitTillStarted( Container container ) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        boolean isStarted = container.isReadySupplier.get();
        while ( !isStarted && (stopWatch.getTime() < container.maxTimeoutMs) ) {
            try {
                TimeUnit.MILLISECONDS.sleep( 500 );
            } catch ( InterruptedException e ) {
                // ignore
            }
            isStarted = container.isReadySupplier.get();
        }
        stopWatch.stop();
        if ( !isStarted ) {
            destroy( container );
            throw new RuntimeException( "The Docker container could not be started correctly." );
        }
    }


    /**
     * This executes multiple defined commands after a delay in the given container
     *
     * @param container the container with specifies the afterCommands and to which they are applied
     */
    private void execAfterInitCommands( Container container ) {
        ExecCreateCmdResponse cmd = client
                .execCreateCmd( container.getContainerId() )
                .withAttachStdout( true )
                .withCmd( container.afterCommands.toArray( new String[0] ) )
                .exec();

        ResultCallbackTemplate<ResultCallback<Frame>, Frame> callback = new ResultCallbackTemplate<>() {
            @Override
            public void onNext( Frame event ) {

            }
        };

        try {
            client.execStartCmd( cmd.getId() ).exec( callback ).awaitCompletion();
        } catch ( InterruptedException e ) {
            throw new RuntimeException( "The afterCommands for the container where not executed properly." );
        }
    }


    private void initContainer( Container container ) {
        if ( !availableImages.contains( container.image ) ) {
            download( container.image );
        }

        Ports bindings = new Ports();

        for ( Map.Entry<Integer, Integer> mapping : container.internalExternalPortMapping.entrySet() ) {
            // ExposedPort is exposed from container and Binding is port from outside
            bindings.bind( ExposedPort.tcp( mapping.getKey() ), Ports.Binding.bindPort( mapping.getValue() ) );
            if ( usedPorts.contains( mapping.getValue() ) ) {
                throw new PortInUseRuntimeException();
            }
        }

        if ( usedNames.contains( container.uniqueName ) ) {
            throw new NameExistsRuntimeException();
        }

        CreateContainerCmd cmd = client.createContainerCmd( container.image.getFullName() )
                .withName( Container.getPhysicalUniqueName( container ) )
                .withCmd( container.initCommands )
                .withEnv( container.envCommands )
                .withHostConfig( new HostConfig()
                        .withPublishAllPorts( true )
                        .withPortBindings( bindings ) );

        CreateContainerResponse response = cmd.exec();
        container.setContainerId( response.getId() );
    }


    /**
     * Tries to download the provided image through Docker,
     * this is necessary to have it accessible when later generating a
     * container from it
     *
     * If the image is already downloaded nothing happens
     */
    public void download( Image image ) {
        PullImageResultCallback callback = new PullImageResultCallback();
        client.pullImageCmd( image.getFullName() ).exec( callback );

        // TODO: blocking for now, maybe change or show warning?
        try {
            callback.awaitCompletion();
        } catch ( InterruptedException e ) {
            throw new RuntimeException( "The downloading of the image  " + image.getFullName() + " failed." );
        }

        availableImages.add( image );
    }


    @Override
    public void stopAll( int adapterId ) {
        if ( containersOnAdapter.containsKey( adapterId ) ) {
            containersOnAdapter.get( adapterId ).forEach( containerName -> availableContainers.get( containerName ).stop() );
        }

    }


    @Override
    public void destroyAll( int adapterId ) {
        if ( containersOnAdapter.containsKey( adapterId ) ) {
            containersOnAdapter.get( adapterId ).forEach( containerName -> availableContainers.get( containerName ).destroy() );
        }
    }


    @Override
    public Map<Integer, List<Integer>> getUsedPortsSorted() {
        HashMap<Integer, List<Integer>> map = new HashMap<>();
        map.put( instanceId, getUsedPorts() );
        return map;
    }


    @Override
    protected void updateConfigs() {
        ConfigDocker newConfig = RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, instanceId );
        if ( !currentConfig.equals( newConfig ) ) {
            // Something changed and we need to get a new client, which matches the new config
            this.client = generateClient( instanceId );
            this.dockerRunning = probeDockerStatus( instanceId ).isSuccessful();
            RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, instanceId ).setDockerRunning( dockerRunning );
        }
        currentConfig = newConfig;
    }


    @Override
    public DockerStatus probeDockerStatus( int dockerId ) {
        if ( dockerId != instanceId ) {
            throw new RuntimeException( "There was a problem retrieving the correct DockerInstance" );
        }
        return probeDocker( client );
    }


    @Override
    public void stop( Container container ) {
        client.stopContainerCmd( container.getPhysicalName() ).exec();
        container.setStatus( ContainerStatus.STOPPED );
    }


    @Override
    public void destroy( Container container ) {
        // While testing the container status itself is possible, in error cases, there might be no status set.
        // Therefore, we have to test by retrieving the container again from the client.
        String status = client.inspectContainerCmd( container.getContainerId() ).exec().getState().getStatus();
        if ( Objects.requireNonNull( status ).equalsIgnoreCase( "running" ) ) {
            stop( container );
        }

        client.removeContainerCmd( container.getPhysicalName() ).withRemoveVolumes( true ).exec();
        container.setStatus( ContainerStatus.DESTROYED );

        usedNames.remove( container.uniqueName );
        usedPorts.removeAll( container.internalExternalPortMapping.values() );
        List<String> adapterContainers = containersOnAdapter
                .get( container.adapterId )
                .stream()
                .filter( cont -> !cont.equals( container.uniqueName ) )
                .collect( Collectors.toList() );
        containersOnAdapter.replace( container.adapterId, ImmutableList.copyOf( adapterContainers ) );
        availableContainers.remove( container.uniqueName );
    }


    // Non-conflicting initializer for DockerManagerImpl()
    protected DockerInstance generateNewSession( int instanceId ) {
        return new DockerInstance( instanceId );
    }

}
