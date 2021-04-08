/*
 * Copyright 2019-2021 The Polypheny Project
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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.async.ResultCallbackTemplate;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DefaultDockerClientConfig.Builder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.exceptions.NameExistsRuntimeException;
import org.polypheny.db.docker.exceptions.PortInUseRuntimeException;
import org.polypheny.db.util.FileSystemManager;


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
public class DockerInstance extends DockerManager {

    @Getter
    private ConfigDocker currentConfig;

    private DockerClient client;
    private final Map<String, Container> availableContainers = new HashMap<>();
    private final List<Image> availableImages = new ArrayList<>();
    private final HashMap<Integer, ImmutableList<String>> containersOnAdapter = new HashMap<>();

    // as Docker does not allow two containers with the same name or which expose the same port ( ports only for running containers )
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

        dockerRunning = testDockerRunning( client );
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

                    if ( splits[0].equals( Image.MONGODB.getName() ) ) {
                        availableImages.add( Image.MONGODB.setVersion( splits[1] ) );
                    }
                }
            }
        } );

        client.listContainersCmd().withShowAll( true ).exec().forEach( container -> {
            Arrays.stream( container.getPorts() ).forEach( containerPort -> usedPorts.add( containerPort.getPublicPort() ) );
            // docker returns the names with a prefixed "/", so we remove it
            usedNames.addAll( Arrays.stream( container.getNames() ).map( cont -> Container.getFromPhysicalName( cont.substring( 1 ) ) ).collect( Collectors.toList() ) );
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
                    .withDockerCertPath( FileSystemManager.getInstance().registerNewFolder( "certs/" + settings.getHost() + "/client" ).getPath() );
        }

        DockerClientConfig config = builder.build();

        ApacheDockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost( config.getDockerHost() )
                .sslConfig( config.getSSLConfig() )
                .build();

        return DockerClientImpl.getInstance( config, httpClient );
    }


    private boolean testDockerRunning( DockerClient client ) {
        // todo dl, better checking, exceptions for code flow is bad practice
        try {
            return null != client.infoCmd().exec();
        } catch ( Exception e ) {
            return false;
        }
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


    @Override
    public Container initialize( Container container ) {
        if ( !usedNames.contains( container.uniqueName ) ) {
            initContainer( container );
        }
        if ( !availableImages.contains( container.image ) ) {
            download( container.image );
        }

        // we add the name and the ports to our book-keeping functions as all previous checks passed
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
            // we got an already destroyed container which we have to recreate in Docker and call this method again
            initialize( container ).start();
            return;
        }

        // we have to check if the container is running and start it if its not
        InspectContainerResponse containerInfo = client.inspectContainerCmd( "/" + container.getPhysicalName() ).exec();
        ContainerState state = containerInfo.getState();
        if ( Objects.equals( state.getStatus(), "exited" ) ) {
            client.startContainerCmd( container.getPhysicalName() ).exec();
        } else if ( Objects.equals( state.getStatus(), "created" ) ) {
            client.startContainerCmd( container.getPhysicalName() ).exec();
            if ( container.afterCommands.size() != 0 ) {
                execAfterInitCommands( container );
            }
        }

        container.setStatus( ContainerStatus.RUNNING );
    }


    /**
     * This executes multiple defined commands after a delay in the given container
     *
     * @param container the container with specifies the afterCommands and to which they are applied
     */
    private void execAfterInitCommands( Container container ) {
        int offset = 0;
        for ( Entry<Integer, List<String>> entry : container.afterCommands.entrySet() ) {
            offset = entry.getKey() - offset;
            try {
                Thread.sleep( offset );
                ExecCreateCmdResponse cmd = client
                        .execCreateCmd( container.getContainerId() )
                        .withAttachStdout( true )
                        .withCmd( entry.getValue().toArray( new String[0] ) )
                        .exec();

                ResultCallbackTemplate<ResultCallback<Frame>, Frame> callback = new ResultCallbackTemplate<ResultCallback<Frame>, Frame>() {
                    @Override
                    public void onNext( Frame event ) {

                    }
                };

                client.execStartCmd( cmd.getId() ).exec( callback ).awaitCompletion();
            } catch ( InterruptedException e ) {
                throw new RuntimeException( e );
            }
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
                .withExposedPorts( container.getExposedPorts() );

        Objects.requireNonNull( cmd.getHostConfig() ).withPortBindings( bindings );
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
    protected void updateConfigs() {
        ConfigDocker newConfig = RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, instanceId );
        if ( !currentConfig.equals( newConfig ) ) {
            // something changed and we need to get a new client, which matches the new config
            this.client = generateClient( instanceId );
            this.dockerRunning = testDockerRunning( instanceId );
            RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, instanceId ).setDockerRunning( dockerRunning );
        }
        currentConfig = newConfig;
    }


    @Override
    public boolean testDockerRunning( int dockerId ) {
        if ( dockerId != instanceId ) {
            throw new RuntimeException( "There was a problem retrieving the correct DockerInstance" );
        }
        return testDockerRunning( client );
    }


    @Override
    public void stop( Container container ) {
        client.stopContainerCmd( container.getPhysicalName() ).exec();
        container.setStatus( ContainerStatus.STOPPED );
    }


    @Override
    public void destroy( Container container ) {
        if ( container.getStatus() == ContainerStatus.RUNNING ) {
            stop( container );
        }
        client.removeContainerCmd( container.getPhysicalName() ).exec();
        container.setStatus( ContainerStatus.DESTROYED );

        usedNames.remove( container.uniqueName );
        usedPorts.removeAll( container.getExposedPorts().stream().map( ExposedPort::getPort ).collect( Collectors.toList() ) );
        List<String> adapterContainers = containersOnAdapter
                .get( container.adapterId )
                .stream()
                .filter( cont -> !cont.equals( container.uniqueName ) )
                .collect( Collectors.toList() );
        containersOnAdapter.replace( container.adapterId, ImmutableList.copyOf( adapterContainers ) );
        availableContainers.remove( container.uniqueName );
    }


    // non-conflicting initializer for DockerManagerImpl()
    protected DockerInstance generateNewSession( int instanceId ) {
        return new DockerInstance( instanceId );
    }

}
