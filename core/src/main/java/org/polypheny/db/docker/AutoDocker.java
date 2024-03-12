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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectExecResponse;
import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.RestartPolicy;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.DockerSetupHelper.DockerReconnectResult;
import org.polypheny.db.docker.DockerSetupHelper.DockerSetupResult;
import org.polypheny.db.docker.models.DockerHost;

@Slf4j
public final class AutoDocker {

    private static final AutoDocker INSTANCE = new AutoDocker();

    private String status = "";

    private Thread thread = null;

    private final DockerHost host = new DockerHost( "localhost", "localhost", "", ConfigDocker.COMMUNICATION_PORT, ConfigDocker.HANDSHAKE_PORT, ConfigDocker.PROXY_PORT );


    public static AutoDocker getInstance() {
        return INSTANCE;
    }


    private void updateStatus( String newStatus ) {
        status = newStatus;
        log.info( "AutoDocker: " + newStatus );
    }


    private void createPolyphenyConnectorVolumeIfNotExists( DockerClient client ) {
        List<InspectVolumeResponse> volumes = client.listVolumesCmd().exec().getVolumes();

        if ( volumes.stream().noneMatch( vol -> vol.getName().equals( DockerUtils.VOLUME_NAME ) ) ) {
            client.createVolumeCmd().withName( DockerUtils.VOLUME_NAME ).exec();
        }
    }


    private Optional<String> findAndStartPolyphenyContainer( DockerClient client ) {
        List<Container> resp = client.listContainersCmd().withShowAll( true ).exec();
        for ( Container c : resp ) {
            for ( String name : c.getNames() ) {
                if ( name.equals( "/" + DockerUtils.CONTAINER_NAME ) ) {
                    if ( !c.getState().equals( "running" ) ) {
                        client.startContainerCmd( c.getId() ).exec();
                    }
                    return Optional.of( c.getId() );
                }
            }
        }
        return Optional.empty();
    }


    private boolean hasLocalImage( DockerClient client, String imageName ) {
        List<Image> images = client.listImagesCmd().withShowAll( true ).exec();
        return images.stream().anyMatch( i -> Arrays.asList( i.getRepoDigests() ).contains( imageName ) );
    }


    private Optional<String> createAndStartPolyphenyContainer( DockerClient client ) {
        final String imageName = DockerUtils.getContainerName( host );

        updateStatus( String.format( "Pulling container image %s", imageName ) );
        PullImageResultCallback callback = new PullImageResultCallback();
        client.pullImageCmd( imageName ).exec( callback );
        try {
            callback.awaitCompletion();
        } catch ( InterruptedException e ) {
            if ( !hasLocalImage( client, imageName ) ) {
                log.error( "PullImage: ", e );
                updateStatus( "Failed to pull image." );
                return Optional.empty();
            }
            log.info( "Cannot pull image from registry, using cached version" );
            updateStatus( "Using local image." );
        }
        createPolyphenyConnectorVolumeIfNotExists( client );

        updateStatus( "Creating container " + DockerUtils.CONTAINER_NAME );
        HostConfig hostConfig = new HostConfig()
                .withBinds( Bind.parse( DockerUtils.VOLUME_NAME + ":/data" ), Bind.parse( "/var/run/docker.sock:/var/run/docker.sock" ) )
                .withPortBindings( PortBinding.parse( "7001:7001" ), PortBinding.parse( "7002:7002" ), PortBinding.parse( "7003:7003" ) )
                .withRestartPolicy( RestartPolicy.unlessStoppedRestart() );

        CreateContainerResponse containerResponse = client.createContainerCmd( imageName )
                .withExposedPorts( ExposedPort.tcp( ConfigDocker.COMMUNICATION_PORT ), ExposedPort.tcp( ConfigDocker.HANDSHAKE_PORT ), ExposedPort.tcp( ConfigDocker.PROXY_PORT ) )
                .withHostConfig( hostConfig )
                .withName( DockerUtils.CONTAINER_NAME )
                .withCmd( "server" )
                .exec();
        String uuid = containerResponse.getId();
        client.startContainerCmd( uuid ).exec();
        return Optional.of( uuid );
    }


    private String createAndStartHandshakeCommand( DockerClient client, String containerUuid ) {
        ExecCreateCmdResponse execResponse = client.execCreateCmd( containerUuid ).withCmd( "./main", "handshake", HandshakeManager.getInstance().getHandshakeParameters( "localhost" ) ).withAttachStdin( true ).withAttachStderr( true ).exec();

        client.execStartCmd( execResponse.getId() ).exec( new ResultCallback<Frame>() {
            @Override
            public void onStart( Closeable closeable ) {
            }


            @Override
            public void onNext( Frame object ) {
            }


            @Override
            public void onError( Throwable throwable ) {
            }


            @Override
            public void onComplete() {
            }


            @Override
            public void close() {
            }
        } );

        return execResponse.getId();
    }


    private void doAutoHandshake() {
        updateStatus( "Starting automatic setup procedure" );
        DockerClient client = getClient();
        Optional<String> maybeUuid = findAndStartPolyphenyContainer( client );

        if ( maybeUuid.isEmpty() ) {
            maybeUuid = createAndStartPolyphenyContainer( client );
            if ( maybeUuid.isEmpty() ) {
                return;
            }
        }

        String execId = createAndStartHandshakeCommand( client, maybeUuid.get() );
        updateStatus( "Performing handshake with container" );
        HandshakeManager.getInstance().restartOrGetHandshake( "localhost" );
        int retries = 0;
        while ( true ) {
            String handshakeStatus = HandshakeManager.getInstance().getHandshake( "localhost" ).get( "status" );
            if ( handshakeStatus.equals( "FAILED" ) || handshakeStatus.equals( "SUCCESS" ) ) {
                break;
            }
            if ( handshakeStatus.equals( "NOT_RUNNING" ) ) {
                InspectExecResponse s = client.inspectExecCmd( execId ).exec();
                if ( s.getExitCodeLong() != null ) {
                    // 137 seems to be the code of an OOM kill, this happens often during tests, so try again
                    if ( s.getExitCodeLong() == 137 && retries < 3 ) {
                        retries += 1;
                        updateStatus( "Handshake process killed, retry " + retries + " of 3" );
                        execId = createAndStartHandshakeCommand( client, maybeUuid.get() );
                        continue;
                    }
                    updateStatus( "Command failed with exit code " + s.getExitCodeLong() );
                    break;
                }
                HandshakeManager.getInstance().restartOrGetHandshake( "localhost" );
            }
            try {
                TimeUnit.SECONDS.sleep( 1 );
            } catch ( InterruptedException e ) {
                // no problem
            }

        }
    }


    public boolean doAutoConnect() {
        if ( !isAvailable() ) {
            return false;
        }

        if ( isConnected() ) {
            return true;
        }

        Optional<Map.Entry<Integer, DockerInstance>> maybeDockerInstance = DockerManager.getInstance().getDockerInstances().entrySet().stream().filter( e -> e.getValue().getHost().hostname().equals( "localhost" ) ).findFirst();

        if ( maybeDockerInstance.isPresent() ) {
            DockerReconnectResult res = DockerSetupHelper.reconnectToInstance( maybeDockerInstance.get().getKey() );

            if ( !res.getError().isEmpty() ) {
                log.info( "AutoDocker: Reconnect failed: " + res.getError() );
                updateStatus( "error: " + res.getError() );
                return false;
            }
        } else {
            DockerSetupResult res = DockerSetupHelper.newDockerInstance( host.hostname(), host.alias(), host.registry(), host.communicationPort(), host.handshakePort(), host.proxyPort(), false );

            if ( res.isSuccess() ) {
                return true;
            }

            if ( !res.getError().isEmpty() ) {
                log.info( "AutoDocker: Setup failed: " + res.getError() );
                updateStatus( "setup failed: " + res.getError() );
                return false;
            }
        }

        // If it is not successful and not an error, a handshake needs to be done
        synchronized ( this ) {
            if ( thread == null || !thread.isAlive() ) {
                thread = new Thread( this::doAutoHandshake );
                thread.start();
            }
        }
        // thread != null
        while ( thread.isAlive() ) {
            try {
                thread.join();
            } catch ( InterruptedException e ) {
                // no problem
            }
        }
        return isConnected();
    }


    private boolean isConnected() {
        return DockerManager.getInstance().getDockerInstances().values().stream().anyMatch( d -> d.getHost().hostname().equals( "localhost" ) && d.isConnected() );
    }


    public boolean isAvailable() {
        try {
            DockerClient client = getClient();
            client.pingCmd().exec();
            return true;
        } catch ( Exception e ) {
            return false;
        }
    }


    public Map<String, Object> getStatus() {
        return Map.of(
                "available", isAvailable(),
                "connected", isConnected(),
                "running", thread != null && thread.isAlive(),
                "message", status
        );
    }


    private DockerClient getClient() {
        DockerClientConfig config = DefaultDockerClientConfig
                .createDefaultConfigBuilder()
                .build();

        ApacheDockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost( config.getDockerHost() )
                .sslConfig( config.getSSLConfig() )
                .responseTimeout( Duration.ofSeconds( RuntimeConfig.DOCKER_TIMEOUT.getInteger() ) )
                .connectionTimeout( Duration.ofSeconds( RuntimeConfig.DOCKER_TIMEOUT.getInteger() ) )
                .build();

        return DockerClientImpl.getInstance( config, httpClient );
    }

}
