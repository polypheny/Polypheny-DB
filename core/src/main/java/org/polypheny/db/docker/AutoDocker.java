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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;

@Slf4j
public final class AutoDocker {

    private static final AutoDocker INSTANCE = new AutoDocker();

    private String status = "";

    private Thread thread = null;


    private AutoDocker() {
    }


    public static AutoDocker getInstance() {
        return INSTANCE;
    }


    private void createPolyphenyConnectorVolumeIfNotExists( DockerClient client ) {
        List<InspectVolumeResponse> volumes = client.listVolumesCmd().exec().getVolumes();

        for ( InspectVolumeResponse vol : volumes ) {
            if ( vol.getName().equals( "polypheny-docker-connector-data" ) ) {
                return;
            }
        }

        client.createVolumeCmd().withName( "polypheny-docker-connector-data" ).exec();
    }


    private Optional<String> findPolyphenyContainer( DockerClient client ) {
        List<Container> resp = client.listContainersCmd().exec();
        for ( Container c : resp ) {
            for ( String name : c.getNames() ) {
                if ( name.equals( "/polypheny-docker-connector" ) ) {
                    return Optional.of( c.getId() );
                }
            }
        }
        return Optional.empty();
    }


    private Optional<String> createAndStartPolyphenyContainer( DockerClient client ) {
        status = "Pulling container image...";
        PullImageResultCallback callback = new PullImageResultCallback();
        client.pullImageCmd( "polypheny/polypheny-docker-connector" ).exec( callback );
        try {
            callback.awaitCompletion();
        } catch ( InterruptedException e ) {
            log.error( "PullImage: ", e );
            status = "Error while pulling image";
            return Optional.empty();
        }
        createPolyphenyConnectorVolumeIfNotExists( client );

        status = "Creating container...";
        HostConfig hostConfig = new HostConfig();
        hostConfig.withBinds( Bind.parse( "polypheny-docker-connector-data:/data" ), Bind.parse( "/var/run/docker.sock:/var/run/docker.sock" ) );
        hostConfig.withPortBindings( PortBinding.parse( "7001:7001" ), PortBinding.parse( "7002:7002" ) );

        CreateContainerResponse containerResponse = client.createContainerCmd( "polypheny/polypheny-docker-connector" )
                .withExposedPorts( ExposedPort.tcp( ConfigDocker.COMMUNICATION_PORT ), ExposedPort.tcp( ConfigDocker.HANDSHAKE_PORT ) )
                .withHostConfig( hostConfig )
                .withName( "polypheny-docker-connector" )
                .withCmd( "server" )
                .exec();
        String uuid = containerResponse.getId();
        client.startContainerCmd( uuid ).exec();
        return Optional.of( uuid );
    }


    private void doAutoHandshake() {
        status = "Starting...";
        DockerClient client = getClient();
        Optional<String> maybeUuid = findPolyphenyContainer( client );

        if ( maybeUuid.isEmpty() ) {
            maybeUuid = createAndStartPolyphenyContainer( client );
            if ( maybeUuid.isEmpty() ) {
                return;
            }
        }

        String polyphenyDockerUuid = maybeUuid.get();
        status = "Starting handshake...";
        HandshakeManager.getInstance().cancelHandshake( "localhost" );
        HandshakeManager.getInstance().startOrGetHandshake( "localhost", ConfigDocker.COMMUNICATION_PORT, ConfigDocker.HANDSHAKE_PORT );
        ExecCreateCmdResponse execResponse = client.execCreateCmd( polyphenyDockerUuid ).withCmd( "./main", "handshake", HandshakeManager.getInstance().getHandshakeParameters( "localhost" ) ).exec();
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
        for ( int i = 0; i < 20; i++ ) {
            String status = HandshakeManager.getInstance().getHandshake( "localhost" ).get( "status" );
            if ( status.equals( "RUNNING" ) ) {
                try {
                    TimeUnit.SECONDS.sleep( 1 );
                } catch ( InterruptedException e ) {
                    // no problem
                }
            }
        }
    }


    public boolean start() {
        if ( isConnected() ) {
            return true;
        }

        if ( !isAvailable() ) {
            return false;
        }

        try {
            DockerSetupHelper.tryConnectDirectly( "localhost" );
            DockerManager.addDockerInstance( "localhost", "localhost", ConfigDocker.COMMUNICATION_PORT );
            return true;
        } catch ( IOException e ) {
            // Need a new handshake
        }

        synchronized ( this ) {
            if ( thread == null || !thread.isAlive() ) {
                Runnable r = this::doAutoHandshake;
                thread = new Thread( r );
                thread.start();
            }
        }
        // thread != null
        while ( thread.isAlive() ) {
            try {
                TimeUnit.SECONDS.sleep( 1 );
            } catch ( InterruptedException e ) {
                // no problem
            }
        }
        return isConnected();
    }


    private boolean isConnected() {
        return RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class )
                .stream()
                .filter( c -> c.getHost().equals( "localhost" ) )
                .anyMatch( c -> DockerManager.getInstance().getInstanceById( c.getId() ).map( DockerInstance::isConnected ).orElse( false ) );
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
        boolean isRunning;
        synchronized ( this ) {
            isRunning = thread != null && thread.isAlive();
        }
        return Map.of(
                "available", isAvailable(),
                "connected", isConnected(),
                "running", isRunning,
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
                .build();

        return DockerClientImpl.getInstance( config, httpClient );
    }


}
