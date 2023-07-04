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
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class AutoDocker {

    private static final AutoDocker INSTANCE = new AutoDocker();
    private DockerClient client;


    private AutoDocker() {
        this.client = null;
    }


    public static AutoDocker getInstance() {
        return INSTANCE;
    }


    public void createPolyphenyConnectorVolumeIfNotExists() {
        List<InspectVolumeResponse> volumes = client.listVolumesCmd().exec().getVolumes();

        for ( InspectVolumeResponse vol : volumes ) {
            if ( vol.getName().equals( "polypheny-docker-connector-data" ) ) {
                return;
            }
        }

        client.createVolumeCmd().withName( "polypheny-docker-connector-data" ).exec();
    }


    public boolean doIt() {
        if ( client == null ) {
            return false;
        }
        List<Container> resp = client.listContainersCmd().exec();
        String polyphenDockerUuid = null;
        for ( Container c : resp ) {
            for ( String name : c.getNames() ) {
                if ( name.equals( "/polypheny-docker-connector" ) ) {
                    polyphenDockerUuid = c.getId();
                    break;
                }
            }
        }
        if ( polyphenDockerUuid == null ) {
            PullImageResultCallback callback = new PullImageResultCallback();
            client.pullImageCmd( "polypheny/polypheny-docker-connector" ).exec( callback );
            try {
                callback.awaitCompletion();
            } catch ( InterruptedException e ) {
                log.error( "PullImage: ", e );
                return false;
            }
            createPolyphenyConnectorVolumeIfNotExists();

            HostConfig hostConfig = new HostConfig();
            hostConfig.withBinds( Bind.parse( "polypheny-docker-connector-data:/data" ), Bind.parse( "/var/run/docker.sock:/var/run/docker.sock" ) );
            hostConfig.withPortBindings( PortBinding.parse( "7001:7001" ), PortBinding.parse( "7002:7002" ) );

            CreateContainerResponse containerResponse = client.createContainerCmd( "polypheny/polypheny-docker-connector" )
                    .withExposedPorts( ExposedPort.tcp( 7001 ), ExposedPort.tcp( 7002 ) )
                    .withHostConfig( hostConfig )
                    .withName( "polypheny-docker-connector" )
                    .withCmd( "server" )
                    .exec();
            polyphenDockerUuid = containerResponse.getId();
            client.startContainerCmd( polyphenDockerUuid ).exec();
        }
        HandshakeHelper.getInstance().redoHandshake( "localhost", 7001, 7002 );
        ExecCreateCmdResponse execResponse = client.execCreateCmd( polyphenDockerUuid ).withCmd( "./main", "handshake", HandshakeHelper.getInstance().getHandshakeParameters( "localhost", 7001 ) ).exec();
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
            String status = HandshakeHelper.getInstance().getHandshake( "localhost", 7001 ).get( "status" );
            if ( status.equals( "RUNNING" ) ) {
                try {
                    TimeUnit.SECONDS.sleep( 1 );
                } catch ( InterruptedException e ) {
                    // no problem
                }
                continue;
            }
            return status.equals( "SUCCESS" );
        }
        return false;
    }


    public boolean isAvailable() {
        if ( client != null ) {
            return true;
        }
        try {
            DockerClientConfig config = DefaultDockerClientConfig
                    .createDefaultConfigBuilder()
                    .build();

            ApacheDockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                    .dockerHost( config.getDockerHost() )
                    .sslConfig( config.getSSLConfig() )
                    .build();

            client = DockerClientImpl.getInstance( config, httpClient );
            client.pingCmd().exec();
            log.info( "AutoDocker is available" );
            return true;
        } catch ( Exception e ) {
            log.info( "AutoDocker not available" );
            client = null;
            return false;
        }
    }

}
