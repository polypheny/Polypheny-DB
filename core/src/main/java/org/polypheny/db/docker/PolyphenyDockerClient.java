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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;

final class PolyphenyDockerClient {

    private final Socket con;
    private final PolyphenyTlsClient client;
    private final InputStream in;
    private final OutputStream out;
    private final String dockerId;

    private static final int VERSION = 1; // In sync with polypheny-docker-connector
    private final transient AtomicInteger counter;

    @Getter
    private boolean connected;


    PolyphenyDockerClient( String hostname, int port, PolyphenyKeypair kp, byte[] serverCertificate ) throws IOException {
        con = new Socket();
        con.connect( new InetSocketAddress( hostname, port ), 5000 );
        this.client = new PolyphenyTlsClient( kp, serverCertificate, con.getInputStream(), con.getOutputStream() );

        this.in = client.getInputStream().get();
        this.out = client.getOutputStream().get();

        this.counter = new AtomicInteger();

        VersionResponse resp = executeRequest( newRequest().setVersion( VersionRequest.newBuilder().setVersion( VERSION ) ) ).getVersion();
        if ( resp.getVersion() != VERSION ) {
            throw new IOException( String.format( "Version mismatch: try to update either the docker container or Polypheny (docker container is version %d, Polypheny version is %d)", resp.getVersion(), VERSION ) );
        }
        this.dockerId = resp.getUuid();
        this.connected = true;
    }


    String getDockerId() {
        return dockerId;
    }


    void close() {
        connected = false;
        client.close();

        try {
            con.close();
        } catch ( IOException ignore ) {
        }
    }


    private void checkConnected() throws IOException {
        if ( !connected ) {
            throw new IOException( "PolyphenyDockerClient is not connected" );
        }
    }


    private Response executeRequest( Request.Builder r ) throws IOException {
        Request req = r.build();
        Response resp;
        synchronized ( this ) {
            try {
                req.writeDelimitedTo( out );
                resp = Response.parseDelimitedFrom( in );
            } catch ( IOException e ) {
                connected = false;
                throw e;
            }
            // https://protobuf.dev/reference/java/api-docs/com/google/protobuf/Parser.html#parseFrom-java.io.InputStream-
            if ( resp == null ) {
                connected = false;
                throw new IOException( "EOF" );
            }
        }
        if ( req.getMessageId() != resp.getMessageId() ) {
            throw new IOException( "Response has the wrong message ID" );
        }
        return resp;
    }


    private Request.Builder newRequest() {
        return Request.newBuilder().setMessageId( counter.getAndIncrement() );
    }


    private PortMaps createPortMap( List<Integer> ports ) {
        List<PortMap> portMaps = ports.stream()
                .map( ( p ) -> PortMap.newBuilder().setContainerPort( p ).setProto( "tcp" ).addBindings( PortBinding.newBuilder().setHostIp( "0.0.0.0" ).build() ).build() )
                .collect( Collectors.toList() );
        return PortMaps.newBuilder().addAllMappings( portMaps ).build();
    }


    /**
     * Used by ContainerBuilder.deploy()
     */
    String createAndStartContainer( String containerName, String imageName, List<Integer> ports, List<String> initCommand, Map<String, String> environmentVariables, List<String> volumes ) throws IOException {
        checkConnected();
        PortMaps portMaps = createPortMap( ports );
        CreateContainerRequest ccr = CreateContainerRequest
                .newBuilder()
                .setContainerName( containerName )
                .setImageName( imageName )
                .setPorts( portMaps )
                .setInitCommand( StringList.newBuilder().addAllStrings( initCommand ).build() )
                .putAllEnvironmentVariables( environmentVariables )
                .setVolumes( StringList.newBuilder().addAllStrings( volumes ).build() )
                .build();

        CreateContainerResponse resp = executeRequest( newRequest().setCreateContainer( ccr ) ).getCreateContainer();

        if ( !resp.getError().isEmpty() ) {
            throw new IOException( "Create Container: " + resp.getError() );
        }
        // Assert correct message
        return resp.getUuid();
    }


    void startContainer( String uuid ) throws IOException {
        checkConnected();
        StartContainerRequest cr = StartContainerRequest
                .newBuilder()
                .setUuid( uuid )
                .build();

        StartContainerResponse resp = executeRequest( newRequest().setStartContainer( cr ) ).getStartContainer();

        if ( !resp.getError().isEmpty() ) {
            throw new IOException( resp.getError() );
        }
    }


    /**
     * Get the status string of the container.  See <a href="https://docs.docker.com/engine/api/v1.43/#tag/Container/operation/ContainerInspect">Docker API</a>
     */
    String getContainerStatus( String uuid ) throws IOException {
        checkConnected();
        InspectContainerRequest ir = InspectContainerRequest.newBuilder().setUuid( uuid ).build();
        InspectContainerResponse resp = executeRequest( newRequest().setInspectContainer( ir ) ).getInspectContainer();

        if ( !resp.getError().isEmpty() ) {
            throw new IOException( resp.getError() );
        }
        return resp.getStatus();
    }


    void stopContainer( String uuid ) throws IOException {
        checkConnected();
        StopContainerRequest cr = StopContainerRequest
                .newBuilder()
                .setUuid( uuid )
                .build();

        StopContainerResponse resp = executeRequest( newRequest().setStopContainer( cr ) ).getStopContainer();

        if ( !resp.getError().isEmpty() ) {
            throw new IOException( resp.getError() );
        }
    }


    /**
     * Deletes a container.  If it is running, it will be stopped forcefully.
     */
    void deleteContainer( String uuid ) throws IOException {
        checkConnected();
        DeleteContainerRequest dr = DeleteContainerRequest.newBuilder().setUuid( uuid ).build();
        DeleteContainerResponse resp = executeRequest( newRequest().setDeleteContainer( dr ) ).getDeleteContainer();

        if ( !resp.getError().isEmpty() ) {
            throw new IOException( resp.getError() );
        }
    }


    /* TODO: This leaks ContainerInfo outside */
    List<ContainerInfo> listContainers() throws IOException {
        checkConnected();
        ListContainersRequest lr = ListContainersRequest.newBuilder().build();
        ListContainersResponse resp = executeRequest( newRequest().setListContainers( lr ) ).getListContainers();

        if ( !resp.getError().isEmpty() ) {
            throw new IOException( resp.getError() );
        }

        return resp.getContainersList();
    }


    private Map<Integer, Integer> getPort( PortMaps maps ) {
        Map<Integer, Integer> result = new HashMap<>();
        for ( PortMap map : maps.getMappingsList() ) {
            for ( PortBinding binding : map.getBindingsList() ) {
                if ( binding.getHostIp().equals( "0.0.0.0" ) ) {
                    result.put( map.getContainerPort(), binding.getHostPort() );
                }
            }
        }
        return result;
    }


    Map<String, Map<Integer, Integer>> getPorts( List<String> uuids ) throws IOException {
        checkConnected();
        GetPortsRequest gr = GetPortsRequest.newBuilder().addAllUuid( uuids ).build();
        Response resp = executeRequest( newRequest().setGetPorts( gr ) );
        Map<String, Map<Integer, Integer>> result = new HashMap<>();
        for ( Map.Entry<String, PortMapOrError> entry : resp.getGetPorts().getPortsMap().entrySet() ) {
            if ( entry.getValue().hasError() ) {
                result.put( entry.getKey(), Map.of() );
                continue;
            }
            result.put( entry.getKey(), getPort( entry.getValue().getOk() ) );
        }
        return result;
    }


    int executeCommand( String uuid, List<String> command ) throws IOException {
        checkConnected();
        ExecuteCommandRequest er = ExecuteCommandRequest
                .newBuilder()
                .setUuid( uuid )
                .setCommand( StringList.newBuilder().addAllStrings( command ) )
                .build();
        ExecuteCommandResponse resp = executeRequest( newRequest().setExecuteCommand( er ) ).getExecuteCommand();

        if ( !resp.getError().isEmpty() ) {
            throw new IOException( resp.getError() );
        }

        return resp.getExitCode();
    }


    void createVolume( String driver, String uniqueName, Map<String, String> options ) throws IOException {
        checkConnected();
        CreateVolumeRequest vr = CreateVolumeRequest
                .newBuilder()
                .setDriver( driver )
                .setName( uniqueName )
                .putAllOptions( options )
                .build();

        CreateVolumeResponse resp = executeRequest( newRequest().setCreateVolume( vr ) ).getCreateVolume();

        if ( !resp.getError().isEmpty() ) {
            throw new IOException( resp.getError() );
        }
    }


    void deleteVolume( String uniqueName ) throws IOException {
        checkConnected();
        DeleteVolumeRequest dv = DeleteVolumeRequest
                .newBuilder()
                .setName( uniqueName )
                .build();

        DeleteVolumeResponse resp = executeRequest( newRequest().setDeleteVolume( dv ) ).getDeleteVolume();

        if ( !resp.getError().isEmpty() ) {
            throw new IOException( resp.getError() );
        }
    }


    void ping() throws IOException {
        checkConnected();
        PingRequest lr = PingRequest.newBuilder().build();
        PingResponse resp = executeRequest( newRequest().setPing( lr ) ).getPing();

        if ( !resp.getError().isEmpty() ) {
            throw new IOException( resp.getError() );
        }
    }

}
