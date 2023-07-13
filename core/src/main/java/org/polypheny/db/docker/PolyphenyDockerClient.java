package org.polypheny.db.docker;

import java.net.Socket;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
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
        con = new Socket( hostname, port );
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
        client.close();

        try {
            con.close();
        } catch ( IOException ignore ) {
        }
    }


    ContainerBuilder newBuilder( String imageName, String uniqueName ) {
        return new ContainerBuilder( imageName, uniqueName );
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
    private String createAndStartContainer( String containerName, String imageName, List<Integer> ports, List<String> initCommand, Map<String, String> environmentVariables, List<String> volumes ) throws IOException {

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

        if ( !resp.getError().equals( "" ) ) {
            throw new IOException( "Create Container: " + resp.getError() );
        }
        // Assert correct message
        return resp.getUuid();
    }


    void startContainer( String uuid ) throws IOException {
        StartContainerRequest cr = StartContainerRequest
                .newBuilder()
                .setUuid( uuid )
                .build();

        StartContainerResponse resp = executeRequest( newRequest().setStartContainer( cr ) ).getStartContainer();

        if ( !resp.getError().equals( "" ) ) {
            throw new IOException( resp.getError() );
        }
    }


    /**
     * Get the status string of the container.  See <a href="https://docs.docker.com/engine/api/v1.43/#tag/Container/operation/ContainerInspect">Docker API</a>
     */
    String getContainerStatus( String uuid ) throws IOException {
        InspectContainerRequest ir = InspectContainerRequest.newBuilder().setUuid( uuid ).build();
        InspectContainerResponse resp = executeRequest( newRequest().setInspectContainer( ir ) ).getInspectContainer();

        if ( !resp.getError().equals( "" ) ) {
            throw new IOException( resp.getError() );
        }
        return resp.getStatus();
    }


    void stopContainer( String uuid ) throws IOException {
        StopContainerRequest cr = StopContainerRequest
                .newBuilder()
                .setUuid( uuid )
                .build();

        StopContainerResponse resp = executeRequest( newRequest().setStopContainer( cr ) ).getStopContainer();

        if ( !resp.getError().equals( "" ) ) {
            throw new IOException( resp.getError() );
        }
    }


    /**
     * Deletes a container.  If it is running, it will be stopped forcefully.
     */
    void deleteContainer( String uuid ) throws IOException {
        DeleteContainerRequest dr = DeleteContainerRequest.newBuilder().setUuid( uuid ).build();
        DeleteContainerResponse resp = executeRequest( newRequest().setDeleteContainer( dr ) ).getDeleteContainer();

        if ( !resp.getError().equals( "" ) ) {
            throw new IOException( resp.getError() );
        }
    }


    /* TODO: This leaks ContainerInfo outside */
    List<ContainerInfo> listContainers() throws IOException {
        ListContainersRequest lr = ListContainersRequest.newBuilder().build();
        ListContainersResponse resp = executeRequest( newRequest().setListContainers( lr ) ).getListContainers();

        if ( !resp.getError().equals( "" ) ) {
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
        ExecuteCommandRequest er = ExecuteCommandRequest
                .newBuilder()
                .setUuid( uuid )
                .setCommand( StringList.newBuilder().addAllStrings( command ) )
                .build();
        ExecuteCommandResponse resp = executeRequest( newRequest().setExecuteCommand( er ) ).getExecuteCommand();

        if ( !resp.getError().equals( "" ) ) {
            throw new IOException( resp.getError() );
        }

        return resp.getExitCode();
    }


    void createVolume( String driver, String uniqueName, Map<String, String> options ) throws IOException {
        CreateVolumeRequest vr = CreateVolumeRequest
                .newBuilder()
                .setDriver( driver )
                .setName( uniqueName )
                .putAllOptions( options )
                .build();

        CreateVolumeResponse resp = executeRequest( newRequest().setCreateVolume( vr ) ).getCreateVolume();

        if ( !resp.getError().equals( "" ) ) {
            throw new IOException( resp.getError() );
        }
    }


    void deleteVolume( String uniqueName ) throws IOException {
        DeleteVolumeRequest dv = DeleteVolumeRequest
                .newBuilder()
                .setName( uniqueName )
                .build();

        DeleteVolumeResponse resp = executeRequest( newRequest().setDeleteVolume( dv ) ).getDeleteVolume();

        if ( !resp.getError().equals( "" ) ) {
            throw new IOException( resp.getError() );
        }
    }


    void ping() throws IOException {
        PingRequest lr = PingRequest.newBuilder().build();
        PingResponse resp = executeRequest( newRequest().setPing( lr ) ).getPing();

        if ( !resp.getError().equals( "" ) ) {
            throw new IOException( resp.getError() );
        }
    }


    final class ContainerBuilder {

        private final String containerName;
        private final String imageName;
        private final List<Integer> exposedPorts;
        private List<String> initCommand;
        private final Map<String, String> environmentVariables;
        private final List<String> volumes;


        /**
         * A Builder for a new container.
         *
         * @param imageName Name of the image to use as a base
         * @param uniqueName The name of the container to create.  Must be unique for that docker instance, globally unique would be even better.
         * Must start with "polypheny_" followed by the Polypheny UUID and another underscore.
         */
        private ContainerBuilder( String imageName, String uniqueName ) {
            this.containerName = DockerContainer.getPhysicalUniqueName( uniqueName );
            this.imageName = imageName;
            this.exposedPorts = new ArrayList<>();
            this.initCommand = List.of();
            this.environmentVariables = new HashMap<>();
            this.volumes = new ArrayList<>();
        }


        /**
         * Creates the container.  The possible exception reasons
         * include that a container with that name already exists.
         *
         * @return The UUID of the freshly created container.
         */
        public String deploy() throws IOException {
            return createAndStartContainer( containerName, imageName, exposedPorts, initCommand, environmentVariables, volumes );
        }


        /**
         * Sets the initial command of the container.
         */
        public ContainerBuilder setInitCommand( List<String> cmd ) {
            initCommand = cmd;
            return this;
        }


        /**
         * Adds a port which Polypheny would like to use.  This is the
         * port inside the container, a separate call to getUsedPorts
         * is required to find out which port is used on the outside.
         */
        public ContainerBuilder addPort( int port ) {
            exposedPorts.add( port );
            return this;
        }


        /**
         * Adds an environment variable.  If the same name is set multiple times, the last value will win.
         */
        public ContainerBuilder putEnvironmentVariable( String name, String value ) {
            environmentVariables.put( name, value );
            return this;
        }


        public ContainerBuilder addVolume( String volume ) {
            volumes.add( volume );
            return this;
        }


    }

}
