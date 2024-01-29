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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.bouncycastle.tls.AlertDescription;
import org.bouncycastle.tls.TlsFatalAlert;
import org.bouncycastle.tls.TlsNoCloseNotifyException;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.util.Util;

/**
 * The container is the main interaction instance for calling classes when interacting with Docker.
 * It holds all information for a specific Container
 */
@Slf4j
public final class DockerContainer {

    private static final Map<String, DockerContainer> containers = new ConcurrentHashMap<>();

    public final String uniqueName;

    /**
     * The UUID of this container.
     */
    @Getter
    private final String containerId;

    private final Map<Integer, ServerSocket> proxies = new HashMap<>();


    public DockerContainer( String containerId, String uniqueName ) {
        this.containerId = containerId;
        this.uniqueName = uniqueName;
        containers.put( containerId, this );
    }


    public static Optional<DockerContainer> getContainerByUUID( String uuid ) {
        return Optional.ofNullable( containers.get( uuid ) );
    }


    private Optional<DockerInstance> getDockerInstance() {
        return DockerManager.getInstance().getInstanceForContainer( containerId );
    }


    public DockerContainer start() throws IOException {
        getDockerInstance()
                .orElseThrow( () -> new IOException( "Cannot start container: not connected to docker host" ) )
                .startContainer( this );
        return this;
    }


    public void stop() throws IOException {
        getDockerInstance()
                .orElseThrow( () -> new IOException( "Cannot stop container: not connected to docker host" ) )
                .stopContainer( this );
    }


    public void destroy() {
        // TODO: When not connected, record these IDs in a list and remove them the next time they are encountered
        getDockerInstance().ifPresent( d -> d.destroyContainer( this ) );
        containers.remove( containerId );
        synchronized ( this ) {
            proxies.values().forEach( Util::closeNoThrow );
        }
    }


    public int execute( List<String> cmd ) throws IOException {
        return getDockerInstance()
                .orElseThrow( () -> new IOException( "Cannot execute command: not connected to docker host" ) )
                .execute( this, cmd );
    }


    public static String getPhysicalUniqueName( String uniqueName ) {
        // while not all Docker containers belong to an adapter we annotate it anyway
        String name = "polypheny_" + RuntimeConfig.INSTANCE_UUID.getString() + "_" + uniqueName;
        if ( Catalog.mode != RunMode.TEST ) {
            name += "_test";
        }
        return name;
    }


    public Optional<String> getHost() {
        return getDockerInstance().map( d -> d.getHost().hostname() );
    }


    public Optional<Integer> getExposedPort( int port ) {
        try {
            Optional<DockerInstance> maybeDockerInstance = getDockerInstance();
            if ( maybeDockerInstance.isPresent() ) {
                Map<Integer, Integer> portMap = maybeDockerInstance.get().getPorts( this );
                return Optional.ofNullable( portMap.getOrDefault( port, null ) );
            }
            throw new IOException();
        } catch ( IOException e ) {
            log.error( "Failed to retrieve list of ports for container " + containerId );
            return Optional.empty();
        }
    }


    private Thread pipe( InputStream in, OutputStream out, String name ) {
        Thread t = new Thread( () -> {
            try ( in; out ) {
                int n;
                do {
                    byte[] buf = new byte[256]; // TODO: Optimize this value
                    n = in.read( buf );
                    if ( n > 0 ) {
                        out.write( buf, 0, n );
                    }
                } while ( n >= 0 );
            } catch ( TlsNoCloseNotifyException ignore ) {
                // ignore
            } catch ( IOException e ) {
                if ( e instanceof SocketException ) {
                    if ( e.getMessage().equals( "Socket closed" ) || e.getMessage().startsWith( "Connection reset by peer" ) ) {
                        return;
                    }
                }
                log.error( "Pipe " + name, e );
            }
        }, name );
        t.start();
        return t;
    }


    private void startProxyForConnection( DockerInstance dockerInstance, Socket local, int port ) {
        final PolyphenyTlsClient client;
        try {
            client = PolyphenyTlsClient.connect( "docker", dockerInstance.getHost().hostname(), dockerInstance.getHost().proxyPort() );
        } catch ( IOException e ) {
            if ( e instanceof SocketException || e instanceof EOFException ) {
                // ignore
            } else if ( e instanceof TlsFatalAlert && ((TlsFatalAlert) e).getAlertDescription() == AlertDescription.handshake_failure ) {
                // ignore
            } else {
                log.info( "startProxyForConnection", e );
            }
            Util.closeNoThrow( local );
            return;
        }
        OutputStream remoteOut = client.getOutputStream().get();
        try {
            remoteOut.write( (containerId + ":" + port + "\n").getBytes( StandardCharsets.UTF_8 ) );
            Thread copyToRemote = pipe( local.getInputStream(), remoteOut, String.format( "polypheny => %s", uniqueName ) );
            Thread copyFromRemote = pipe( client.getInputStream().get(), local.getOutputStream(), String.format( "polypheny <= %s", uniqueName ) );
            new Thread( () -> {
                while ( true ) {
                    try {
                        copyToRemote.join();
                        copyFromRemote.join();
                        break;
                    } catch ( InterruptedException ignore ) {

                    }
                }
                log.info( "Pipe threads done, terminating..." );
                client.close();
            } );
        } catch ( IOException e ) {
            log.info( "startProxyForConnection3", e );
            client.close();
            Util.closeNoThrow( local );
        }
    }


    private ServerSocket startServer( int port ) {
        try {
            ServerSocket server = new ServerSocket( 0, 10, InetAddress.getLoopbackAddress() );
            Runnable r = () -> {
                while ( true ) {
                    try {
                        Socket local = server.accept();
                        DockerInstance dockerInstance = getDockerInstance().orElseThrow( () -> new IOException( "Not connected to docker host" ) );
                        startProxyForConnection( dockerInstance, local, port );
                    } catch ( IOException e ) {
                        if ( !(e instanceof SocketException) || !e.getMessage().equals( "Socket closed" ) ) {
                            log.info( "Server Socket for port " + port + " closed", e );
                        }
                        synchronized ( this ) {
                            Util.closeNoThrow( proxies.remove( port ) );
                        }
                        break;
                    }
                }
            };
            new Thread( r ).start();
            return server;
        } catch ( Exception e ) {
            log.error( "Failed to start local proxy server: ", e );
            throw new GenericRuntimeException( e );
        }
    }


    public HostAndPort connectToContainer( int port ) {
        synchronized ( this ) {
            ServerSocket s = proxies.computeIfAbsent( port, this::startServer );
            return new HostAndPort( s.getInetAddress().getHostAddress(), s.getLocalPort() );
        }
    }


    /**
     * The container gets probed until the defined ready supplier returns true or the timeout is reached
     */
    public boolean waitTillStarted( Supplier<Boolean> isReadySupplier, long maxTimeoutMs ) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        boolean isStarted = isReadySupplier.get();
        while ( !isStarted && (stopWatch.getTime() < maxTimeoutMs) ) {
            try {
                TimeUnit.MILLISECONDS.sleep( 500 );
            } catch ( InterruptedException e ) {
                // ignore
            }
            isStarted = isReadySupplier.get();
        }
        stopWatch.stop();
        return isStarted;
    }


    public record HostAndPort(String host, int port) {

    }

}
