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
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;

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


    private DockerInstance getDockerInstance() {
        return DockerManager.getInstance().getInstanceForContainer( containerId ).get();
    }


    /**
     * Starts the container
     *
     * @return the started container
     */
    public DockerContainer start() throws IOException {
        getDockerInstance().startContainer( this );
        return this;
    }


    /**
     * Stops the container
     */
    public void stop() throws IOException {
        getDockerInstance().stopContainer( this );
    }


    /**
     * Destroys the container, which stops and removes it from the system
     */
    public void destroy() {
        log.info( "Destroying container with ID " + this.getContainerId() );
        getDockerInstance().destroyContainer( this );
        containers.remove( containerId );
        proxies.forEach( ( k, v ) -> {
            try {
                v.close();
            } catch ( IOException ignore ) {
                // ignore
            }
        } );
    }


    public int execute( List<String> cmd ) throws IOException {
        return getDockerInstance().execute( this, cmd );
    }


    public static String getPhysicalUniqueName( String uniqueName ) {
        // while not all Docker containers belong to an adapter we annotate it anyway
        String name = "polypheny_" + RuntimeConfig.INSTANCE_UUID.getString() + "_" + uniqueName;
        if ( !Catalog.testMode ) {
            return name;
        } else {
            return name + "_test";
        }
    }


    public String getHost() {
        return getDockerInstance().getHost();
    }


    public Optional<Integer> getExposedPort( int port ) {
        try {
            Map<Integer, Integer> portMap = getDockerInstance().getPorts( this );
            return Optional.ofNullable( portMap.getOrDefault( port, null ) );
        } catch ( IOException e ) {
            log.error( "Failed to retrieve list of ports for container " + containerId );
            return Optional.empty();
        }
    }


    private Runnable pipe( InputStream in, OutputStream out, String name ) {
        return () -> {
            try ( in; out ) {
                while ( true ) {
                    byte[] buf = new byte[256];
                    int n = in.read( buf );
                    if ( n >= 0 ) {
                        out.write( buf, 0, n );
                    } else {
                        break;
                    }
                }
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
        };
    }


    private void startProxyForConnection( Socket local, int port ) {
        try {
            Socket remote = new Socket( getHost(), ConfigDocker.PROXY_PORT );
            PolyphenyKeypair kp = PolyphenyCertificateManager.loadClientKeypair( getHost() );
            byte[] serverCert = PolyphenyCertificateManager.loadServerCertificate( getHost() );
            PolyphenyTlsClient client = new PolyphenyTlsClient( kp, serverCert, remote.getInputStream(), remote.getOutputStream() );
            InputStream in = client.getInputStream().get();
            OutputStream out = client.getOutputStream().get();

            out.write( (containerId + ":" + port + "\n").getBytes( StandardCharsets.UTF_8 ) );

            Runnable handleOutput = pipe( local.getInputStream(), out, String.format( "polypheny => %s", uniqueName ) );
            Runnable handleInput = pipe( in, local.getOutputStream(), String.format( "polypheny <= %s", uniqueName ) );
            new Thread( handleOutput ).start();
            new Thread( handleInput ).start();
        } catch ( IOException e ) {
            if ( e instanceof SocketException || e instanceof EOFException ) {
                // ignore
            } else if ( e instanceof TlsFatalAlert && ((TlsFatalAlert) e).getAlertDescription() == AlertDescription.handshake_failure ) {
                // ignore
            } else {
                log.info( "startProxyForConnection: " + e );
            }
            try {
                local.close();
            } catch ( IOException ignore ) {
                // ignore failures during cleanup
            }
        }
    }


    private ServerSocket startServer( int port ) {
        try {
            ServerSocket s = new ServerSocket( 0, 10, InetAddress.getLoopbackAddress() );
            Runnable r = () -> {
                while ( true ) {
                    try {
                        Socket local = s.accept();
                        startProxyForConnection( local, port );
                    } catch ( Exception e ) {
                        if ( e instanceof SocketException && e.getMessage().equals( "Socket closed" ) ) {
                            log.info( "Server Socket for port " + port + " closed" );
                        } else {
                            log.info( "Server Socket for port " + port + " closed", e );
                        }
                        synchronized ( this ) {
                            proxies.remove( port );
                        }
                        break;
                    }
                }
            };
            new Thread( r ).start();
            return s;
        } catch ( Exception e ) {
            log.error( "Failed to start local proxy server: ", e );
            throw new RuntimeException( e );
        }
    }


    public HostAndPort connectToContainer( int port ) {
        synchronized ( this ) {
            ServerSocket s = proxies.get( port );
            if ( s == null ) {
                s = startServer( port );
                proxies.put( port, s );
            }
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


    static public class HostAndPort {

        @Getter
        final String host;
        @Getter
        final int port;


        HostAndPort( String host, int port ) {
            this.host = host;
            this.port = port;
        }

    }

}
