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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.docker.PolyphenyHandshakeClient.State;
import org.polypheny.db.docker.models.DockerHost;

@Slf4j
public final class HandshakeManager {

    private static final HandshakeManager INSTANCE = new HandshakeManager();
    private final Map<String, Handshake> handshakes = new HashMap<>();


    private HandshakeManager() {
    }


    public static HandshakeManager getInstance() {
        return INSTANCE;
    }


    Map<String, String> newHandshake( DockerHost host, Runnable onCompletion, boolean startHandshake ) {
        synchronized ( this ) {
            Handshake old = handshakes.remove( host.hostname() );
            if ( old != null ) {
                old.cancel();
            }

            try {
                Handshake h = new Handshake( host, onCompletion );
                handshakes.put( host.hostname(), h );
                if ( startHandshake ) {
                    h.startOrRestart();
                }
                return h.serializeHandshake();
            } catch ( IOException e ) {
                throw new GenericRuntimeException( e );
            }
        }
    }


    public Map<String, String> restartOrGetHandshake( String hostname ) {
        synchronized ( this ) {
            Handshake h = handshakes.get( hostname );
            if ( h == null ) {
                throw new GenericRuntimeException( "No handshake for hostname " + hostname );
            }
            try {
                h.startOrRestart();
                return h.serializeHandshake();
            } catch ( IOException e ) {
                throw new GenericRuntimeException( e );
            }
        }
    }


    public boolean cancelHandshake( String hostname ) {
        synchronized ( this ) {
            Handshake h = handshakes.remove( DockerUtils.normalizeHostname( hostname ) );
            if ( h != null ) {
                h.cancel();
            }
            return h != null;
        }
    }


    public Map<String, String> getHandshake( String hostname ) {
        synchronized ( this ) {
            return handshakes.get( DockerUtils.normalizeHostname( hostname ) ).serializeHandshake();
        }
    }


    String getHandshakeParameters( String hostname ) {
        synchronized ( this ) {
            return handshakes.get( DockerUtils.normalizeHostname( hostname ) ).getHandshakeParameters();
        }
    }


    private static class Handshake {

        private Thread handshakeThread = null;
        private DockerHost host;
        private boolean containerRunningGuess;
        private PolyphenyHandshakeClient client;
        private final Runnable onCompletion;
        private boolean cancelled = false;
        private final AtomicLong timeout = new AtomicLong();


        private Handshake( DockerHost host, Runnable onCompletion ) throws IOException {
            this.host = host;
            this.client = new PolyphenyHandshakeClient( host.hostname(), host.handshakePort(), timeout, onCompletion );
            this.onCompletion = onCompletion;
        }


        private boolean guessIfContainerExists() {
            try {
                Socket s = new Socket();
                s.connect( new InetSocketAddress( host.hostname(), host.communicationPort() ), 5000 );
                s.close();
                return true;
            } catch ( IOException ignore ) {
                return false;
            }
        }


        private Map<String, String> serializeHandshake() {
            return Map.of( "hostname", host.hostname(), "runCommand", getRunCommand(), "execCommand", getExecCommand(), "status", client.getState().toString(), "lastErrorMessage", client.getLastErrorMessage(), "containerExists", containerRunningGuess ? "true" : "false" );
        }


        private void startOrRestart() throws IOException {
            if ( cancelled || client.getState() == State.SUCCESS ) {
                return;
            }
            synchronized ( this ) {
                if ( client.getState() == State.FAILED ) {
                    client = new PolyphenyHandshakeClient( host.hostname(), host.handshakePort(), timeout, onCompletion );
                    // No issue, if the client is in failed state, it will not return a success
                    handshakeThread = null;
                }
                containerRunningGuess = guessIfContainerExists();
                timeout.set( Instant.now().getEpochSecond() + 20 );
                if ( handshakeThread == null || !handshakeThread.isAlive() ) {
                    if ( client.getState() == State.NOT_RUNNING ) {
                        client.prepareNextTry();
                    }
                    Runnable doHandshake = () -> {
                        if ( client.doHandshake() ) {
                            log.info( "Handshake with " + host.hostname() + " successful" );
                        }
                    };
                    handshakeThread = new Thread( doHandshake );
                    handshakeThread.start();
                }
            }
        }


        private void cancel() {
            synchronized ( this ) {
                timeout.set( 0 );
                handshakeThread.interrupt();
                cancelled = true;
            }
        }


        private String getPortMapping() {
            return Stream.of(
                    new int[]{ host.communicationPort(), ConfigDocker.COMMUNICATION_PORT },
                    new int[]{ host.handshakePort(), ConfigDocker.HANDSHAKE_PORT },
                    new int[]{ host.proxyPort(), ConfigDocker.PROXY_PORT }
            ).map( p -> String.format( "-p %d:%d", p[0], p[1] ) ).collect( Collectors.joining( " " ) );
        }


        // Don't forget to change AutoDocker as well!
        private String getRunCommand() {
            return "docker run -d -v " + DockerUtils.VOLUME_NAME + ":/data -v /var/run/docker.sock:/var/run/docker.sock " + getPortMapping() + " --restart unless-stopped --name polypheny-docker-connector " + DockerUtils.getContainerName( host ) + " server " + client.getHandshakeParameters();
        }


        // Don't forget to change AutoDocker as well!
        private String getExecCommand() {
            return "docker exec -it " + DockerUtils.CONTAINER_NAME + " ./main handshake " + client.getHandshakeParameters();
        }


        private String getHandshakeParameters() {
            return client.getHandshakeParameters();
        }

    }

}
