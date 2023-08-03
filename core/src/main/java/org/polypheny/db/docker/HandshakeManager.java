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
import java.net.Socket;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.PolyphenyHandshakeClient.State;

@Slf4j
public final class HandshakeManager {

    private static final HandshakeManager INSTANCE = new HandshakeManager();
    private final Map<String, Handshake> handshakes = new HashMap<>();


    private HandshakeManager() {
    }


    public static HandshakeManager getInstance() {
        return INSTANCE;
    }


    private String normalizeHostname( String hostname ) {
        // TODO: add more validation/sanity checks
        String newHostname = hostname.strip();
        if ( newHostname.isEmpty() ) {
            throw new RuntimeException( "invalid hostname \"" + newHostname + "\"" );
        }
        return newHostname;
    }


    Map<String, String> newHandshake( String hostname, String registry, int communicationPort, int handshakePort, int proxyPort, Runnable onCompletion, boolean startHandshake ) {
        hostname = normalizeHostname( hostname );
        synchronized ( this ) {
            Handshake old = handshakes.remove( hostname );
            if ( old != null ) {
                old.cancel();
            }

            try {
                Handshake h = new Handshake( hostname, registry, communicationPort, handshakePort, proxyPort, onCompletion );
                handshakes.put( hostname, h );
                if ( startHandshake ) {
                    h.startOrRestart();
                }
                return h.serializeHandshake();
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }
    }


    public Map<String, String> restartOrGetHandshake( String hostname ) {
        hostname = normalizeHostname( hostname );
        synchronized ( this ) {
            Handshake h = handshakes.get( hostname );
            if ( h == null ) {
                throw new RuntimeException( "No handshake for hostname " + hostname );
            }
            try {
                h.startOrRestart();
                return h.serializeHandshake();
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }
    }


    public boolean cancelHandshake( String hostname ) {
        hostname = normalizeHostname( hostname );
        synchronized ( this ) {
            Handshake h = handshakes.remove( hostname );
            if ( h != null ) {
                h.cancel();
            }
            return h != null;
        }
    }


    public Map<String, String> getHandshake( String hostname ) {
        synchronized ( this ) {
            return handshakes.get( normalizeHostname( hostname ) ).serializeHandshake();
        }
    }


    String getHandshakeParameters( String hostname ) {
        synchronized ( this ) {
            return handshakes.get( normalizeHostname( hostname ) ).getHandshakeParameters();
        }
    }


    private static class Handshake {

        private Thread handshakeThread = null;
        private final String hostname;
        private final String registry;
        private final int communicationPort;
        private final int handshakePort;
        private final int proxyPort;
        private boolean containerRunningGuess;
        private PolyphenyHandshakeClient client;
        private final Runnable onCompletion;
        private boolean cancelled = false;
        private final AtomicLong timeout = new AtomicLong();


        private Handshake( String hostname, String registry, int communicationPort, int handshakePort, int proxyPort, Runnable onCompletion ) throws IOException {
            this.hostname = hostname;
            this.registry = registry;
            this.communicationPort = communicationPort;
            this.handshakePort = handshakePort;
            this.proxyPort = proxyPort;
            this.client = new PolyphenyHandshakeClient( hostname, handshakePort, timeout, onCompletion );
            this.onCompletion = onCompletion;
        }


        private boolean guessIfContainerExists() {
            try {
                Socket s = new Socket( hostname, communicationPort );
                s.close();
                return true;
            } catch ( IOException ignore ) {
                return false;
            }
        }


        private Map<String, String> serializeHandshake() {
            return Map.of( "hostname", hostname, "runCommand", getRunCommand(), "execCommand", getExecCommand(), "status", client.getState().toString(), "lastErrorMessage", client.getLastErrorMessage(), "containerExists", containerRunningGuess ? "true" : "false" );
        }


        private void startOrRestart() throws IOException {
            if ( cancelled || client.getState() == State.SUCCESS ) {
                return;
            }
            synchronized ( this ) {
                if ( client.getState() == State.FAILED ) {
                    client = new PolyphenyHandshakeClient( hostname, handshakePort, timeout, onCompletion );
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
                            log.info( "Handshake with " + hostname + " successful" );
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


        private String getContainerName() {
            final String registryToUse = registry.isEmpty() ? RuntimeConfig.DOCKER_CONTAINER_REGISTRY.getString() : registry;
            if ( registryToUse.isEmpty() || registryToUse.endsWith( "/" ) ) {
                return registryToUse + "polypheny/polypheny-docker-connector";
            } else {
                return registryToUse + "/" + "polypheny/polypheny-docker-connector";
            }
        }


        private String getPortMapping() {
            return Stream.of(
                    new int[]{ communicationPort, ConfigDocker.COMMUNICATION_PORT },
                    new int[]{ handshakePort, ConfigDocker.HANDSHAKE_PORT },
                    new int[]{ proxyPort, ConfigDocker.PROXY_PORT }
            ).map( p -> String.format( "-p %d:%d", p[0], p[1] ) ).collect( Collectors.joining( " " ) );
        }


        // Don't forget to change AutoDocker as well!
        private String getRunCommand() {
            return "docker run -d -v polypheny-docker-connector-data:/data -v /var/run/docker.sock:/var/run/docker.sock " + getPortMapping() + " --restart unless-stopped --name polypheny-docker-connector " + getContainerName() + " server " + client.getHandshakeParameters();
        }


        // Don't forget to change AutoDocker as well!
        private String getExecCommand() {
            return "docker exec -it polypheny-docker-connector ./main handshake " + client.getHandshakeParameters();
        }


        private String getHandshakeParameters() {
            return client.getHandshakeParameters();
        }

    }

}
