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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.docker.PolyphenyHandshakeClient.State;
import org.polypheny.db.docker.exceptions.DockerUserException;
import org.polypheny.db.docker.models.DockerHost;
import org.polypheny.db.docker.models.HandshakeInfo;

@Slf4j
public final class HandshakeManager {

    private static final HandshakeManager INSTANCE = new HandshakeManager();
    private final Map<Long, Handshake> handshakes = new HashMap<>();


    private HandshakeManager() {
    }


    public static HandshakeManager getInstance() {
        return INSTANCE;
    }


    HandshakeInfo newHandshake( DockerHost host, Runnable onCompletion, boolean startHandshake ) {
        synchronized ( this ) {
            cancelHandshakes( host.hostname() );

            try {
                Handshake h = new Handshake( host, onCompletion );
                handshakes.put( h.getId(), h );
                if ( startHandshake ) {
                    h.startOrRestart();
                }
                return h.serializeHandshake();
            } catch ( IOException e ) {
                throw new GenericRuntimeException( e );
            }
        }
    }


    public HandshakeInfo restartHandshake( long id ) {
        synchronized ( this ) {
            Handshake h = handshakes.get( id );
            if ( h == null ) {
                throw new DockerUserException( 404, "No handshake with id " + id );
            }
            try {
                h.startOrRestart();
                return h.serializeHandshake();
            } catch ( IOException e ) {
                throw new GenericRuntimeException( e );
            }
        }
    }


    void ensureHandshakeIsRunning( long id ) {
        synchronized ( this ) {
            Handshake h = handshakes.get( id );
            if ( h == null ) {
                throw new GenericRuntimeException( "No handshake with id " + id );
            }
            try {
                h.startOrRestart();
            } catch ( IOException e ) {
                throw new GenericRuntimeException( e );
            }
        }
    }


    public boolean cancelAndRemoveHandshake( long id ) {
        synchronized ( this ) {
            Handshake h = handshakes.remove( id );
            if ( h != null ) {
                h.cancel();
            }
            return h != null;
        }
    }


    void cancelHandshakes( String hostname ) {
        synchronized ( this ) {
            List<Long> ids = handshakes.values().stream().filter( h -> h.host.hostname().equals( hostname ) ).map( Handshake::getId ).toList();
            ids.forEach( this::cancelAndRemoveHandshake );
        }
    }


    public Optional<HandshakeInfo> getHandshake( long id ) {
        synchronized ( this ) {
            Handshake h = handshakes.get( id );
            return Optional.ofNullable( h != null ? h.serializeHandshake() : null );
        }
    }


    public List<HandshakeInfo> getActiveHandshakes() {
        synchronized ( this ) {
            return handshakes.values().stream()
                    .map( Handshake::serializeHandshake )
                    .filter( h -> !h.status().equals( "CANCELLED" ) && !h.status().equals( "SUCCESS" ) ).toList();
        }
    }


    String getHandshakeParameters( long id ) {
        synchronized ( this ) {
            return handshakes.get( id ).getHandshakeParameters();
        }
    }


    private static class Handshake {

        private static final AtomicLong ID_BUILDER = new AtomicLong();

        @Getter
        private final long id;
        private Thread handshakeThread = null;
        private final DockerHost host;
        private boolean containerRunningGuess;
        private PolyphenyHandshakeClient client;
        private final Runnable onCompletion;
        private boolean cancelled = false;
        private final AtomicLong timeout = new AtomicLong();


        private Handshake( DockerHost host, Runnable onCompletion ) throws IOException {
            this.id = ID_BUILDER.getAndIncrement();
            this.host = host;
            this.client = new PolyphenyHandshakeClient( host.hostname(), host.handshakePort(), timeout, onCompletion );
            this.onCompletion = onCompletion;
        }


        private boolean guessIfContainerExists() {
            try {
                Socket s = new Socket();
                s.connect( new InetSocketAddress( host.hostname(), host.communicationPort() ), 1000 );
                s.close();
                return true;
            } catch ( IOException ignore ) {
                return false;
            }
        }


        private HandshakeInfo serializeHandshake() {
            return new HandshakeInfo( id, host, getRunCommand(), getExecCommand(), cancelled ? "CANCELLED" : client.getState().toString(), client.getLastErrorMessage(), containerRunningGuess );
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
                            log.info( "Handshake with {} successful", host.hostname() );
                        }
                    };
                    handshakeThread = new Thread( doHandshake, "HandshakeThread" );
                    handshakeThread.start();
                }
            }
        }


        private void cancel() {
            synchronized ( this ) {
                synchronized ( client ) {
                    if ( client.getState() != State.SUCCESS ) {
                        timeout.set( 0 );
                        handshakeThread.interrupt();
                        cancelled = true;
                    }
                }
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
