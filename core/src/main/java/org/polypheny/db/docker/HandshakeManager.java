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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.ConfigManager;

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
        if ( newHostname.equals( "" ) ) {
            throw new RuntimeException( "invalid hostname \"" + newHostname + "\"" );
        }
        return newHostname;
    }


    public Map<String, String> startHandshake( String hostname, int communicationPort, int handshakePort ) {
        hostname = normalizeHostname( hostname );
        Handshake h;
        synchronized ( this ) {
            if ( !handshakes.containsKey( hostname ) ) {
                try {
                    handshakes.putIfAbsent( hostname, new Handshake( hostname, communicationPort, handshakePort ) );
                } catch ( IOException e ) {
                    throw new RuntimeException( e );
                }
            }
            h = handshakes.get( hostname );
        }
        h.start();
        return h.serializeHandshake();
    }


    public Map<String, String> redoHandshake( String hostname, int communicationPort, int handshakePort ) {
        synchronized ( this ) {
            handshakes.remove( hostname );
        }
        return startHandshake( hostname, communicationPort, handshakePort );
    }


    public List<Map<String, String>> getHandshakes() {
        List<Map<String, String>> h = new ArrayList<>();
        for ( Map.Entry<String, Handshake> entry : handshakes.entrySet() ) {
            h.add( entry.getValue().serializeHandshake() );
        }
        return h;
    }


    public Map<String, String> getHandshake( String hostname ) {
        return handshakes.get( hostname ).serializeHandshake();
    }


    String getHandshakeParameters( String hostname ) {
        return handshakes.get( hostname ).getHandshakeParameters();
    }


    private static class Handshake {

        private Thread handshakeThread;
        private final String hostname;
        private final int communicationPort;
        private boolean containerRunningGuess;
        private final PolyphenyHandshakeClient client;


        private Handshake( String hostname, int communicationPort, int handshakePort ) throws IOException {
            this.hostname = hostname;
            this.communicationPort = communicationPort;
            client = new PolyphenyHandshakeClient( hostname, handshakePort );
            handshakeThread = null;
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


        private synchronized void start() {
            if ( handshakeThread == null || !handshakeThread.isAlive() ) {
                Runnable doHandshake = () -> {
                    if ( !client.doHandshake() ) {
                        log.info( "Handshake failed" );
                    } else {
                        // TODO: racy
                        log.info( "Saving docker config" );
                        List<ConfigDocker> configList = ConfigManager.getInstance().getConfig( "runtime/dockerInstances" ).getList( ConfigDocker.class );
                        for ( ConfigDocker c : configList ) {
                            if ( c.getHost().equals( hostname ) && c.getPort() == communicationPort ) {
                                c.setDockerRunning( true );
                                return;
                            }
                        }
                        // Add a new entry
                        configList.add( new ConfigDocker( hostname, communicationPort ).setDockerRunning( true ) );
                        ConfigManager.getInstance().getConfig( "runtime/dockerInstances" ).setConfigObjectList( configList.stream().map( ConfigDocker::toMap ).collect( Collectors.toList() ), ConfigDocker.class );
                    }
                };
                containerRunningGuess = guessIfContainerExists();
                handshakeThread = new Thread( doHandshake );
                handshakeThread.start();
            }
        }


        // Don't forget to change AutoDocker as well!
        private String getRunCommand() {
            return "docker run -d -v polypheny-docker-connector-data:/data -v /var/run/docker.sock:/var/run/docker.sock -p 7001:7001 -p 7002:7002 --name polypheny-docker-connector polypheny/polypheny-docker-connector server " + client.getHandshakeParameters();
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
