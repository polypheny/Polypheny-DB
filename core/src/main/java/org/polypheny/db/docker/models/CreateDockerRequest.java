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

package org.polypheny.db.docker.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.docker.DockerUtils;
import org.polypheny.db.docker.exceptions.DockerUserException;


public record CreateDockerRequest(@JsonProperty String hostname, @JsonProperty String alias, @JsonProperty String registry,
                                  @JsonProperty Integer communicationPort, @JsonProperty Integer handshakePort, @JsonProperty Integer proxyPort) {

    private static int checkPortIsValid( Integer port, int defaultValue ) {
        if ( port == null ) {
            return defaultValue;
        }
        if ( port <= 0 || port > 65535 ) {
            throw new DockerUserException( "Invalid port number %d", port );
        }
        return port;
    }


    public CreateDockerRequest( String hostname, String alias, String registry, Integer communicationPort, Integer handshakePort, Integer proxyPort ) {
        this.hostname = DockerUtils.normalizeHostname( hostname );
        if ( this.hostname.isEmpty() ) {
            throw new DockerUserException( "Hostname must not be empty" );
        }
        this.alias = alias;
        if ( alias.isEmpty() ) {
            throw new DockerUserException( "Alias must not be empty" );
        }
        this.registry = registry;
        this.communicationPort = checkPortIsValid( communicationPort, ConfigDocker.COMMUNICATION_PORT );
        this.handshakePort = checkPortIsValid( handshakePort, ConfigDocker.HANDSHAKE_PORT );
        this.proxyPort = checkPortIsValid( proxyPort, ConfigDocker.PROXY_PORT );
        if ( this.communicationPort.equals( this.handshakePort ) || this.handshakePort.equals( this.proxyPort ) || this.communicationPort.equals( this.proxyPort ) ) {
            throw new DockerUserException( "Communication, handshake and proxy port must be different" );
        }
    }

}
