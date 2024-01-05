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
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.DockerUtils;

public record DockerHost(@JsonProperty String hostname, @JsonProperty String alias, @JsonProperty String registry,
                         @JsonProperty int communicationPort, @JsonProperty int handshakePort, @JsonProperty int proxyPort) {

    public String getRegistryOrDefault() {
        return registry.isEmpty() ?
                RuntimeConfig.DOCKER_CONTAINER_REGISTRY.getString()
                : registry;
    }


    private int checkPortIsValid( int port ) {
        if ( port <= 0 || port > 65535 ) {
            throw new GenericRuntimeException( "Invalid port number %d", port );
        }
        return port;
    }


    public DockerHost( String hostname, String alias, String registry, int communicationPort, int handshakePort, int proxyPort ) {
        if ( communicationPort == handshakePort || handshakePort == proxyPort || communicationPort == proxyPort ) {
            throw new GenericRuntimeException( "Communication, handshake and proxy port must be different" );
        }
        this.hostname = DockerUtils.normalizeHostname( hostname );
        if ( this.hostname.isEmpty() ) {
            throw new GenericRuntimeException( "Hostname must not be empty" );
        }
        this.alias = alias;
        if ( alias.isEmpty() ) {
            throw new GenericRuntimeException( "Alias must not be empty" );
        }
        this.registry = registry;
        this.communicationPort = checkPortIsValid( communicationPort );
        this.handshakePort = checkPortIsValid( handshakePort );
        this.proxyPort = checkPortIsValid( proxyPort );
    }

}
