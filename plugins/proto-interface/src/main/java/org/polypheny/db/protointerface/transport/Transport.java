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

<<<<<<<< HEAD:plugins/proto-interface/src/main/java/org/polypheny/db/protointerface/transport/Transport.java
package org.polypheny.db.protointerface.transport;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

public interface Transport extends Closeable {

    Optional<String> getPeer();

    void sendMessage( byte[] msg ) throws IOException;

    byte[] receiveMessage() throws IOException;

    void close();
========
package org.polypheny.db.docker.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AutoDockerStatus( @JsonProperty boolean available, @JsonProperty boolean connected, @JsonProperty boolean running, @JsonProperty String status ) {
>>>>>>>> master:core/src/main/java/org/polypheny/db/docker/models/AutoDockerStatus.java

}
