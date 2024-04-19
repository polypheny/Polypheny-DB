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

<<<<<<<< HEAD:plugins/proto-interface/src/main/java/org/polypheny/db/protointerface/statements/Signaturizable.java
package org.polypheny.db.protointerface.statements;

import java.util.List;
import org.polypheny.db.protointerface.proto.ParameterMeta;

public interface Signaturizable {

    List<ParameterMeta> getParameterMetas();
========
package org.polypheny.db.docker.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record AutoDockerResult( @JsonProperty AutoDockerStatus status, @JsonProperty List<DockerInstanceInfo> instances ) {
>>>>>>>> master:core/src/main/java/org/polypheny/db/docker/models/AutoDockerResult.java

}
