/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.schemaDiscovery;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

public class DocumentObjectNode extends Node implements AbstractNode {

    @Getter
    @Setter
    @JsonProperty
    private String jsonPath;
    @Getter
    @Setter
    @JsonProperty
    private boolean cardCandidate;


    public DocumentObjectNode( String name, String jsonPath, boolean cardCandidate ) {
        super( "object", name );
        this.jsonPath = jsonPath;
        this.cardCandidate = cardCandidate;
    }

}
