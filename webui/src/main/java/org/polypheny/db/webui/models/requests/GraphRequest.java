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

package org.polypheny.db.webui.models.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;


@EqualsAndHashCode(callSuper = true)
@Getter
@Value
@Jacksonized
@SuperBuilder
public class GraphRequest extends QueryRequest {

    public List<String> nodeIds;

    public List<String> edgeIds;


    @JsonCreator
    public GraphRequest(
            @JsonProperty("nodeIds") List<String> nodeIds,
            @JsonProperty("edgeIds") List<String> edgeIds,
            @JsonProperty("query") String query,
            @JsonProperty("analyze") boolean analyze,
            @JsonProperty("cache") boolean cache,
            @JsonProperty("language") String language,
            @JsonProperty("namespaceId") String namespace ) {
        super( query, analyze, cache, language, namespace );
        this.nodeIds = nodeIds;
        this.edgeIds = edgeIds;
    }

}
