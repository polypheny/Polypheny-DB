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


import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Jacksonized
@SuperBuilder
public class QueryRequest extends UIRequest {


    @JsonCreator
    public QueryRequest(
            @JsonProperty("query") String query,
            @JsonProperty("analyze") boolean analyze,
            @JsonProperty("cache") boolean cache,
            @JsonProperty("language") String language,
            @JsonAlias("database") @JsonProperty("namespace") String namespace ) {
        super();
        this.query = query;
        this.analyze = analyze;
        this.cache = cache;
        this.language = language;
        this.namespace = namespace;
    }


    /**
     * A query from the SQL console
     */
    @JsonProperty
    public String query;

    /**
     * TRUE if information about the query execution should be added to the Query Analyzer (InformationManager)
     */
    @JsonProperty
    public boolean analyze;

    /**
     * TRUE if the query should use the cache
     */
    @JsonProperty
    public boolean cache;

    /**
     * This flag defines which language was used for this query
     */
    @JsonProperty
    public String language;


}
