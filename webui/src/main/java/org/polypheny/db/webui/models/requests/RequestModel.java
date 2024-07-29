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
import lombok.experimental.SuperBuilder;

@SuperBuilder(toBuilder = true)
public class RequestModel {

    /**
     * ExpressionType of a request, e.g. QueryRequest or RelAlgRequest
     */
    public String type;

    public String payload;
    public String source;


    @JsonCreator
    public RequestModel( @JsonProperty("type") String type, @JsonProperty("payload") String payload, @JsonProperty("source") String source ) {
        this.type = type;
        this.payload = payload;
        this.source = source;
    }

}
