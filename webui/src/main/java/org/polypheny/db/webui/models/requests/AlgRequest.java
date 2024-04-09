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


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
import org.polypheny.db.webui.models.UIAlgNode;


@Jacksonized
@SuperBuilder
public class AlgRequest extends UIRequest {

    @JsonProperty
    public UIAlgNode topNode;
    @JsonProperty
    public boolean useCache;
    /**
     * TRUE if information about the query execution should be added to the Query Analyzer (InformationManager)
     */
    @JsonProperty
    public boolean analyze;
    @JsonProperty
    public boolean createView;
    @JsonProperty
    public String viewName;
    @JsonProperty
    public String store;
    @JsonProperty
    public String freshness;
    @JsonProperty
    public String interval;
    @JsonProperty
    public String timeUnit;

}
