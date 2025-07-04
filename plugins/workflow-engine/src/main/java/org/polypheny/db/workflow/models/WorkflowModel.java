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

package org.polypheny.db.workflow.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;

@Value
@AllArgsConstructor
public class WorkflowModel {

    public String format_version = "0.0.1";

    List<ActivityModel> activities;
    List<EdgeModel> edges;
    WorkflowConfigModel config;
    Map<String, JsonNode> variables;

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    String description; // only used for specifying the description in sample workflows. Never gets serialized.

    @JsonInclude(JsonInclude.Include.NON_NULL) // do not serialize EdgeState in static version
    WorkflowState state;


    public WorkflowModel() {
        activities = new ArrayList<>();
        edges = new ArrayList<>();
        config = WorkflowConfigModel.of();
        state = null;
        variables = Map.of();
        description = null;
    }


    public WorkflowModel( List<ActivityModel> activities, List<EdgeModel> edges, WorkflowConfigModel config, Map<String, JsonNode> variables ) {
        this.activities = activities;
        this.edges = edges;
        this.config = config;
        this.variables = variables;
        this.state = null;
        this.description = null;
    }


    public void validate() throws Exception {
        this.config.validate();
    }

}
