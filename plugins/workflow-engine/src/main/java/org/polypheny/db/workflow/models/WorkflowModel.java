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

package org.polypheny.db.workflow.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;

@Value
@AllArgsConstructor
public class WorkflowModel {

    public String format_version = "0.0.1";

    List<ActivityModel> activities;
    List<EdgeModel> edges;
    Map<String, Object> config;

    @JsonInclude(JsonInclude.Include.NON_NULL) // do not serialize EdgeState in static version
    WorkflowState state;


    public WorkflowModel() {
        activities = new ArrayList<>();
        edges = new ArrayList<>();
        config = new HashMap<>();
        state = null;
    }


    public static WorkflowModel getSample() {
        List<ActivityModel> activities = List.of(
                new ActivityModel( "extract", UUID.randomUUID(), Map.of(), Map.of(), Map.of(), null ),
                new ActivityModel( "transform", UUID.randomUUID(), Map.of(), Map.of(), Map.of(), null ),
                new ActivityModel( "load", UUID.randomUUID(), Map.of(), Map.of(), Map.of(), null )
        );
        List<EdgeModel> edges = List.of(
                new EdgeModel( activities.get( 0 ).getId(), activities.get( 1 ).getId(), 0, 0, false, null ),
                new EdgeModel( activities.get( 1 ).getId(), activities.get( 2 ).getId(), 0, 0, false, null )
        );
        Map<String, Object> config = Map.of( "relStore", "hsqldb", "docStore", "hsqldb", "graphStore", "hsqldb" );
        return new WorkflowModel( activities, edges, config, null );
    }

}
