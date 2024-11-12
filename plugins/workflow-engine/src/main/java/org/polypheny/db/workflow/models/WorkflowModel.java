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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class WorkflowModel {

    public String format_version = "0.0.1";

    List<ActivityModel> activities;
    List<EdgeModel> edges;
    Map<String, Object> config;


    public WorkflowModel() {
        activities = new ArrayList<>();
        edges = new ArrayList<>();
        config = new HashMap<>();
    }


    public static WorkflowModel getSample() {
        List<ActivityModel> activities = List.of(
                new ActivityModel( "extract", UUID.randomUUID(), Map.of(), Map.of(), Map.of() ),
                new ActivityModel( "transform", UUID.randomUUID(), Map.of(), Map.of(), Map.of() ),
                new ActivityModel( "load", UUID.randomUUID(), Map.of(), Map.of(), Map.of() )
        );
        List<EdgeModel> edges = List.of(
                new EdgeModel( activities.get( 0 ).getId(), activities.get( 1 ).getId(), 0, 0, false ),
                new EdgeModel( activities.get( 1 ).getId(), activities.get( 2 ).getId(), 0, 0, false )
        );
        Map<String, Object> config = Map.of("relStore", "hsqldb", "docStore", "hsqldb", "graphStore", "hsqldb");
        return new WorkflowModel(activities, edges, config);
    }

}
