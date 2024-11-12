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

package org.polypheny.db.workflow.dag;

import java.util.List;
import java.util.Map;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.edges.Edge;

public class WorkflowImpl implements Workflow {
    private final List<Activity> activities;
    private final List<Edge> edges;
    private final Map<String, Object> config;


    public WorkflowImpl( List<Activity> activities, List<Edge> edges, Map<String, Object> config ) {
        this.activities = activities;
        this.edges = edges;
        this.config = config;
    }


    @Override
    public List<Activity> getActivities() {
        return activities;
    }


    @Override
    public List<Edge> getEdges() {
        return edges;
    }


    @Override
    public Map<String, Object> getConfig() {
        return config;
    }

}
