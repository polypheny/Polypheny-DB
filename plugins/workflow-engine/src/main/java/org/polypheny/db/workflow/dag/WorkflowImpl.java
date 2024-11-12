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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.models.WorkflowModel;

public class WorkflowImpl implements Workflow {

    private final Map<UUID, Activity> activities;
    private final Map<Pair<UUID, UUID>, Edge> edges;
    private final Map<String, Object> config;


    public WorkflowImpl() {
        this.activities = new ConcurrentHashMap<>();
        this.edges = new ConcurrentHashMap<>();
        this.config = new ConcurrentHashMap<>();
    }


    private WorkflowImpl( Map<UUID, Activity> activities, Map<Pair<UUID, UUID>, Edge> edges, Map<String, Object> config ) {
        this.activities = activities;
        this.edges = edges;
        this.config = config;
    }


    @Override
    public List<Activity> getActivities() {
        return new ArrayList<>( activities.values() );
    }


    @Override
    public List<Edge> getEdges() {
        return new ArrayList<>( edges.values() );
    }


    @Override
    public Map<String, Object> getConfig() {
        return config;
    }


    public static Workflow fromModel( WorkflowModel model ) {

        Map<UUID, Activity> activities = new ConcurrentHashMap<>();
        Map<Pair<UUID, UUID>, Edge> edges = new ConcurrentHashMap<>();
        /* TODO: uncomment when Activity.fromModel and Edge.fromModel are implemented
        for ( ActivityModel a : model.getActivities() ) {
            activities.put( a.getId(), Activity.fromModel( a ) );
        }
        for ( EdgeModel e : model.getEdges()) {
            edges.put( Pair.of( e.getFromId(), e.getToId() ), Edge.fromModel( e, activities ) );
        }*/

        return new WorkflowImpl( activities, edges, model.getConfig() );
    }

}
