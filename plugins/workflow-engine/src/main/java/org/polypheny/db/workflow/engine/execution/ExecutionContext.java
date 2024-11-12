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

package org.polypheny.db.workflow.engine.execution;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.edges.Edge;


@Getter
public class ExecutionContext {

    @Setter
    private WorkflowState state = WorkflowState.IDLE;
    private final Map<UUID, ActivityState> activityStateMap = new ConcurrentHashMap<>();
    private final Map<Pair<UUID, UUID>, EdgeState> edgeStateMap = new ConcurrentHashMap<>();
    private final Map<UUID, Map<String, Object>> variableMap = new ConcurrentHashMap<>(); // stores for each activity its variable snapshot. TODO: change value type


    public ExecutionContext( List<Activity> activities, List<Edge> edges, Map<String, Object> globalVars ) {
        for ( Activity a : activities ) {
            activityStateMap.put( a.getId(), ActivityState.IDLE );
            variableMap.put( a.getId(), new ConcurrentHashMap<>( globalVars ) );
        }
        for ( Edge e : edges ) {
            edgeStateMap.put( e.toPair(), EdgeState.IDLE );
        }
    }


    public enum WorkflowState {
        IDLE,
        EXECUTING
    }


    public enum ActivityState {
        IDLE,
        QUEUED,
        EXECUTING,
        SKIPPED,  // => execution was aborted
        FAILED,
        FINISHED,
        SAVED  // => finished + checkpoint created
    }


    public enum EdgeState {
        IDLE,
        ACTIVE,
        INACTIVE
    }

}
