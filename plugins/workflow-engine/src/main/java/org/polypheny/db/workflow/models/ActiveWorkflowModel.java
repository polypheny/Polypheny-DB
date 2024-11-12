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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Value;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.engine.execution.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.ExecutionContext.ActivityState;
import org.polypheny.db.workflow.engine.execution.ExecutionContext.EdgeState;
import org.polypheny.db.workflow.engine.execution.ExecutionContext.WorkflowState;


/**
 * Contains the state of a workflow currently open in a session.
 * It therefore includes information on execution state. It is not possible to reconstruct a workflow
 * with identical execution state from an ActiveWorkflowModel.
 * Only the static form based on the WorkflowModel can be reconstructed.
 */
@Value
public class ActiveWorkflowModel {

    WorkflowModel workflow;

    WorkflowState state;
    Map<UUID, ActivityState> activityStateMap;
    Map<String, EdgeState> edgeStateMap = new HashMap<>();  // Concatenates key from Pair<UUID, UUID> to "UUID -> UUID"
    Map<UUID, Map<String, Object>> variableMap;


    public ActiveWorkflowModel( Workflow wf, ExecutionContext ec ) {
        this.workflow = wf.toModel();
        state = ec.getState();
        activityStateMap = Map.copyOf( ec.getActivityStateMap() );

        for ( Map.Entry<Pair<UUID, UUID>, EdgeState> entry : ec.getEdgeStateMap().entrySet() ) {
            String concatenatedKey = entry.getKey().getLeft() + " -> " + entry.getKey().getRight();
            edgeStateMap.put( concatenatedKey, entry.getValue() );
        }

        variableMap = ec.getVariableMap().entrySet()
                .stream().collect(
                        Collectors.toUnmodifiableMap(
                                Entry::getKey,
                                entry -> Collections.unmodifiableMap( entry.getValue() )  // shallow copy of inner map
                        )
                );
    }


}
