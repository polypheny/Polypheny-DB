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
import java.util.Optional;
import java.util.UUID;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.variables.VariableStore;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.models.EdgeModel;
import org.polypheny.db.workflow.models.WorkflowConfigModel;
import org.polypheny.db.workflow.models.WorkflowModel;

/**
 * Represents an opened workflow that can be edited and executed.
 */
public interface Workflow {

    List<ActivityWrapper> getActivities(); // TODO: change return type to Map<UUID, Activity> ?

    ActivityWrapper getActivity( UUID activityId );

    /**
     * Get all edges of this workflow as list with arbitrary order.
     *
     * @return a list of all edges in this workflow.
     */
    List<Edge> getEdges();

    /**
     * Returns an unmodifiable list of all edges between from and to.
     *
     * @param from source activity id
     * @param to target activity id
     * @return A list of all edges between from and to. If there are no edges, an empty list is returned.
     */
    List<Edge> getEdges( UUID from, UUID to );

    List<Edge> getEdges( ActivityWrapper from, ActivityWrapper to );

    /**
     * Returns a list of all edges incident to the target.
     * The order of the returned edges is arbitrary.
     *
     * @param target target activity id
     * @return a list of all edges incident to the target
     */
    List<Edge> getInEdges( UUID target );

    List<Edge> getOutEdges( UUID source );

    Edge getEdge( EdgeModel model );

    Edge getEdge( ExecutionEdge execEdge );

    DataEdge getDataEdge( UUID to, int toPort );

    WorkflowConfigModel getConfig();

    WorkflowState getState();

    VariableStore getVariables();

    /**
     * Sets the state of this workflow to the specified value.
     * A workflow should not change its state by itself (after initialization).
     * This falls under the responsibility of the scheduler.
     * In general, any state related logic should be handled outside the workflow class.
     *
     * @param state the new state to be used
     */
    void setState( WorkflowState state );

    /**
     * Updates the inTypePreview, outTypePreview and settingsPreview of the specified activity,
     * if it is not already executed.
     * To get consistent results, updates should be called in topological order.
     * If the update resulted in an invalid state, the outTypePreview and settingsPreview is not updated.
     *
     * @param activityId the activity whose previews will be updated
     */
    void updatePreview( UUID activityId );

    /**
     * Updates the inTypePreview, outTypePreview and settingsPreview of the specified activity,
     * if it is not already executed.
     * To get consistent results, updates should be called in topological order.
     * If the update resulted in an invalid state, an exception is thrown.
     *
     * @param activityId the activity whose previews will be updated
     */
    void updateValidPreview( UUID activityId ) throws ActivityException;

    /**
     * Recomputes the variables of the specified activity based on the variables stored in its input activities
     * and the edge state.
     * The resulting variables might not yet be stable.
     *
     * @param activityId the activity whose variables will be recomputed
     */
    void recomputeInVariables( UUID activityId );

    /**
     * Returns true if all edges that could change the variables of the specified activity are
     * not IDLE (except for ignored edges).
     * If the specified activity is not yet executed, calling {@code recomputeInVariables()} more than once
     * does not change its variables.
     *
     * @param activityId the activity whose variables will be recomputed
     * @return true if the variables of the activities are stable.
     */
    boolean hasStableInVariables( UUID activityId );

    /**
     * Returns a list containing a preview of all input types for the specified activity.
     * Not yet available input types are empty Optionals.
     * As inactive data edges cannot transmit data, their type is set to null.
     *
     * @param activityId target activity
     * @return a list of all input types ordered by inPort index
     */
    List<Optional<AlgDataType>> getInputTypes( UUID activityId );

    int getInPortCount( UUID activityId );

    void addActivity( ActivityWrapper activity );

    void deleteActivity( UUID activityId );

    void deleteEdge( EdgeModel model );

    AttributedDirectedGraph<UUID, ExecutionEdge> toDag();

    void validateStructure( StorageManager sm ) throws Exception;

    void validateStructure( StorageManager sm, AttributedDirectedGraph<UUID, ExecutionEdge> subDag ) throws IllegalStateException;


    /**
     * Returns a WorkflowModel of this workflow.
     * It can be either a static representation (without states) or a dynamic representation (with states).
     * The static representation is used for persistently storing workflows.
     *
     * @param includeState whether to get the dynamic representation that includes states or not
     * @return WorkflowModel corresponding to this workflow
     */
    default WorkflowModel toModel( boolean includeState ) {
        WorkflowState state = includeState ? getState() : null;
        return new WorkflowModel( getActivities().stream().map( a -> a.toModel( includeState ) ).toList(),
                getEdges().stream().map( e -> e.toModel( includeState ) ).toList(),
                getConfig(), getVariables().getVariables(), state );
    }


    enum WorkflowState {
        IDLE,
        EXECUTING,
        INTERRUPTED
    }

}
