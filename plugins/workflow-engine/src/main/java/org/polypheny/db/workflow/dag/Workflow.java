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

package org.polypheny.db.workflow.dag;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.EdgeModel;
import org.polypheny.db.workflow.models.RenderModel;
import org.polypheny.db.workflow.models.WorkflowConfigModel;
import org.polypheny.db.workflow.models.WorkflowModel;

/**
 * Represents an opened workflow that can be edited and executed.
 */
public interface Workflow {

    List<ActivityWrapper> getActivities();

    int getActivityCount();

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

    void setConfig( WorkflowConfigModel config );

    WorkflowState getState();

    Map<String, JsonNode> getVariables();

    void updateVariables( Map<String, JsonNode> variables );

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
     * Not yet available input types are {@link org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType}.
     * As inactive data edges cannot transmit data, their type is {@link org.polypheny.db.workflow.dag.activities.TypePreview.InactiveType}.
     * Input ports without a connected edge have a {@link org.polypheny.db.workflow.dag.activities.TypePreview.MissingType}.
     *
     * @param activityId target activity
     * @return a list of all input types ordered by inPort index
     */
    List<TypePreview> getInputTypes( UUID activityId );

    int getInPortCount( UUID activityId );


    Set<UUID> getReachableActivities( UUID rootId, boolean includeRoot );

    /**
     * Resets the activity and all activities reachable from it.
     *
     * @param activityId target activity, or null if all activities should be reset
     * @param sm the StorageManager to be used to delete any existing checkpoints for activities being reset
     */
    void reset( UUID activityId, StorageManager sm );

    void reset( StorageManager sm );

    void resetFailedExecutionInit( StorageManager sm );

    ActivityWrapper addActivity( String activityType, RenderModel renderModel );

    ActivityWrapper cloneActivity( UUID activityId, double posX, double posY );

    void deleteActivity( UUID activityId, StorageManager sm );

    void addEdge( EdgeModel model, StorageManager sm );

    void deleteEdge( EdgeModel model, StorageManager sm );

    /**
     * Reorder the multi-edges by moving the specified edge to the targetIndex
     *
     * @param edge the edge to move
     * @param targetIndex the new index in the list of multi-edges for that activity. -1 to move to the end
     * @param sm the storage manager
     */
    void moveMultiEdge( EdgeModel edge, int targetIndex, StorageManager sm );

    ActivityWrapper updateActivity( UUID activityId, @Nullable Map<String, JsonNode> settings, @Nullable ActivityConfigModel config, @Nullable RenderModel rendering, StorageManager sm );

    /**
     * Returns the number of seconds until the execution of the subtree containing the given activities times out.
     * If there is more than one activity in the set, the sum of all timeout durations is returned.
     *
     * @param activities the activities of the subtree to be executed
     * @return the timout duration in seconds, or 0 if no timeout is desired.
     */
    int getTimeoutSeconds( Set<UUID> activities );

    AttributedDirectedGraph<UUID, ExecutionEdge> toDag();

    void validateStructure( StorageManager sm ) throws Exception;

    void validateStructure( StorageManager sm, AttributedDirectedGraph<UUID, ExecutionEdge> subDag ) throws IllegalStateException;

    default ActivityWrapper getActivityOrThrow( UUID activityId ) {
        return Objects.requireNonNull( getActivity( activityId ), "Activity does not exist: " + activityId );
    }


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
                getConfig(), getVariables(), null, state );
    }


    enum WorkflowState {
        IDLE,
        EXECUTING,
        INTERRUPTED
    }

}
