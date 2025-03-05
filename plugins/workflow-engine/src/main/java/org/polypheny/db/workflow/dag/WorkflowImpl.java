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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.util.graph.CycleDetector;
import org.polypheny.db.util.graph.TopologicalOrderIterator;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.InactiveType;
import org.polypheny.db.workflow.dag.activities.TypePreview.MissingType;
import org.polypheny.db.workflow.dag.activities.impl.special.NestedInputActivity;
import org.polypheny.db.workflow.dag.activities.impl.special.NestedOutputActivity;
import org.polypheny.db.workflow.dag.activities.impl.special.NestedWorkflowActivity;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge.ExecutionEdgeFactory;
import org.polypheny.db.workflow.engine.scheduler.GraphUtils;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.ActivityConfigModel.CommonType;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.EdgeModel;
import org.polypheny.db.workflow.models.RenderModel;
import org.polypheny.db.workflow.models.WorkflowConfigModel;
import org.polypheny.db.workflow.models.WorkflowModel;

public class WorkflowImpl implements Workflow {

    private final Map<UUID, ActivityWrapper> activities;
    private final Map<Pair<UUID, UUID>, List<Edge>> edges;
    @Getter
    private WorkflowConfigModel config;
    @Getter
    @Setter
    private WorkflowState state = WorkflowState.IDLE;
    @Getter
    private Map<String, JsonNode> variables; // workflow variables


    public WorkflowImpl() {
        this( new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), WorkflowConfigModel.of(), Map.of() );
    }


    private WorkflowImpl( Map<UUID, ActivityWrapper> activities, Map<Pair<UUID, UUID>, List<Edge>> edges, WorkflowConfigModel config, Map<String, JsonNode> variables ) {
        this.activities = activities;
        this.edges = edges;
        this.config = config;
        this.variables = variables;

        TopologicalOrderIterator.of( toDag() ).forEach( this::updatePreview );
    }


    public static Workflow fromModel( WorkflowModel model ) throws Exception {
        model.validate();

        Map<UUID, ActivityWrapper> activities = new ConcurrentHashMap<>();
        Map<Pair<UUID, UUID>, List<Edge>> edges = new ConcurrentHashMap<>();

        for ( ActivityModel a : model.getActivities() ) {
            activities.put( a.getId(), ActivityWrapper.fromModel( a ) );
        }
        for ( EdgeModel e : model.getEdges() ) {
            Pair<UUID, UUID> key = e.toPair();
            List<Edge> edgeList = edges.computeIfAbsent( key, k -> new ArrayList<>() );
            edgeList.add( Edge.fromModel( e, activities ) );
        }

        return new WorkflowImpl( activities, edges, model.getConfig(), model.getVariables() );
    }


    @Override
    public List<ActivityWrapper> getActivities() {
        return new ArrayList<>( activities.values() );
    }


    @Override
    public int getActivityCount() {
        return activities.size();
    }


    @Override
    public ActivityWrapper getActivity( UUID activityId ) {
        return activities.get( activityId );
    }


    @Override
    public List<Edge> getEdges() {
        return edges.values()
                .stream()
                .flatMap( List::stream )
                .toList();
    }


    @Override
    public List<Edge> getEdges( UUID from, UUID to ) {
        return Collections.unmodifiableList( edges.getOrDefault( Pair.of( from, to ), new ArrayList<>() ) );
    }


    @Override
    public List<Edge> getEdges( ActivityWrapper from, ActivityWrapper to ) {
        return getEdges( from.getId(), to.getId() );
    }


    @Override
    public List<Edge> getInEdges( UUID target ) {
        // TODO: make more efficient
        return getEdges().stream().filter( e -> e.getTo().getId().equals( target ) ).toList();
    }


    @Override
    public List<Edge> getOutEdges( UUID source ) {
        // TODO: make more efficient
        return getEdges().stream().filter( e -> e.getFrom().getId().equals( source ) ).toList();
    }


    @Override
    public Edge getEdge( EdgeModel model ) {
        List<Edge> candidates = getEdges( model.getFromId(), model.getToId() );
        for ( Edge e : candidates ) {
            if ( e.isEquivalent( model ) ) {
                return e;
            }
        }
        return null;
    }


    @Override
    public Edge getEdge( ExecutionEdge execEdge ) {
        List<Edge> candidates = edges.get( Pair.of( execEdge.getSource(), execEdge.getTarget() ) );
        if ( candidates != null ) {
            for ( Edge candidate : candidates ) {
                if ( execEdge.representsEdge( candidate ) ) {
                    return candidate;
                }
            }
        }
        return null;
    }


    @Override
    public DataEdge getDataEdge( UUID to, int toPort ) {
        for ( Edge edge : getInEdges( to ) ) {
            if ( edge instanceof DataEdge dataEdge ) {
                if ( dataEdge.getToPort() == toPort ) {
                    return dataEdge;
                }
            }
        }
        return null;
    }


    @Override
    public void setConfig( WorkflowConfigModel config ) {
        config.validate();
        this.config = config;
    }


    @Override
    public void updateVariables( Map<String, JsonNode> variables ) {
        this.variables = Map.copyOf( variables );
        for ( ActivityWrapper wrapper : activities.values() ) {
            wrapper.getVariables().updateWorkflowVariables( this.variables );
        }
    }


    @Override
    public void updatePreview( UUID activityId ) {
        try {
            updatePreview( activityId, false );
        } catch ( ActivityException ignored ) {
            assert false;
        }
    }


    @Override
    public void updateValidPreview( UUID activityId ) throws ActivityException {
        updatePreview( activityId, true );
    }


    private void updatePreview( UUID activityId, boolean throwIfInvalid ) throws ActivityException {
        ActivityWrapper wrapper = getActivity( activityId );
        if ( wrapper.getState().isExecuted() || wrapper.getState() == ActivityState.SKIPPED ) {
            return; // previews are only important for activities that can still get executed
        }
        recomputeInVariables( activityId );
        List<TypePreview> inTypes = getInputTypes( activityId );
        wrapper.setInTypePreview( inTypes );
        try {
            wrapper.updateOutTypePreview( inTypes, hasStableInVariables( activityId ) );
        } catch ( ActivityException e ) {
            if ( throwIfInvalid ) {
                throw e;
            }
        }
    }


    @Override
    public void recomputeInVariables( UUID activityId ) {
        ActivityWrapper wrapper = activities.get( activityId );
        List<Edge> inEdges = getInEdges( activityId );
        wrapper.getVariables().mergeInputStores( inEdges, wrapper.getDef().getDynamicInPortCount( inEdges ), variables );
    }


    @Override
    public boolean hasStableInVariables( UUID activityId ) {
        for ( Edge edge : getInEdges( activityId ) ) {
            if ( edge.getState() == EdgeState.IDLE && !edge.isIgnored() ) {
                return false;
            }
        }
        return true;
    }


    @Override
    public List<TypePreview> getInputTypes( UUID activityId ) {
        List<TypePreview> inputTypes = new ArrayList<>();

        for ( int i = 0; i < getInPortCount( activityId ); i++ ) {
            DataEdge dataEdge = getDataEdge( activityId, i );
            inputTypes.add( getTypePreview( dataEdge ) );
            if ( dataEdge != null && dataEdge.isMulti() ) {
                // the first of possibly several multi-edges
                int multiIdx = i + 1;
                DataEdge multiEdge = getDataEdge( activityId, multiIdx );
                while ( multiEdge != null ) {
                    inputTypes.add( getTypePreview( multiEdge ) );
                    multiEdge = getDataEdge( activityId, ++multiIdx );
                }
            }
        }
        return Collections.unmodifiableList( inputTypes );
    }


    private TypePreview getTypePreview( DataEdge dataEdge ) {
        if ( dataEdge == null ) {
            return MissingType.of(); // not yet connected
        } else if ( dataEdge.getState() == EdgeState.INACTIVE ) {
            return InactiveType.of();
        }
        return dataEdge.getFrom().getOutTypePreview().get( dataEdge.getFromPort() );
    }


    @Override
    public int getInPortCount( UUID activityId ) {
        return activities.get( activityId ).getDef().getInPorts().length;
    }


    @Override
    public Set<UUID> getReachableActivities( UUID rootId, boolean includeRoot ) {
        Set<UUID> visited = new HashSet<>();
        Queue<UUID> open = new LinkedList<>( List.of( rootId ) );
        while ( !open.isEmpty() ) {
            UUID n = open.remove();
            if ( visited.contains( n ) ) {
                continue;
            }
            visited.add( n );
            getOutEdges( n ).forEach( e -> open.add( e.getTo().getId() ) );
        }

        if ( !includeRoot ) {
            visited.remove( rootId );
        }
        return visited;
    }


    private void resetAll( Collection<UUID> activities, StorageManager sm ) {
        AttributedDirectedGraph<UUID, ExecutionEdge> subDag = GraphUtils.getInducedSubgraph( toDag(), activities );
        for ( UUID n : TopologicalOrderIterator.of( subDag ) ) {
            ActivityWrapper wrapper = this.activities.get( n );
            wrapper.resetExecution( variables );
            sm.dropCheckpoints( n );
            updatePreview( n );
            for ( ExecutionEdge e : subDag.getOutwardEdges( n ) ) {
                getEdge( e ).resetExecution();
            }
        }
    }


    @Override
    public void reset( UUID activityId, StorageManager sm ) {
        if ( activityId == null ) {
            reset( sm );
            return;
        }
        resetAll( getReachableActivities( activityId, true ), sm );
    }


    @Override
    public void reset( StorageManager sm ) {
        resetAll( getActivities().stream().map( ActivityWrapper::getId ).toList(), sm );
    }


    @Override
    public void resetFailedExecutionInit( StorageManager sm ) {
        resetAll( getActivities().stream()
                .filter( a -> a.getState() == ActivityState.QUEUED )
                .map( ActivityWrapper::getId ).toList(), sm
        );
        setState( WorkflowState.IDLE );
    }


    @Override
    public ActivityWrapper addActivity( String activityType, RenderModel renderModel ) {
        ActivityWrapper wrapper = ActivityWrapper.fromModel( new ActivityModel( activityType, renderModel ) );
        addActivity( wrapper );
        return wrapper;
    }


    private void addActivity( ActivityWrapper activity ) {
        if ( activities.containsKey( activity.getId() ) ) {
            throw new GenericRuntimeException( "Cannot add activity instance that is already part of this workflow." );
        }
        activities.put( activity.getId(), activity );
        updatePreview( activity.getId() ); // creates empty previews
    }


    @Override
    public ActivityWrapper cloneActivity( UUID activityId, double posX, double posY ) {
        ActivityModel clonedModel = getActivityOrThrow( activityId ).toModel( false ).createCopy( posX, posY );
        ActivityWrapper wrapper = ActivityWrapper.fromModel( clonedModel );
        addActivity( wrapper );
        return wrapper;
    }


    @Override
    public void deleteActivity( UUID activityId, StorageManager sm ) {
        Set<UUID> reachable = getReachableActivities( activityId, false );
        Set<Edge> removedEdges = new HashSet<>();
        edges.entrySet().removeIf( entry -> {
            if ( entry.getKey().left.equals( activityId ) || entry.getKey().right.equals( activityId ) ) {
                removedEdges.addAll( entry.getValue() );
                return true;
            }
            return false;
        } );
        removedEdges.forEach( this::updateMultiEdges );
        activities.remove( activityId );
        sm.dropCheckpoints( activityId );
        resetAll( reachable, sm );
    }


    @Override
    public void addEdge( EdgeModel model, StorageManager sm ) {
        if ( getEdge( model ) != null ) {
            throw new GenericRuntimeException( "Cannot add an edge that is already part of this workflow." );
        }
        if ( model.getFromId().equals( model.getToId() ) ) {
            throw new GenericRuntimeException( "Cannot add an edge with same source and target activity." );
        }
        // We allow the workflow to temporarily have more than 1 in-edge per data input, to allow the UI to swap the source activity.
        // The occupation validation is performed before execution.

        Edge edge = Edge.fromModel( model, activities );
        if ( edge instanceof DataEdge dataEdge && dataEdge.isMulti() ) {
            for ( Edge e : getInEdges( model.getToId() ) ) {
                if ( e instanceof DataEdge de && de.isMulti() && de.getFrom().getId().equals( model.getFromId() )
                        && de.getFromPort() == model.getFromPort() ) {
                    throw new GenericRuntimeException( "Cannot add the same edge more than once to a multi inPort." );
                }
            }
            if ( !dataEdge.isFirstMulti() ) {
                DataEdge previous = getDataEdge( model.getToId(), model.getToPort() - 1 );
                if ( previous == null ) {
                    throw new GenericRuntimeException( "Cannot add an edge to a multi inPort which is out of order." );
                }
            }
        }
        edges.computeIfAbsent( model.toPair(), k -> new ArrayList<>() ).add( edge );

        if ( !(new CycleDetector<>( toDag() ).findCycles().isEmpty()) ) {
            edges.get( model.toPair() ).remove( edge );
            throw new GenericRuntimeException( "Cannot add an edge that would result in a cycle." );
        }

        reset( edge.getTo().getId(), sm );
    }


    @Override
    public void deleteEdge( EdgeModel model, StorageManager sm ) {
        Edge edge = getEdge( model );
        List<Edge> edgeList = edges.get( model.toPair() );
        if ( edgeList == null ) {
            return;
        }
        edgeList.remove( edge );
        updateMultiEdges( edge );
        reset( edge.getTo().getId(), sm );
    }


    private void updateMultiEdges( Edge deletedEdge ) {
        if ( deletedEdge instanceof DataEdge dataEdge && dataEdge.isMulti() ) {
            int nextIdx = dataEdge.getToPort() + 1;
            UUID toId = dataEdge.getTo().getId();
            DataEdge nextEdge = getDataEdge( toId, nextIdx );
            while ( nextEdge != null ) {
                edges.get( nextEdge.toPair() ).remove( nextEdge );
                DataEdge movedEdge = DataEdge.of( nextEdge, nextIdx - 1 );
                edges.get( movedEdge.toPair() ).add( movedEdge );
                nextEdge = getDataEdge( toId, ++nextIdx );
            }
        }
    }


    @Override
    public ActivityWrapper updateActivity( UUID activityId, @Nullable Map<String, JsonNode> settings, @Nullable ActivityConfigModel config, @Nullable RenderModel rendering, StorageManager sm ) {
        ActivityWrapper wrapper = getActivityOrThrow( activityId );
        if ( rendering != null ) {
            wrapper.setRendering( rendering );
        }

        boolean requiresReset = false;
        if ( config != null ) {
            requiresReset = !wrapper.getConfig().equals( config );
            wrapper.setConfig( config );
        }
        if ( settings != null ) {
            requiresReset = requiresReset || !wrapper.getSerializableSettings().equals( settings );
            if ( settings.isEmpty() ) {
                wrapper.resetSettings();
            } else {
                wrapper.updateSettings( settings );
            }
        }

        if ( requiresReset ) {
            reset( activityId, sm );
        }
        return wrapper;
    }


    @Override
    public int getTimeoutSeconds( Set<UUID> activities ) {
        int baseTimeout = Math.max( 0, config.getTimeoutSeconds() );
        int timeout = 0;
        for ( UUID activityId : activities ) {
            int activityTimeout = Math.max( 0, getActivity( activityId ).getConfig().getTimeoutSeconds() );
            if ( activityTimeout > 0 ) { // override base timeout
                timeout += activityTimeout;
            } else {
                timeout += baseTimeout; // activity cannot disable base timeout
            }
        }
        return timeout;
    }


    @Override
    public AttributedDirectedGraph<UUID, ExecutionEdge> toDag() {
        AttributedDirectedGraph<UUID, ExecutionEdge> dag = AttributedDirectedGraph.create( new ExecutionEdgeFactory() );

        activities.keySet().forEach( dag::addVertex );
        getEdges().forEach( edge -> dag.addEdge( edge.getFrom().getId(), edge.getTo().getId(), edge ) );

        return dag;
    }


    @Override
    public void validateStructure( StorageManager sm ) throws Exception {
        validateStructure( sm, toDag() );
    }


    @Override
    public void validateStructure( StorageManager sm, AttributedDirectedGraph<UUID, ExecutionEdge> subDag ) throws IllegalStateException {
        if ( subDag.vertexSet().isEmpty() && subDag.edgeSet().isEmpty() ) {
            return;
        }

        for ( ExecutionEdge execEdge : subDag.edgeSet() ) {
            if ( execEdge.getSource().equals( execEdge.getTarget() ) ) {
                throw new IllegalStateException( "Source activity must differ from target activity for edge: " + execEdge );
            }
            if ( !activities.containsKey( execEdge.getSource() ) || !activities.containsKey( execEdge.getTarget() ) ) {
                throw new IllegalStateException( "Source and target activities of an edge must be part of the workflow: " + execEdge );
            }
            Edge edge = getEdge( execEdge );
            if ( edge instanceof DataEdge data && !data.isCompatible() ) {
                throw new IllegalStateException( "Incompatible port types for data edge: " + edge );
            }
        }

        if ( !(new CycleDetector<>( subDag ).findCycles().isEmpty()) ) {
            throw new IllegalStateException( "A workflow must not contain cycles" );
        }

        Set<Integer> nestedInputs = new HashSet<>();
        boolean foundNestedOutput = false;
        for ( UUID n : TopologicalOrderIterator.of( subDag ) ) {
            ActivityWrapper wrapper = getActivity( n );
            CommonType type = wrapper.getConfig().getCommonType();

            if ( wrapper.getState() == ActivityState.SAVED ) {
                if ( !sm.hasAllCheckpoints( n, wrapper.getDef().getOutPorts().length ) ) {
                    throw new IllegalStateException( "Found missing checkpoint for saved activity: " + wrapper );
                }
            } else if ( wrapper.getState() != ActivityState.FINISHED ) {
                for ( int i = 0; i < wrapper.getDef().getOutPorts().length; i++ ) {
                    if ( sm.hasCheckpoint( n, i ) && !sm.isLinkedCheckpoint( n, i ) ) { // NestedInputs get their checkpoints set in advance but are still executed
                        throw new IllegalStateException( "Found a checkpoint for an activity that has not yet been executed successfully: " + wrapper );
                    }
                }
            }
            if ( type != CommonType.NONE && wrapper.getActivity() instanceof NestedWorkflowActivity ) {
                throw new IllegalStateException( "Nested workflow activities cannot be part of a common transaction: " + wrapper );
            }
            if ( wrapper.getActivity() instanceof NestedInputActivity ) {
                if ( !wrapper.getSettingsPreview().keysPresent( NestedInputActivity.INDEX_KEY ) ) {
                    throw new IllegalStateException( "Index for nested input must be statically defined: " + wrapper );
                }
                int i = wrapper.getSettingsPreview().getInt( NestedInputActivity.INDEX_KEY );
                if ( nestedInputs.contains( i ) ) {
                    throw new IllegalStateException( "Found duplicate nested input for index " + i + ": " + wrapper );
                }
                nestedInputs.add( i );
            } else if ( wrapper.getActivity() instanceof NestedOutputActivity ) {
                if ( foundNestedOutput ) {
                    throw new IllegalStateException( "Found more than one nested output: " + wrapper );
                }
                foundNestedOutput = true;
            }

            Set<Integer> requiredInPorts = wrapper.getDef().getRequiredInPorts();
            Set<Integer> occupiedInPorts = new HashSet<>();
            Set<Integer> multiInPortIndices = new HashSet<>();
            for ( ExecutionEdge execEdge : subDag.getInwardEdges( n ) ) {
                ActivityWrapper source = getActivity( execEdge.getSource() );
                CommonType sourceType = source.getConfig().getCommonType();

                if ( !execEdge.isControl() ) {
                    int toPort = execEdge.getToPort();
                    if ( wrapper.getDef().getInPort( toPort ).isMulti() ) {
                        multiInPortIndices.add( toPort ); // throws null-pointer if port too high and not multi-port
                    }

                    requiredInPorts.remove( toPort );

                    if ( occupiedInPorts.contains( toPort ) ) {
                        throw new IllegalStateException( "InPort " + toPort + " is already occupied: " + execEdge );
                    }
                    occupiedInPorts.add( toPort );
                }

                if ( wrapper.getState().isExecuted() && !source.getState().isExecuted() && !getEdge( execEdge ).isIgnored() ) {
                    throw new IllegalStateException( "An activity that is executed cannot have a not yet executed predecessor: " + execEdge );
                }
                if ( type == CommonType.EXTRACT ) {
                    if ( sourceType != CommonType.EXTRACT ) {
                        throw new IllegalStateException( "An activity with CommonType EXTRACT must only have EXTRACT predecessors: " + execEdge );
                    }
                    if ( execEdge.isControl() && !execEdge.isOnSuccess() ) {
                        throw new IllegalStateException( "Cannot have a onFail control edge between common EXTRACT activities" + execEdge );
                    }
                } else if ( sourceType == CommonType.LOAD ) {
                    if ( type != CommonType.LOAD ) {
                        throw new IllegalStateException( "An activity with CommonType LOAD must only have LOAD successors: " + execEdge );
                    }
                    if ( execEdge.isControl() && !execEdge.isOnSuccess() ) {
                        throw new IllegalStateException( "Cannot have a onFail control edge between common LOAD activities" + execEdge );
                    }
                }

            }

            if ( wrapper.getState() != ActivityState.SAVED ) { // already saved activities do not need their predecessors in the subDag
                if ( !requiredInPorts.isEmpty() ) {
                    throw new IllegalStateException( "Activity is missing the required data input(s) " + requiredInPorts + ": " + wrapper );
                }
                if ( !multiInPortIndices.isEmpty() ) {
                    int multiIdx = wrapper.getDef().getInPorts().length - 1;
                    for ( int i = multiIdx; i < multiIdx + multiInPortIndices.size(); i++ ) {
                        if ( !multiInPortIndices.contains( i ) ) {
                            throw new IllegalStateException( "Multi-InPort indices must be in consecutive order, but " + i + "is missing for " + wrapper );
                        }
                    }
                }

            }
        }
        if ( !nestedInputs.isEmpty() && Collections.max( nestedInputs ) != nestedInputs.size() - 1 ) {
            throw new IllegalStateException( "A nested input has skipped an input index, which is not permitted." );
        }
    }


    @Override
    public String toString() {
        return "WorkflowImpl{" +
                "\n    activities=" + getActivities() +
                ", \n    edges=" + getEdges() +
                ", \n    config=" + config +
                ", \n    state=" + state +
                ", \n    variables=" + variables +
                "\n}";
    }

}
