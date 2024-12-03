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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge.ExecutionEdgeFactory;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.EdgeModel;
import org.polypheny.db.workflow.models.WorkflowConfigModel;
import org.polypheny.db.workflow.models.WorkflowModel;

public class WorkflowImpl implements Workflow {

    private final Map<UUID, ActivityWrapper> activities;
    private final Map<Pair<UUID, UUID>, List<Edge>> edges;
    private final WorkflowConfigModel config;
    @Getter
    @Setter
    private WorkflowState state = WorkflowState.IDLE;


    public WorkflowImpl() {
        this.activities = new ConcurrentHashMap<>();
        this.edges = new ConcurrentHashMap<>();
        this.config = WorkflowConfigModel.of();
    }


    private WorkflowImpl( Map<UUID, ActivityWrapper> activities, Map<Pair<UUID, UUID>, List<Edge>> edges, WorkflowConfigModel config ) {
        this.activities = activities;
        this.edges = edges;
        this.config = config;
    }


    public static Workflow fromModel( WorkflowModel model ) {

        Map<UUID, ActivityWrapper> activities = new ConcurrentHashMap<>();
        Map<Pair<UUID, UUID>, List<Edge>> edges = new ConcurrentHashMap<>();

        for ( ActivityModel a : model.getActivities() ) {
            activities.put( a.getId(), ActivityWrapper.fromModel( a ) );
        }
        for ( EdgeModel e : model.getEdges() ) {
            Pair<UUID, UUID> key = Pair.of( e.getFromId(), e.getToId() );
            List<Edge> edgeList = edges.computeIfAbsent( key, k -> new ArrayList<>() );
            edgeList.add( Edge.fromModel( e, activities ) );
        }

        return new WorkflowImpl( activities, edges, model.getConfig() );
    }


    @Override
    public List<ActivityWrapper> getActivities() {
        return new ArrayList<>( activities.values() );
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
        return getEdges().stream().filter( e -> e.getTo().getId().equals( target ) ).toList();
    }


    @Override
    public List<Edge> getOutEdges( UUID source ) {
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
    public WorkflowConfigModel getConfig() {
        return config;
    }


    @Override
    public void addActivity( ActivityWrapper activity ) {
        if ( activities.containsKey( activity.getId() ) ) {
            throw new GenericRuntimeException( "Cannot add activity instance that is already part of this workflow." );
        }
        activities.put( activity.getId(), activity );
    }


    @Override
    public void deleteActivity( UUID activityId ) {
        edges.entrySet().removeIf( entry -> entry.getKey().left.equals( activityId ) || entry.getKey().right.equals( activityId ) );
        activities.remove( activityId );
    }


    @Override
    public void deleteEdge( EdgeModel model ) {
        List<Edge> edgeList = edges.get( model.toPair() );
        if ( edgeList == null ) {
            return;
        }
        edgeList.removeIf( e -> e.isEquivalent( model ) );
    }


    @Override
    public AttributedDirectedGraph<UUID, ExecutionEdge> toDag() {
        AttributedDirectedGraph<UUID, ExecutionEdge> dag = AttributedDirectedGraph.create( new ExecutionEdgeFactory() );

        activities.keySet().forEach( dag::addVertex );
        getEdges().forEach( edge -> dag.addEdge( edge.getFrom().getId(), edge.getTo().getId(), edge ) );

        return dag;
    }

}
