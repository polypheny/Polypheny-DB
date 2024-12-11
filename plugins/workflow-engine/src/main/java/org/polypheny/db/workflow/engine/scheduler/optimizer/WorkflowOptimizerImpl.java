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

package org.polypheny.db.workflow.engine.scheduler.optimizer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.util.graph.TopologicalOrderIterator;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorType;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge.ExecutionEdgeFactory;
import org.polypheny.db.workflow.engine.scheduler.GraphUtils;
import org.polypheny.db.workflow.models.ActivityConfigModel.CommonType;

public class WorkflowOptimizerImpl extends WorkflowOptimizer {


    public WorkflowOptimizerImpl( Workflow workflow, AttributedDirectedGraph<UUID, ExecutionEdge> execDag ) {
        super( workflow, execDag );
    }


    @Override
    public List<SubmissionFactory> computeNextTrees( CommonType commonType ) {
        AttributedDirectedGraph<UUID, ExecutionEdge> subDag = AttributedDirectedGraph.create( new ExecutionEdgeFactory() );
        Map<UUID, NodeColor> nodeColors = new HashMap<>();
        Map<ExecutionEdge, EdgeColor> edgeColors = new HashMap<>();
        initializeSubDag( getCommonSubExecDag( commonType ), subDag, nodeColors, edgeColors ); // ignore irrelevant nodes of execDag

        // order determines priority if an activity implements multiple interfaces
        determineVariableWriters( subDag, nodeColors, edgeColors );
        if ( isFusionEnabled ) {
            determineFusions( subDag, nodeColors, edgeColors );
        }
        if ( isPipelineEnabled ) {
            determinePipes( subDag, nodeColors, edgeColors );
        }

        System.out.print( "\nSub-DAG: " + subDag );
        System.out.print( ", Node Colors: " + nodeColors );
        System.out.println( ", Edge Colors: " + edgeColors );

        // TODO: ensure common transaction boundaries are respected

        return createFactories( subDag, getFirstConnectedComponents( subDag, nodeColors, edgeColors ) );
    }


    private void initializeSubDag( AttributedDirectedGraph<UUID, ExecutionEdge> baseDag, AttributedDirectedGraph<UUID, ExecutionEdge> subDag, Map<UUID, NodeColor> nodeColors, Map<ExecutionEdge, EdgeColor> edgeColors ) {
        for ( UUID n : baseDag.vertexSet() ) {
            ActivityState state = getState( n );
            // ignore finished or skipped activities
            if ( state == ActivityState.QUEUED || state == ActivityState.EXECUTING ) {
                nodeColors.put( n, state == ActivityState.EXECUTING ? NodeColor.EXECUTING : NodeColor.UNDEFINED );
                subDag.addVertex( n );
            }
        }

        for ( UUID source : subDag.vertexSet() ) {
            for ( ExecutionEdge edge : baseDag.getOutwardEdges( source ) ) {
                UUID target = edge.getTarget();
                if ( !subDag.vertexSet().contains( target ) ) {
                    continue; // edge to an activity that was already aborted
                }

                Edge edgeData = workflow.getEdge( edge );

                assert edgeData.getState() == EdgeState.IDLE : "Encountered edge of queued or executing activity that is not idle: " + edge;
                if ( edgeData.isIgnored() ) {
                    continue; // control edges that are no longer required are not added to the subDag
                }

                EdgeColor color = EdgeColor.UNDEFINED;
                if ( edge.isControl() ) {
                    color = EdgeColor.CONTROL;
                } else if ( requiresCheckpoint( source ) || getState( source ) == ActivityState.EXECUTING ) {
                    color = EdgeColor.CHECKPOINT;
                }
                edgeColors.put( edge, color );
                subDag.addEdge( source, target, edge );
            }
        }
    }


    private void determineVariableWriters( AttributedDirectedGraph<UUID, ExecutionEdge> subDag, Map<UUID, NodeColor> nodeColors, Map<ExecutionEdge, EdgeColor> edgeColors ) {
        subDag.vertexSet().stream().filter( this::requestsToWrite ).forEach( activityId -> {
            nodeColors.put( activityId, NodeColor.WRITER );
            subDag.getOutwardEdges( activityId ).stream().filter( e -> edgeColors.get( e ) == EdgeColor.UNDEFINED ).forEach( edge -> {
                edgeColors.put( edge, EdgeColor.CHECKPOINT );
            } );
        } );
    }


    private void determineFusions( AttributedDirectedGraph<UUID, ExecutionEdge> subDag, Map<UUID, NodeColor> nodeColors, Map<ExecutionEdge, EdgeColor> edgeColors ) {
        Set<UUID> fused = new HashSet<>();
        for ( UUID source : TopologicalOrderIterator.of( subDag ) ) {
            if ( nodeColors.get( source ) != NodeColor.UNDEFINED || !canFuse( source ) ) {
                continue; // ignore already colored activities
            }

            List<ExecutionEdge> edges = subDag.getOutwardEdges( source );
            if ( edges.size() != 1 ) {
                continue;
            }
            ExecutionEdge edge = edges.get( 0 );
            UUID target = edge.getTarget();
            if ( nodeColors.get( target ) != NodeColor.UNDEFINED || edgeColors.get( edge ) != EdgeColor.UNDEFINED ) {
                continue; // checkpoint or control edge, writer node
            }

            if ( canFuse( target ) ) {
                edgeColors.put( edge, EdgeColor.FUSED );
                fused.add( source ); // source or target might already be in set
                fused.add( target );
            }
        }

        fused.forEach( n -> nodeColors.put( n, NodeColor.FUSED ) ); // only apply color AFTER iteration
    }


    private void determinePipes( AttributedDirectedGraph<UUID, ExecutionEdge> subDag, Map<UUID, NodeColor> nodeColors, Map<ExecutionEdge, EdgeColor> edgeColors ) {
        Set<UUID> piped = new HashSet<>();
        for ( UUID source : TopologicalOrderIterator.of( subDag ) ) {
            if ( nodeColors.get( source ) != NodeColor.UNDEFINED || !canPipe( source ) ) {
                continue; // ignore already colored activities
            }

            List<ExecutionEdge> edges = subDag.getOutwardEdges( source );
            if ( edges.size() != 1 ) {
                continue;
            }
            ExecutionEdge edge = edges.get( 0 );
            UUID target = edge.getTarget();
            if ( nodeColors.get( target ) != NodeColor.UNDEFINED || edgeColors.get( edge ) != EdgeColor.UNDEFINED ) {
                continue; // checkpoint or control edge, writer or fused node
            }

            if ( canPipe( target ) ) {
                edgeColors.put( edge, EdgeColor.PIPED );
                piped.add( source ); // source or target might already be in set
                piped.add( target );
            }
        }

        piped.forEach( n -> nodeColors.put( n, NodeColor.PIPED ) ); // only apply color AFTER iteration
    }


    private List<Pair<Set<UUID>, NodeColor>> getFirstConnectedComponents( AttributedDirectedGraph<UUID, ExecutionEdge> subDag, Map<UUID, NodeColor> nodeColors, Map<ExecutionEdge, EdgeColor> edgeColors ) {

        // TODO: replace with contracted graph to make cleaner?
        List<Pair<Set<UUID>, NodeColor>> connected = new ArrayList<>();
        Set<UUID> visited = new HashSet<>();

        for ( UUID start : subDag.vertexSet().stream().filter( n -> subDag.getInwardEdges( n ).isEmpty() ).toList() ) {
            if ( visited.contains( start ) ) {
                continue; // already part of a (possibly ignored) component
            }
            visited.add( start );
            NodeColor color = nodeColors.get( start );
            if ( color.executorType == null ) {
                continue;
            }
            Set<UUID> component = new HashSet<>();
            component.add( start );
            if ( color.compatibleEdge == null ) {
                connected.add( Pair.of( component, color ) );
                continue;
            }

            // move through inverted tree forwards in graph up to root
            List<ExecutionEdge> outEdges = subDag.getOutwardEdges( start ).stream().filter( e -> edgeColors.get( e ) == color.compatibleEdge ).toList();
            assert outEdges.size() <= 1 : "Found connected component which is not an inverted tree";
            if ( outEdges.isEmpty() ) {
                connected.add( Pair.of( component, color ) );
                continue;
            }
            ExecutionEdge outEdge = outEdges.get( 0 );
            UUID target = null;
            while ( outEdge != null ) {
                target = outEdge.getTarget();
                assert nodeColors.get( target ) == color;

                outEdges = subDag.getOutwardEdges( target ).stream().filter( e -> edgeColors.get( e ) == color.compatibleEdge ).toList();
                assert outEdges.size() <= 1 : "Found connected component which is not an inverted tree";
                outEdge = outEdges.isEmpty() ? null : outEdges.get( 0 );
            }
            assert target != null;

            // explore backward (DFS) and check for dependencies
            boolean hasDependency = false;
            Queue<UUID> open = new LinkedList<>( List.of( target ) );
            while ( !open.isEmpty() ) {
                UUID n = open.remove();
                if ( component.contains( n ) ) {
                    continue;
                }
                assert nodeColors.get( n ) == color;
                component.add( n );
                visited.add( n );

                List<ExecutionEdge> inEdges = subDag.getInwardEdges( n );
                for ( ExecutionEdge inEdge : inEdges ) {
                    if ( color.compatibleEdge == edgeColors.get( inEdge ) ) {
                        open.add( inEdge.getSource() );
                    } else {
                        hasDependency = true;
                    }
                }
            }

            if ( !hasDependency ) {
                connected.add( Pair.of( component, color ) );
            }
        }
        return connected;
    }


    private List<SubmissionFactory> createFactories( AttributedDirectedGraph<UUID, ExecutionEdge> subDag, List<Pair<Set<UUID>, NodeColor>> components ) {
        PriorityQueue<Pair<Integer, SubmissionFactory>> queue
                = new PriorityQueue<>( Comparator.comparingInt( obj -> (int) ((Pair<?, ?>) obj).getLeft() ).reversed() );

        for ( Pair<Set<UUID>, NodeColor> component : components ) {
            SubmissionFactory factory = new SubmissionFactory(
                    GraphUtils.getInducedSubgraph( subDag, component.left ),
                    component.left,
                    component.right.executorType, // TODO: use fusion executor even for single activities if possible
                    CommonType.NONE );
            queue.add( Pair.of( factory.getActivities().size(), factory ) ); // larger trees have higher priority
        }

        List<SubmissionFactory> result = new ArrayList<>();
        while ( !queue.isEmpty() ) {
            result.add( queue.remove().right );
        }
        return result;
    }


    /**
     * We successively assign "colors" (or roles) to nodes and edges of the execDag
     */
    private enum NodeColor {
        UNDEFINED( ExecutorType.DEFAULT ),
        EXECUTING,
        FUSED( ExecutorType.FUSION, EdgeColor.FUSED ),
        PIPED( ExecutorType.PIPE, EdgeColor.PIPED ),
        WRITER( ExecutorType.VARIABLE_WRITER );

        public final ExecutorType executorType;
        public final EdgeColor compatibleEdge;


        NodeColor() {
            this( null, null );
        }


        NodeColor( ExecutorType executorType ) {
            this( executorType, null );
        }


        NodeColor( ExecutorType executorType, EdgeColor compatibleEdge ) {
            this.executorType = executorType;
            this.compatibleEdge = compatibleEdge;
        }
    }


    private enum EdgeColor {
        UNDEFINED,
        CONTROL,
        CHECKPOINT,
        FUSED,
        PIPED
    }

}
