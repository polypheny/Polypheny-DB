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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.util.graph.TopologicalOrderIterator;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge.ExecutionEdgeFactory;

public class WorkflowOptimizerImpl extends WorkflowOptimizer {


    public WorkflowOptimizerImpl( Workflow workflow, AttributedDirectedGraph<UUID, ExecutionEdge> execDag ) {
        super( workflow, execDag );
    }


    @Override
    public List<SubmissionFactory> computeNextTrees() {
        AttributedDirectedGraph<UUID, ExecutionEdge> subDag = AttributedDirectedGraph.create( new ExecutionEdgeFactory() ); // ignore irrelevant nodes of execDag
        Map<UUID, NodeColor> nodeColors = new HashMap<>();
        Map<ExecutionEdge, EdgeColor> edgeColors = new HashMap<>();
        initializeSubDag(subDag, nodeColors, edgeColors );

        // order determines priority if an activity implements multiple interfaces
        determineVariableWriters(subDag, nodeColors, edgeColors );
        determineFusions(subDag, nodeColors, edgeColors );
        determinePipes(subDag, nodeColors, edgeColors );

        // getContractedDag(subDag, nodeColors, edgeColors) or directly if possible
        // get blocks with no incoming edges

        // TODO: ensure common transaction boundaries are respected

        return null;
    }


    private void initializeSubDag(AttributedDirectedGraph<UUID, ExecutionEdge> subDag, Map<UUID, NodeColor> nodeColors, Map<ExecutionEdge, EdgeColor> edgeColors) {

        for ( UUID n : execDag.vertexSet() ) {
            ActivityState state = getState( n );
            // ignore finished or skipped activities
            if ( state == ActivityState.QUEUED || state == ActivityState.EXECUTING ) {
                nodeColors.put( n, state == ActivityState.EXECUTING ? NodeColor.EXECUTING : NodeColor.DEFAULT );
                subDag.addVertex( n );
            }
        }

        for ( UUID source : subDag.vertexSet() ) {
            for ( ExecutionEdge edge : execDag.getOutwardEdges( source ) ) {
                UUID target = edge.getTarget();
                if ( !subDag.vertexSet().contains( target ) ) {
                    continue; // edge to an activity that was already aborted
                }

                assert getEdgeState( edge ) == EdgeState.IDLE : "Encountered edge of queued or executing activity that is not idle: " + edge;
                if (edge.isIgnored()) {
                    continue; // control edges that are no longer required are not added to the subDag
                }

                EdgeColor color = EdgeColor.UNDEFINED;
                if (edge.isControl()) {
                    color = EdgeColor.CONTROL;
                } else if (requiresCheckpoint( source ) || getState( source ) == ActivityState.EXECUTING) {
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
        for (UUID source : TopologicalOrderIterator.of( subDag ) ) {
            if (nodeColors.get( source ) != NodeColor.DEFAULT || !canFuse( source )) {
                continue; // ignore already colored activities
            }

            List<ExecutionEdge> edges = subDag.getOutwardEdges( source );
            if (edges.size() != 1 ) {
                continue;
            }
            ExecutionEdge edge = edges.get( 0 );
            UUID target = edge.getTarget();
            if (nodeColors.get( target ) != NodeColor.DEFAULT || edgeColors.get( edge ) != EdgeColor.UNDEFINED) {
                continue; // checkpoint or control edge, writer node
            }

            if (canFuse( target ) ) {
                edgeColors.put( edge, EdgeColor.FUSED );
                fused.add( source ); // source or target might already be in set
                fused.add( target );
            }
        }

        fused.forEach( n -> nodeColors.put( n, NodeColor.FUSED ) ); // only apply color AFTER iteration
    }


    private void determinePipes( AttributedDirectedGraph<UUID, ExecutionEdge> subDag, Map<UUID, NodeColor> nodeColors, Map<ExecutionEdge, EdgeColor> edgeColors ) {
        Set<UUID> piped = new HashSet<>();
        for (UUID source : TopologicalOrderIterator.of( subDag ) ) {
            if (nodeColors.get( source ) != NodeColor.DEFAULT || !canPipe( source )) {
                continue; // ignore already colored activities
            }

            List<ExecutionEdge> edges = subDag.getOutwardEdges( source );
            if (edges.size() != 1 ) {
                continue;
            }
            ExecutionEdge edge = edges.get( 0 );
            UUID target = edge.getTarget();
            if (nodeColors.get( target ) != NodeColor.DEFAULT || edgeColors.get( edge ) != EdgeColor.UNDEFINED) {
                continue; // checkpoint or control edge, writer or fused node
            }

            if (canPipe( target ) ) {
                edgeColors.put( edge, EdgeColor.PIPED );
                piped.add( source ); // source or target might already be in set
                piped.add( target );
            }
        }

        piped.forEach( n -> nodeColors.put( n, NodeColor.PIPED ) ); // only apply color AFTER iteration
    }


    /**
     * We successively assign "colors" (or roles) to nodes and edges of the execDag
     */
    private enum NodeColor {
        DEFAULT,
        EXECUTING,
        FUSED,
        PIPED,
        WRITER
    }


    private enum EdgeColor {
        UNDEFINED,
        CONTROL,
        CHECKPOINT,
        FUSED,
        PIPED
    }

}
