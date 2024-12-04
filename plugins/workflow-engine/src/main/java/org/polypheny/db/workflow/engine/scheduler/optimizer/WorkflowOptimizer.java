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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.VariableWriter;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.engine.execution.DefaultExecutor;
import org.polypheny.db.workflow.engine.execution.Executor;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorType;
import org.polypheny.db.workflow.engine.execution.FusionExecutor;
import org.polypheny.db.workflow.engine.execution.PipeExecutor;
import org.polypheny.db.workflow.engine.execution.VariableWriterExecutor;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge.ExecutionEdgeFactory;
import org.polypheny.db.workflow.engine.scheduler.ExecutionSubmission;
import org.polypheny.db.workflow.engine.scheduler.GraphUtils;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.models.ActivityConfigModel.CommonTransaction;

public abstract class WorkflowOptimizer {

    final Workflow workflow;
    final AttributedDirectedGraph<UUID, ExecutionEdge> execDag;

    Map<UUID, List<Optional<AlgDataType>>> typePreviews;
    Map<UUID, Map<String, Optional<SettingValue>>> settingsPreviews;


    protected WorkflowOptimizer( Workflow workflow, AttributedDirectedGraph<UUID, ExecutionEdge> execDag ) {
        this.workflow = workflow;
        this.execDag = execDag;
    }


    public final List<SubmissionFactory> computeNextTrees( Map<UUID, List<Optional<AlgDataType>>> typePreviews, Map<UUID, Map<String, Optional<SettingValue>>> settingsPreviews, int submissionCount, CommonTransaction commonType ) {
        this.typePreviews = typePreviews;
        this.settingsPreviews = settingsPreviews;

        List<SubmissionFactory> orderedCandidates = computeNextTrees( commonType );
        return orderedCandidates.subList( 0, Math.min( submissionCount, orderedCandidates.size() ) );
    }


    /**
     * Returns a list of candidate submissions based on the current state of the optimizer.
     * The list is ordered by priority (most important submission first).
     * This operation must not perform any changes to any of the fields of the abstract WorkflowOptimizer.
     * There is no guarantee whether the returned submissions will actually be queued for execution.
     *
     * @return A list of SubmissionFactories that can be used to create actual submissions.
     */
    abstract List<SubmissionFactory> computeNextTrees( CommonTransaction commonType );


    /**
     * Whether the specified activity can fuse, without taking the workflow structure into account
     *
     * @param activityId the activity to check
     * @return true if this activity can fuse when viewed in isolation
     */
    boolean canFuse( UUID activityId ) {
        if ( workflow.getActivity( activityId ).getActivity() instanceof Fusable fusable ) {
            return fusable.canFuse( typePreviews.get( activityId ), settingsPreviews.get( activityId ) ).orElse( false );
        }
        return false;
    }


    boolean canPipe( UUID activityId ) {
        if ( workflow.getActivity( activityId ).getActivity() instanceof Pipeable pipeable ) {
            return pipeable.canPipe( typePreviews.get( activityId ), settingsPreviews.get( activityId ) ).orElse( false );
        }
        return false;
    }


    boolean requestsToWrite( UUID activityId ) {
        if ( workflow.getActivity( activityId ).getActivity() instanceof VariableWriter writer ) {
            // true is more restricting for optimizer -> return true if empty Optional
            return writer.requestsToWrite( typePreviews.get( activityId ), settingsPreviews.get( activityId ) ).orElse( true );
        }
        return false;
    }


    boolean requiresCheckpoint( UUID activityId ) {
        return workflow.getActivity( activityId ).getConfig().isEnforceCheckpoint();
    }


    ActivityState getState( UUID activityId ) {
        return workflow.getActivity( activityId ).getState();
    }


    AttributedDirectedGraph<UUID, ExecutionEdge> getCommonSubExecDag( CommonTransaction commonType ) {
        Set<UUID> nodes = new HashSet<>();
        for ( UUID n : execDag.vertexSet() ) {
            ActivityWrapper wrapper = workflow.getActivity( n );
            if ( wrapper.getConfig().getTransactionMode() == commonType ) {
                nodes.add( n );
            }
        }
        return GraphUtils.getInducedSubgraph( execDag, nodes );
    }


    Edge getEdge( ExecutionEdge edge ) {
        for ( Edge candidate : workflow.getEdges( edge.getSource(), edge.getTarget() ) ) {
            if ( edge.representsEdge( candidate ) ) {
                return candidate;
            }
        }
        throw new IllegalArgumentException( "Cannot return Edge of ExecutionEdge that is not part of the workflow: " + edge );
    }


    EdgeState getEdgeState( ExecutionEdge edge ) {
        return getEdge( edge ).getState();
    }


    @Getter
    public static class SubmissionFactory {

        private final AttributedDirectedGraph<UUID, ExecutionEdge> tree;
        private final Set<UUID> activities;
        private final ExecutorType executorType;
        private final CommonTransaction commonType;


        public SubmissionFactory( AttributedDirectedGraph<UUID, ExecutionEdge> tree, Set<UUID> activities, ExecutorType executorType, CommonTransaction commonType ) {
            this.tree = tree;
            this.activities = activities;
            this.executorType = executorType;
            this.commonType = commonType;
        }


        public SubmissionFactory( UUID activity, ExecutorType executorType, CommonTransaction commonType ) {
            this.executorType = executorType;
            this.activities = Set.of( activity );
            this.commonType = commonType;
            this.tree = AttributedDirectedGraph.create( new ExecutionEdgeFactory() );
            this.tree.addVertex( activity );
        }


        public ExecutionSubmission create( StorageManager sm, Workflow wf ) {
            Executor executor = switch ( executorType ) {
                case DEFAULT -> new DefaultExecutor( sm, wf, getActivity() );
                case FUSION -> new FusionExecutor( sm, wf, tree );
                case PIPE -> new PipeExecutor( sm, wf, tree, 1000 );
                case VARIABLE_WRITER -> new VariableWriterExecutor( sm, wf, getActivity() );
            };

            return new ExecutionSubmission( commonType, executor, activities, sm.getSessionId() );
        }


        private UUID getActivity() {
            if ( activities.size() != 1 ) {
                throw new GenericRuntimeException( "Invalid number of activities: " + activities.size() );
            }
            return activities.iterator().next();
        }

    }

}
