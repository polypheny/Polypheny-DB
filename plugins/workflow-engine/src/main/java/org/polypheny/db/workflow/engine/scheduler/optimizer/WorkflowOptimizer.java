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
import java.util.Set;
import java.util.UUID;
import lombok.Getter;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.VariableWriter;
import org.polypheny.db.workflow.engine.execution.DefaultExecutor;
import org.polypheny.db.workflow.engine.execution.Executor;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorType;
import org.polypheny.db.workflow.engine.execution.FusionExecutor;
import org.polypheny.db.workflow.engine.execution.PipeExecutor;
import org.polypheny.db.workflow.engine.execution.VariableWriterExecutor;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge.ExecutionEdgeFactory;
import org.polypheny.db.workflow.engine.scheduler.ExecutionSubmission;
import org.polypheny.db.workflow.engine.scheduler.GraphUtils;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.models.ActivityConfigModel.CommonType;
import org.polypheny.db.workflow.models.WorkflowConfigModel;

public abstract class WorkflowOptimizer {

    final Workflow workflow;
    final AttributedDirectedGraph<UUID, ExecutionEdge> execDag;
    final boolean isFusionEnabled;
    final boolean isPipelineEnabled;


    protected WorkflowOptimizer( Workflow workflow, AttributedDirectedGraph<UUID, ExecutionEdge> execDag ) {
        this.workflow = workflow;
        this.execDag = execDag;
        WorkflowConfigModel config = workflow.getConfig();
        isFusionEnabled = config.isFusionEnabled();
        isPipelineEnabled = config.isPipelineEnabled();
    }


    public final List<SubmissionFactory> computeNextTrees( int submissionCount, CommonType commonType ) {

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
    abstract List<SubmissionFactory> computeNextTrees( CommonType commonType );


    /**
     * Whether the specified activity can fuse, without taking the workflow structure into account
     *
     * @param activityId the activity to check
     * @return true if this activity can fuse when viewed in isolation
     */
    boolean canFuse( UUID activityId ) {
        ActivityWrapper wrapper = workflow.getActivity( activityId );
        if ( wrapper.getActivity() instanceof Fusable fusable ) {
            return fusable.canFuse( wrapper.getInTypePreview(), wrapper.getSettingsPreview() ).orElse( false );
        }
        return false;
    }


    boolean canPipe( UUID activityId ) {
        ActivityWrapper wrapper = workflow.getActivity( activityId );
        if ( wrapper.getActivity() instanceof Pipeable pipeable ) {
            return pipeable.canPipe( wrapper.getInTypePreview(), wrapper.getSettingsPreview() ).orElse( false );
        }
        return false;
    }


    boolean requestsToWrite( UUID activityId ) {
        ActivityWrapper wrapper = workflow.getActivity( activityId );
        if ( wrapper.getActivity() instanceof VariableWriter writer ) {
            // true is more restricting for optimizer -> return true if empty Optional
            return writer.requestsToWrite( wrapper.getInTypePreview(), wrapper.getSettingsPreview() ).orElse( true );
        }
        return false;
    }


    boolean requiresCheckpoint( UUID activityId ) {
        return workflow.getActivity( activityId ).getConfig().isEnforceCheckpoint();
    }


    ActivityState getState( UUID activityId ) {
        return workflow.getActivity( activityId ).getState();
    }


    AttributedDirectedGraph<UUID, ExecutionEdge> getCommonSubExecDag( CommonType commonType ) {
        Set<UUID> nodes = new HashSet<>();
        for ( UUID n : execDag.vertexSet() ) {
            ActivityWrapper wrapper = workflow.getActivity( n );
            if ( wrapper.getConfig().getCommonType() == commonType ) {
                nodes.add( n );
            }
        }
        return GraphUtils.getInducedSubgraph( execDag, nodes );
    }


    @Getter
    public static class SubmissionFactory {

        private final AttributedDirectedGraph<UUID, ExecutionEdge> tree;
        private final Set<UUID> activities;
        private final ExecutorType executorType;
        private final CommonType commonType;


        public SubmissionFactory( AttributedDirectedGraph<UUID, ExecutionEdge> tree, Set<UUID> activities, ExecutorType executorType, CommonType commonType ) {
            this.tree = tree;
            this.activities = activities;
            this.executorType = executorType;
            this.commonType = commonType;
        }


        public SubmissionFactory( UUID activity, ExecutorType executorType, CommonType commonType ) {
            this.executorType = executorType;
            this.activities = Set.of( activity );
            this.commonType = commonType;
            this.tree = AttributedDirectedGraph.create( new ExecutionEdgeFactory() );
            this.tree.addVertex( activity );
        }


        public ExecutionSubmission create( StorageManager sm, Workflow wf ) {
            UUID root = getRootActivity(); // root of inverted tree
            ExecutionInfo info = new ExecutionInfo( activities, executorType );
            Executor executor = switch ( executorType ) {
                case DEFAULT -> new DefaultExecutor( sm, wf, root, info );
                case FUSION -> new FusionExecutor( sm, wf, tree, root, info );
                case PIPE -> new PipeExecutor( sm, wf, tree, root, wf.getConfig().getPipelineQueueCapacity(), info );
                case VARIABLE_WRITER -> new VariableWriterExecutor( sm, wf, getRootActivity(), info );
            };

            return new ExecutionSubmission( executor, activities, root, commonType, sm.getSessionId(), wf.getTimeoutSeconds( activities ), info );
        }


        private UUID getRootActivity() {
            if ( activities.size() == 1 ) {
                return activities.iterator().next();
            }
            return GraphUtils.findInvertedTreeRoot( tree );
        }

    }

}
