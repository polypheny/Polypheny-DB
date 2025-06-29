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

package org.polypheny.db.workflow.engine.scheduler;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.util.graph.TopologicalOrderIterator;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.dag.activities.impl.special.NestedInputActivity;
import org.polypheny.db.workflow.dag.activities.impl.special.NestedOutputActivity;
import org.polypheny.db.workflow.dag.edges.ControlEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo.LogLevel;
import org.polypheny.db.workflow.engine.monitoring.ExecutionMonitor;
import org.polypheny.db.workflow.engine.scheduler.optimizer.WorkflowOptimizer;
import org.polypheny.db.workflow.engine.scheduler.optimizer.WorkflowOptimizer.SubmissionFactory;
import org.polypheny.db.workflow.engine.scheduler.optimizer.WorkflowOptimizerImpl;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.models.ActivityConfigModel.CommonType;
import org.polypheny.db.workflow.session.NestedSessionManager;

/**
 * The scheduler takes a workflow and a (optional) target activitiy.
 * It determines what activities have to be executed in order to execute the target activity or all leaf nodes of the workflow, if no target was specified.
 * The scheduler uses the fact that activities with checkpoints do not have to be executed again (if the entire workflow should be executed again, it first needs to be reset).
 * <p>
 * We can assume that only a single thread (the GlobalScheduler coordinator) is accessing methods of a
 * WorkflowScheduler instance after its construction.
 * Sole control over the provided Workflow and StorageManager is given to the scheduler from creation up to the point the execution is finished.
 */
@Slf4j
public class WorkflowScheduler {

    private final Workflow workflow;
    private final StorageManager sm;
    private final NestedSessionManager nestedManager;
    private final int maxWorkers;
    private final AttributedDirectedGraph<UUID, ExecutionEdge> execDag;
    private final WorkflowOptimizer optimizer;
    @Getter
    private final ExecutionMonitor executionMonitor;
    private CountDownLatch finishLatch;

    private boolean isAborted; // by interruption
    private boolean isFinished;
    private Set<UUID> mustSucceed = new HashSet<>();
    private Set<UUID> mustFail = new HashSet<>();

    private int pendingCount = 0; // current number of unfinished submissions
    private final Set<UUID> remainingActivities = new HashSet<>(); // activities that have not finished execution
    private final Map<CommonType, Partition> partitions = new HashMap<>();
    private Partition activePartition;
    private final UUID targetActivityId;


    public WorkflowScheduler( Workflow workflow, StorageManager sm, @Nullable NestedSessionManager nestedManager, ExecutionMonitor monitor, int globalWorkers, @Nullable UUID targetActivity ) throws Exception {
        this.workflow = workflow;
        this.sm = sm;
        this.nestedManager = nestedManager;
        this.executionMonitor = monitor;
        this.maxWorkers = Math.min( workflow.getConfig().getMaxWorkers(), globalWorkers );
        this.targetActivityId = targetActivity;

        if ( targetActivity != null && workflow.getActivity( targetActivity ).getState() == ActivityState.SAVED ) {
            throw new GenericRuntimeException( "A saved activity first needs to be reset before executing it" );
        }

        this.execDag = targetActivity == null ? prepareExecutionDag() : prepareExecutionDag( List.of( targetActivity ) );

        workflow.validateStructure( sm, this.execDag );
        workflow.setState( WorkflowState.EXECUTING );
        this.optimizer = new WorkflowOptimizerImpl( workflow, execDag );

    }


    public List<ExecutionSubmission> startExecution() {
        executionMonitor.setTotalCount( (int) workflow.getActivities().stream()
                .filter( activity -> activity.getState() == ActivityState.QUEUED )
                .count()
        );
        List<ExecutionSubmission> submissions = computeNextSubmissions();
        executionMonitor.forwardStates();
        finishLatch = new CountDownLatch( 1 );
        return submissions;
    }


    public void interruptExecution() {
        isAborted = true;
        workflow.setState( WorkflowState.INTERRUPTED );
        executionMonitor.forwardStates();
        activePartition.abort();
    }


    /**
     * Check whether the results of all submitted execution submissions
     * have been handled and no more submissions will ever be made by
     * this scheduler.
     * <p>
     * Note that the scheduler is not immediately finished after interrupting its execution,
     * but only after handling the results of the existing submissions.
     *
     * @return true if this scheduler is finished
     */
    public boolean isFinished() {
        return isFinished;
    }


    public boolean awaitFinish( int seconds ) throws InterruptedException {
        return finishLatch == null || finishLatch.await( seconds, TimeUnit.SECONDS );
    }


    public boolean isCommonActive( @NonNull CommonType commonType ) {
        return sm.isCommonActive( commonType );
    }


    private AttributedDirectedGraph<UUID, ExecutionEdge> prepareExecutionDag() throws Exception {
        List<UUID> targets = new ArrayList<>();
        for ( ActivityWrapper wrapper : workflow.getActivities() ) {
            UUID id = wrapper.getId();
            if ( workflow.getOutEdges( id ).isEmpty() && wrapper.getState() != ActivityState.SAVED ) {
                targets.add( id );
            }
        }
        return prepareExecutionDag( targets );
    }


    private AttributedDirectedGraph<UUID, ExecutionEdge> prepareExecutionDag( List<UUID> targets ) throws Exception {
        if ( targets.isEmpty() ) {
            throw new GenericRuntimeException( "Cannot prepare executionDag for empty targets" );
        }
        Set<UUID> savedActivities = new HashSet<>();
        Set<ExecutionEdge> edgesToIgnore = new HashSet<>(); // edges that go from finished to successfully executed activities (see explanation of compromise solution below)
        Set<UUID> finishedActivities = new HashSet<>();
        Queue<UUID> open = new LinkedList<>( targets );
        Set<UUID> visited = new HashSet<>();

        // perform reverse DFS from targets to saved nodes
        while ( !open.isEmpty() ) {
            UUID n = open.remove();
            if ( visited.contains( n ) ) {
                continue;
            }
            visited.add( n );

            ActivityWrapper nWrapper = workflow.getActivity( n );
            if ( nWrapper.getState() == ActivityState.SAVED ) {
                savedActivities.add( n );
                continue;
            } else if ( nWrapper.getState() == ActivityState.FINISHED ) {
                // this can happen when a new outgoing edge to a FINISHED activity has been created between executions.
                // problem: the entire fused / piped subtree that this activity belongs to and all executed successor subtrees also need to be executed again
                // solution: either reset activities already when user creates edge (but this is not very robust), or handle it here, possibly recomputing large parts of the DAG
                // compromise solution: only recompute this activity (and unsaved data-edge predecessors) -> assumption: activities are idempotent (not actually true, but good enough)
                List<Edge> outEdges = workflow.getOutEdges( n );
                finishedActivities.add( n );
                edgesToIgnore.addAll( outEdges.stream()
                        .filter( e -> !finishedActivities.contains( e.getTo().getId() ) && e.getTo().getState().isSuccess() )
                        .map( e -> new ExecutionEdge( e.getFrom().getId(), e.getTo().getId(), e ) )
                        .toList() );
            }

            nWrapper.resetExecution( workflow.getVariables() );
            nWrapper.setState( ActivityState.QUEUED );
            sm.dropCheckpoints( n );
            remainingActivities.add( n );

            List<Edge> inEdges = workflow.getInEdges( n );
            for ( Edge edge : inEdges ) {
                edge.setState( EdgeState.IDLE );
                open.add( edge.getFrom().getId() );
            }
        }

        AttributedDirectedGraph<UUID, ExecutionEdge> execDag = GraphUtils.getInducedSubgraph( workflow.toDag(), visited, edgesToIgnore );

        // handle saved activities (= simulate that they finish their execution successfully)
        for ( UUID saved : savedActivities ) {
            updateGraph( true, Set.of( saved ), saved, execDag ); // result propagation needs to happen individually
        }

        for ( UUID n : TopologicalOrderIterator.of( execDag ) ) {
            workflow.updateValidPreview( n );
            ActivityWrapper wrapper = workflow.getActivity( n );
            wrapper.applyContext( nestedManager, sm );
            if ( wrapper.getActivity() instanceof NestedOutputActivity ) {
                mustSucceed.add( n ); // config value is ignored
            } else {
                switch ( wrapper.getConfig().getExpectedOutcome() ) {
                    case MUST_SUCCEED -> mustSucceed.add( n );
                    case MUST_FAIL -> mustFail.add( n );
                }
            }
        }

        for ( CommonType type : List.of( CommonType.EXTRACT, CommonType.NONE, CommonType.LOAD ) ) {
            Partition partition = new Partition( type, execDag );
            partitions.put( type, partition );
            if ( activePartition == null && !partition.isFinished ) {
                activePartition = partition;
                activePartition.start();
            }
        }

        return execDag;
    }


    public List<ExecutionSubmission> handleExecutionResult( ExecutionResult result ) {
        pendingCount--;

        try {
            updateTransactions( result );
        } catch ( TransactionException e ) {
            result = new ExecutionResult( result.getSubmission(), new ExecutorException( "An error occurred while closing open transactions of executed activities", e ) );
        }

        if ( !result.isSuccess() ) {
            sm.dropCheckpoints( result.getRootId() ); // remove any created checkpoints
            setErrorVariable( result.getActivities(), result.getException() );
        }

        updateGraph( result.isSuccess(), result.getActivities(), result.getRootId(), execDag );
        updatePartitions();

        if ( remainingActivities.isEmpty() ) {
            assert pendingCount == 0;
            setFinished();
            return null;
        }

        if ( isAborted ) {
            if ( pendingCount == 0 ) {
                setFinished();
            }
            return null;
        }

        try {
            List<ExecutionSubmission> next = computeNextSubmissions();
            executionMonitor.forwardStates();
            return next;
        } catch ( Exception e ) {
            // this should never happen, but as a fallback we finish workflow execution
            log.error( "An unexpected error occurred while determining the next activities to be submitted", e );
            setFinished();
            return null;
        }
    }


    private void updateTransactions( ExecutionResult result ) throws TransactionException {
        CommonType type = result.getCommonType();
        Set<UUID> activities = result.getActivities();
        remainingActivities.removeAll( activities );

        assert type == activePartition.commonType;
        activePartition.setResolved( activities, result.isSuccess() );
        if ( activePartition.isAtomic ) {
            if ( result.isSuccess() && activePartition.hasFailed ) {
                throw new TransactionException( "Transaction has already been rolled back" ); // handle remaining activities after abort
            }
        } else {
            if ( result.isSuccess() ) {
                activities.forEach( sm::commitTransaction );
            } else {
                activities.forEach( sm::rollbackTransaction );
            }
        }
    }


    private List<ExecutionSubmission> computeNextSubmissions() {
        List<SubmissionFactory> factories = optimizer.computeNextTrees( maxWorkers - pendingCount, activePartition.commonType );
        if ( pendingCount == 0 && factories.isEmpty() ) {
            throw new IllegalStateException( "The optimizer is unable to determine the next activity to be executed" );
        }
        pendingCount += factories.size();
        List<ExecutionSubmission> submissions = factories.stream().map( f -> f.create( sm, workflow ) ).toList();
        for ( ExecutionSubmission submission : submissions ) {
            setStates( submission.getActivities(), ActivityState.EXECUTING );
            executionMonitor.addInfo( submission.getInfo() );
        }

        return submissions;
    }


    private void setErrorVariable( Set<UUID> activities, ExecutorException exception ) {
        ObjectNode value = exception.getVariableValue( activities );
        for ( UUID n : activities ) {
            workflow.getActivity( n ).getVariables().setError( value );
        }
    }


    private void updateGraph( boolean isSuccess, Set<UUID> activities, UUID rootId, AttributedDirectedGraph<UUID, ExecutionEdge> dag ) {
        // must not access this.execDag as it might be null at this point
        ActivityWrapper root = workflow.getActivity( rootId );
        boolean isInitialUpdate = root.getState() == ActivityState.SAVED;

        for ( UUID n : activities ) {
            ActivityWrapper wrapper = workflow.getActivity( n );
            if ( !isInitialUpdate ) {
                if ( isSuccess ) {
                    wrapper.setState( n.equals( rootId ) ? ActivityState.SAVED : ActivityState.FINISHED );
                } else {
                    wrapper.setState( ActivityState.FAILED );
                }
            }

            for ( ExecutionEdge execEdge : dag.getOutwardEdges( n ) ) {
                Edge edge = workflow.getEdge( execEdge );
                if ( activities.contains( execEdge.getTarget() ) ) {
                    edge.setState( isSuccess ? EdgeState.ACTIVE : EdgeState.INACTIVE );
                } else if ( !(isInitialUpdate && edge.getTo().getState() == ActivityState.SAVED) ) { // already saved target activities do not have to be updated again
                    assert edge.isIgnored() || !edge.getTo().getState().isExecuted() :
                            "Encountered an activity that was executed before its predecessors: " + edge.getTo();

                    boolean isActive = isSuccess ?
                            !(edge instanceof ControlEdge control) || control.isOnSuccess() :
                            (edge instanceof ControlEdge control && !control.isOnSuccess());
                    propagateResult( isActive, edge, dag, isInitialUpdate );
                }
            }
        }

        if ( workflow.getConfig().isDropUnusedCheckpoints() ) {
            for ( UUID n : dag.vertexSet() ) {
                if ( n == targetActivityId ) {
                    continue; // We assume the user wants to keep the checkpoints of the target activity
                }
                ActivityWrapper wrapper = workflow.getActivity( n );
                if ( wrapper.getConfig().isEnforceCheckpoint() || wrapper.getState() != ActivityState.SAVED || wrapper.getActivity() instanceof NestedInputActivity ) {
                    continue;
                }
                if ( dag.getOutwardEdges( n ).stream().allMatch( execEdge -> {
                    ActivityWrapper target = workflow.getActivity( execEdge.getTarget() );
                    if ( target.getActivity() instanceof NestedOutputActivity ) {
                        return false; // checkpoints might be required by the parent workflow
                    }
                    ActivityState state = target.getState();
                    return state.isExecuted() || state == ActivityState.SKIPPED || workflow.getEdge( execEdge ).isIgnored();
                } )
                ) {
                    sm.dropCheckpoints( n ); // all successors are already executed
                    wrapper.setState( ActivityState.FINISHED );
                }
            }
        }

        if ( !isInitialUpdate ) {
            // updates previews beyond the limits of the dag
            for ( UUID n : getReachableNonExecutedNodes( activities ) ) {  // previously only updated within dag: GraphUtils.getTopologicalIterable( dag, rootId, false )
                workflow.updatePreview( n );
            }
        }
    }


    private void propagateResult( boolean isActive, Edge edge, AttributedDirectedGraph<UUID, ExecutionEdge> dag, boolean ignorePartitions ) {
        // must not access this.execDag as it might be null at this point
        Partition sourcePartition = partitions.get( edge.getFrom().getConfig().getCommonType() );
        Partition targetPartition = partitions.get( edge.getTo().getConfig().getCommonType() );
        if ( !(ignorePartitions || sourcePartition.canPropagateTo( targetPartition )) ) {
            return;
        }
        edge.setState( isActive ? EdgeState.ACTIVE : EdgeState.INACTIVE );
        if ( edge.isIgnored() || (isActive && !(edge instanceof ControlEdge)) ) {
            return; // Cannot propagate activation of a data edge or ignored edge, since it cannot change the state of the activity
        }
        ActivityWrapper target = edge.getTo();
        assert !target.getState().isExecuted() : "Encountered an activity that was executed before its predecessors: " + target;

        List<Edge> inEdges = workflow.getInEdges( target.getId() );
        EdgeState canExecute = target.canExecute( inEdges );
        switch ( canExecute ) {
            case IDLE -> {
            }
            case ACTIVE -> {
                List<ControlEdge> controlEdges = inEdges.stream().filter( e -> e instanceof ControlEdge control && !control.isActive() ).map( e -> (ControlEdge) e ).toList();
                controlEdges.forEach( e -> e.setIgnored( true ) ); // the not active edges can be ignored, as they are no longer relevant
                if ( target.getState() == ActivityState.SKIPPED ) {
                    // previously skipped because of common extract that succeeded on its own, but transaction failed
                    target.setState( ActivityState.QUEUED );
                    remainingActivities.add( target.getId() );
                }
            }
            case INACTIVE -> {
                target.setState( ActivityState.SKIPPED );
                remainingActivities.remove( target.getId() );
                executionMonitor.addSkippedActivity( target.getId() );
                if ( targetPartition != null ) { // in case of initial propagation for saved activities, there is no targetPartition yet
                    targetPartition.setResolved( target.getId(), false ); // no need to catch the exception, as the transaction is already rolled back
                }
                // a skipped activity does NOT count as failed -> onFail control edges also become INACTIVE
                dag.getOutwardEdges( target.getId() ).forEach( e -> propagateResult( false, workflow.getEdge( e ), dag, ignorePartitions ) );
            }
        }
    }


    private void updatePartitions() {
        while ( activePartition.isAllResolved() ) {
            activePartition.finish( execDag );
            if ( remainingActivities.isEmpty() ) {
                break;
            }

            activePartition = switch ( activePartition.commonType ) {
                case EXTRACT -> partitions.get( CommonType.NONE );
                case NONE -> partitions.get( CommonType.LOAD );
                case LOAD -> throw new IllegalStateException( "Cannot have remaining activities when load is finished" );
            };
            activePartition.start();
        }
    }


    private void setFinished() {
        if ( !remainingActivities.isEmpty() ) {
            setStates( remainingActivities, ActivityState.SKIPPED );
        }
        workflow.setState( WorkflowState.IDLE );
        executionMonitor.stop( isOverallSuccess() );
        isFinished = true;
        finishLatch.countDown();
    }


    private void setStates( Set<UUID> activities, ActivityState state ) {
        activities.forEach( id -> workflow.getActivity( id ).setState( state ) );
        if ( state == ActivityState.SKIPPED ) {
            activities.forEach( executionMonitor::addSkippedActivity );
        }
    }


    private Iterable<UUID> getReachableNonExecutedNodes( Set<UUID> roots ) {
        AttributedDirectedGraph<UUID, ExecutionEdge> graph = workflow.toDag();
        Set<UUID> reachable = new HashSet<>();
        Queue<UUID> open = new LinkedList<>();

        for ( UUID root : roots ) {
            graph.getOutwardEdges( root ).stream()
                    .filter( e -> waitsForExecution( e.getTarget() ) )
                    .forEach( e -> open.add( (UUID) e.target ) );
        }
        while ( !open.isEmpty() ) {
            UUID n = open.remove();
            if ( !reachable.contains( n ) ) {
                reachable.add( n );
                graph.getOutwardEdges( n ).stream()
                        .filter( e -> waitsForExecution( e.getTarget() ) )
                        .forEach( e -> open.add( (UUID) e.target ) );
            }
        }
        return TopologicalOrderIterator.of( GraphUtils.getInducedSubgraph( graph, reachable ) );
    }


    private boolean waitsForExecution( UUID activityId ) {
        ActivityState state = workflow.getActivity( activityId ).getState();
        return !state.isExecuted() && state != ActivityState.SKIPPED;
    }


    private boolean isOverallSuccess() {
        for ( UUID n : mustSucceed ) {
            if ( !workflow.getActivity( n ).getState().isSuccess() ) {
                return false;
            }
        }
        for ( UUID n : mustFail ) {
            if ( workflow.getActivity( n ).getState().isSuccess() ) {
                return false;
            }
        }
        return true;
    }


    private class Partition {

        private final CommonType commonType;
        private final boolean isAtomic;
        private final Set<UUID> activities;
        private final Set<UUID> remaining = new HashSet<>();
        private boolean hasFailed = false; // whether an activity of this partition was not successful (FAILED or SKIPPED)
        private boolean isStarted = false; // whether execution has started
        private boolean isFinished;


        private Partition( CommonType commonType, AttributedDirectedGraph<UUID, ExecutionEdge> dag ) {
            this.commonType = commonType;
            this.isAtomic = commonType != CommonType.NONE;
            this.activities = dag.vertexSet().stream().filter( n -> workflow.getActivity( n ).getConfig().getCommonType() == commonType ).collect( Collectors.toSet() );
            this.remaining.addAll( activities.stream().filter( n -> !workflow.getActivity( n ).getState().isExecuted() ).toList() );
            this.isFinished = remaining.isEmpty();
        }


        private void start() {
            assert !isStarted;
            if ( isAtomic && !hasFailed && !isAllResolved() ) {
                sm.startCommonTransaction( commonType );
            }

            if ( isAtomic && hasFailed ) {
                // partition failed before it was started
                for ( UUID n : activities ) {
                    if ( remaining.contains( n ) ) {
                        workflow.getActivity( n ).setState( ActivityState.SKIPPED );
                        remaining.remove( n );
                        remainingActivities.remove( n ); // also remove from workflow-wide remaining list
                    }
                }
            }

            isStarted = true;
        }


        private boolean contains( UUID activity ) {
            return activities.contains( activity );
        }


        private boolean isAllResolved() {
            return remaining.isEmpty();
        }


        private void setResolved( UUID activity, boolean isSuccess ) throws TransactionException {
            remaining.remove( activity );

            if ( hasFailed ) {
                return;
            }
            if ( isSuccess ) {
                assert isStarted : "Cannot resolve the successful execution of an activity if its partition has not yet started";
                if ( isAtomic && isAllResolved() ) {
                    try {
                        sm.commitCommonTransaction( commonType );
                    } catch ( TransactionException e ) {
                        hasFailed = true;
                        throw e;
                    }
                }
                return;
            }

            hasFailed = true;
            if ( isAtomic ) {
                if ( isStarted ) {
                    sm.rollbackCommonTransaction( commonType );
                    // we do not drop already created checkpoints
                }
            }
        }


        private void setResolved( Set<UUID> activities, boolean isSuccess ) throws TransactionException {
            activities.forEach( n -> setResolved( n, isSuccess ) );
        }


        private void finish( AttributedDirectedGraph<UUID, ExecutionEdge> dag ) {
            if ( !isStarted || isFinished || !remaining.isEmpty() ) {
                throw new IllegalStateException( "Partition is not in a valid state to be finished" );
            }

            if ( isAtomic ) {
                for ( UUID n : activities ) {
                    ActivityWrapper wrapper = workflow.getActivity( n );
                    if ( hasFailed ) {
                        wrapper.setRolledBack( true );
                        ExecutionInfo info = wrapper.getExecutionInfo();
                        if ( info != null ) {
                            info.appendLog( wrapper.getId(), LogLevel.ERROR, "Activity was rolled back because the common transaction was aborted." );
                        }
                    }

                    for ( ExecutionEdge execEdge : dag.getOutwardEdges( n ) ) {
                        if ( activities.contains( execEdge.getTarget() ) ) {
                            continue;
                        }
                        Edge edge = workflow.getEdge( execEdge );
                        assert !edge.getTo().getState().isExecuted() :
                                "Encountered an activity that was executed before the previous partition finished: " + edge.getTo();

                        boolean isActive = !hasFailed && wrapper.getState().isSuccess() ?
                                !(edge instanceof ControlEdge control) || control.isOnSuccess() :
                                (edge instanceof ControlEdge control && !control.isOnSuccess());
                        propagateResult( isActive, edge, dag, true ); // propagate across partition border
                    }
                }
            }
            isFinished = true;
        }


        private boolean canPropagateTo( Partition other ) {
            return this == other || !this.isAtomic;
        }


        private void abort() {
            if ( !isFinished ) {
                hasFailed = true;
                if ( isStarted && isAtomic ) {
                    sm.rollbackCommonTransaction( commonType );
                }
            }
        }

    }

}
